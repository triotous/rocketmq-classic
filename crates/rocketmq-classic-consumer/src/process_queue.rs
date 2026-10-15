// P10-06 attaches these queue operations to bounded consume requests. Keep
// this owner private until the callback lifecycle is complete.
#![allow(dead_code)]

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Arc, Mutex, MutexGuard},
    time::{SystemTime, UNIX_EPOCH},
};

use rocketmq_classic_common::message::{MessageExt, MessageQueue};
use tokio::time::Instant;

/// A single queue assignment's retained messages and in-flight consume work.
/// Every operation that can change ownership state is scoped to its generation.
pub(crate) struct ProcessQueue {
    queue: MessageQueue,
    generation: u64,
    state: Mutex<ProcessQueueState>,
    orderly_consume_gate: Arc<tokio::sync::Mutex<()>>,
}

struct ProcessQueueState {
    messages: BTreeMap<i64, QueueMessage>,
    in_flight_batches: HashMap<u64, Vec<i64>>,
    cached_count: usize,
    cached_bytes: u64,
    highest_seen_offset: Option<i64>,
    dropped: bool,
    consuming: bool,
    next_batch_id: u64,
    last_pull_timestamp_millis: i64,
    last_consume_timestamp_millis: i64,
    locked: bool,
    last_lock_timestamp_millis: i64,
    last_lock_instant: Option<Instant>,
    lock_attempt_id: u64,
    try_unlock_times: u64,
}

struct QueueMessage {
    message: Arc<MessageExt>,
    body_bytes: u64,
    state: MessageState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MessageState {
    Ready,
    InFlight(u64),
    Held,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OrderlyCompletion {
    HoldCurrent,
    CommitAll,
    RollbackAll,
    RequeueCurrent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LockAttemptResult {
    Applied,
    Superseded,
    Dropped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ProcessQueueError {
    #[error("work belongs to an older queue assignment generation")]
    StaleGeneration,
    #[error("process queue assignment has been dropped")]
    Dropped,
    #[error("consume batch does not belong to this process queue")]
    UnknownBatch,
    #[error("consume batch no longer owns all of its messages")]
    BatchOwnershipLost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InsertOutcome {
    pub(crate) inserted_count: usize,
    pub(crate) dispatch_to_consume: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConsumeBatch {
    id: u64,
    generation: u64,
    queue: MessageQueue,
    offsets: Vec<i64>,
    messages: Vec<Arc<MessageExt>>,
}

impl ConsumeBatch {
    pub(crate) const fn id(&self) -> u64 {
        self.id
    }

    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn queue(&self) -> &MessageQueue {
        &self.queue
    }

    pub(crate) fn offsets(&self) -> &[i64] {
        &self.offsets
    }

    pub(crate) fn messages(&self) -> &[Arc<MessageExt>] {
        &self.messages
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompletionOutcome {
    pub(crate) completed_offsets: Vec<i64>,
    pub(crate) requeued_offsets: Vec<i64>,
    pub(crate) next_commit_offset: Option<i64>,
    pub(crate) has_ready_messages: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProcessQueueSnapshot {
    pub(crate) queue: MessageQueue,
    pub(crate) generation: u64,
    pub(crate) cached_count: usize,
    pub(crate) cached_bytes: u64,
    pub(crate) minimum_offset: Option<i64>,
    pub(crate) maximum_offset: Option<i64>,
    pub(crate) max_span: i64,
    pub(crate) next_commit_offset: Option<i64>,
    pub(crate) in_flight_count: usize,
    pub(crate) dropped: bool,
    pub(crate) consuming: bool,
    pub(crate) last_pull_timestamp_millis: i64,
    pub(crate) last_consume_timestamp_millis: i64,
    pub(crate) locked: bool,
    pub(crate) last_lock_timestamp_millis: i64,
    pub(crate) try_unlock_times: u64,
}

impl ProcessQueue {
    pub(crate) fn new(queue: MessageQueue, generation: u64) -> Self {
        let now = epoch_millis();
        Self {
            queue,
            generation,
            state: Mutex::new(ProcessQueueState {
                messages: BTreeMap::new(),
                in_flight_batches: HashMap::new(),
                cached_count: 0,
                cached_bytes: 0,
                highest_seen_offset: None,
                dropped: false,
                consuming: false,
                next_batch_id: 1,
                last_pull_timestamp_millis: now,
                last_consume_timestamp_millis: now,
                locked: false,
                last_lock_timestamp_millis: now,
                last_lock_instant: Some(Instant::now()),
                lock_attempt_id: 0,
                try_unlock_times: 0,
            }),
            orderly_consume_gate: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub(crate) fn queue(&self) -> &MessageQueue {
        &self.queue
    }

    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn orderly_consume_gate(&self) -> Arc<tokio::sync::Mutex<()>> {
        self.orderly_consume_gate.clone()
    }

    /// Retain pulled messages by queue offset. Repeated offsets update a ready
    /// cached message but never double-count it; an in-flight message remains
    /// owned by the listener batch that already received it.
    pub(crate) fn insert_messages(
        &self,
        generation: u64,
        messages: Vec<MessageExt>,
    ) -> Result<InsertOutcome, ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_active(generation, &state)?;

        let mut inserted_count = 0usize;
        for message in messages {
            let offset = message.queue_offset();
            let body_bytes = message.body().len() as u64;
            state.highest_seen_offset = Some(
                state
                    .highest_seen_offset
                    .map_or(offset, |highest| highest.max(offset)),
            );
            match state
                .messages
                .get(&offset)
                .map(|entry| (entry.state, entry.body_bytes))
            {
                Some((MessageState::Ready, previous_body_bytes)) => {
                    if let Some(existing) = state.messages.get_mut(&offset) {
                        existing.message = Arc::new(message);
                        existing.body_bytes = body_bytes;
                    }
                    state.cached_bytes = state
                        .cached_bytes
                        .saturating_sub(previous_body_bytes)
                        .saturating_add(body_bytes);
                }
                Some((MessageState::InFlight(_) | MessageState::Held, _)) => {}
                None => {
                    state.messages.insert(
                        offset,
                        QueueMessage {
                            message: Arc::new(message),
                            body_bytes,
                            state: MessageState::Ready,
                        },
                    );
                    state.cached_count = state.cached_count.saturating_add(1);
                    state.cached_bytes = state.cached_bytes.saturating_add(body_bytes);
                    inserted_count = inserted_count.saturating_add(1);
                }
            }
        }

        state.last_pull_timestamp_millis = epoch_millis();
        let has_ready_messages = state
            .messages
            .values()
            .any(|message| message.state == MessageState::Ready);
        let dispatch_to_consume = has_ready_messages && !state.consuming;
        if dispatch_to_consume {
            state.consuming = true;
        }
        Ok(InsertOutcome {
            inserted_count,
            dispatch_to_consume,
        })
    }

    pub(crate) fn take_batch(
        &self,
        generation: u64,
        batch_size: usize,
    ) -> Result<Option<ConsumeBatch>, ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_active(generation, &state)?;
        if batch_size == 0 {
            return Ok(None);
        }

        let offsets = state
            .messages
            .iter()
            .filter(|(_, message)| message.state == MessageState::Ready)
            .take(batch_size)
            .map(|(offset, _)| *offset)
            .collect::<Vec<_>>();
        if offsets.is_empty() {
            if state.in_flight_batches.is_empty() {
                state.consuming = false;
            }
            return Ok(None);
        }

        let batch_id = Self::allocate_batch_id(&mut state);
        let mut messages = Vec::with_capacity(offsets.len());
        for offset in &offsets {
            if let Some(message) = state.messages.get_mut(offset) {
                message.state = MessageState::InFlight(batch_id);
                messages.push(message.message.clone());
            }
        }
        state.in_flight_batches.insert(batch_id, offsets.clone());
        state.consuming = true;
        state.last_consume_timestamp_millis = epoch_millis();
        Ok(Some(ConsumeBatch {
            id: batch_id,
            generation,
            queue: self.queue.clone(),
            offsets,
            messages,
        }))
    }

    /// Remove only terminally handled offsets. Every other message in the
    /// batch returns to the ready map, so retries stay accounted until success.
    pub(crate) fn complete_batch(
        &self,
        generation: u64,
        batch_id: u64,
        terminal_offsets: &[i64],
    ) -> Result<CompletionOutcome, ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_active(generation, &state)?;
        let Some(offsets) = state.in_flight_batches.get(&batch_id).cloned() else {
            return Err(ProcessQueueError::UnknownBatch);
        };
        if offsets.iter().any(|offset| {
            state
                .messages
                .get(offset)
                .is_none_or(|message| message.state != MessageState::InFlight(batch_id))
        }) {
            return Err(ProcessQueueError::BatchOwnershipLost);
        }

        let terminal_offsets = terminal_offsets.iter().copied().collect::<BTreeSet<_>>();
        state.in_flight_batches.remove(&batch_id);
        let mut completed_offsets = Vec::new();
        let mut requeued_offsets = Vec::new();
        for offset in offsets {
            if terminal_offsets.contains(&offset) {
                if let Some(message) = state.messages.remove(&offset) {
                    state.cached_count = state.cached_count.saturating_sub(1);
                    state.cached_bytes = state.cached_bytes.saturating_sub(message.body_bytes);
                    completed_offsets.push(offset);
                }
            } else if let Some(message) = state.messages.get_mut(&offset) {
                message.state = MessageState::Ready;
                requeued_offsets.push(offset);
            }
        }
        let has_ready_messages = state
            .messages
            .values()
            .any(|message| message.state == MessageState::Ready);
        state.consuming = !state.messages.is_empty();
        state.last_consume_timestamp_millis = epoch_millis();
        let next_commit_offset = next_commit_offset(&state);
        Ok(CompletionOutcome {
            completed_offsets,
            requeued_offsets,
            next_commit_offset,
            has_ready_messages,
        })
    }

    /// Applies Java orderly semantics to the current batch and any prior
    /// autoCommit=false batches retained in `Held` state.
    pub(crate) fn complete_orderly_batch(
        &self,
        generation: u64,
        batch_id: u64,
        completion: OrderlyCompletion,
    ) -> Result<CompletionOutcome, ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_active(generation, &state)?;
        let Some(offsets) = state.in_flight_batches.get(&batch_id).cloned() else {
            return Err(ProcessQueueError::UnknownBatch);
        };
        if offsets.iter().any(|offset| {
            state
                .messages
                .get(offset)
                .is_none_or(|message| message.state != MessageState::InFlight(batch_id))
        }) {
            return Err(ProcessQueueError::BatchOwnershipLost);
        }

        state.in_flight_batches.remove(&batch_id);
        let mut completed_offsets = Vec::new();
        let mut requeued_offsets = Vec::new();
        let transition_offsets = match completion {
            OrderlyCompletion::CommitAll => state
                .messages
                .iter()
                .filter(|(_, message)| {
                    message.state == MessageState::Held
                        || message.state == MessageState::InFlight(batch_id)
                })
                .map(|(offset, _)| *offset)
                .collect::<Vec<_>>(),
            OrderlyCompletion::RollbackAll => state
                .messages
                .iter()
                .filter(|(_, message)| {
                    message.state == MessageState::Held
                        || message.state == MessageState::InFlight(batch_id)
                })
                .map(|(offset, _)| *offset)
                .collect::<Vec<_>>(),
            OrderlyCompletion::HoldCurrent | OrderlyCompletion::RequeueCurrent => offsets,
        };

        for offset in transition_offsets {
            let current_batch = state
                .messages
                .get(&offset)
                .is_some_and(|message| message.state == MessageState::InFlight(batch_id));
            let transition_held = completion == OrderlyCompletion::RollbackAll
                && state
                    .messages
                    .get(&offset)
                    .is_some_and(|message| message.state == MessageState::Held);
            if completion == OrderlyCompletion::CommitAll {
                if let Some(message) = state.messages.remove(&offset) {
                    state.cached_count = state.cached_count.saturating_sub(1);
                    state.cached_bytes = state.cached_bytes.saturating_sub(message.body_bytes);
                    completed_offsets.push(offset);
                }
            } else if current_batch || transition_held {
                if let Some(message) = state.messages.get_mut(&offset) {
                    match completion {
                        OrderlyCompletion::HoldCurrent => message.state = MessageState::Held,
                        OrderlyCompletion::RollbackAll | OrderlyCompletion::RequeueCurrent => {
                            message.state = MessageState::Ready;
                            requeued_offsets.push(offset);
                        }
                        OrderlyCompletion::CommitAll => unreachable!(),
                    }
                }
            }
        }

        let has_ready_messages = state
            .messages
            .values()
            .any(|message| message.state == MessageState::Ready);
        // Held messages are committed or rolled back only by a later listener
        // result. They must not keep dispatch latched when no ready message is
        // available, otherwise a future pull would never schedule a callback.
        state.consuming = has_ready_messages;
        state.last_consume_timestamp_millis = epoch_millis();
        let next_commit_offset = next_commit_offset(&state);
        Ok(CompletionOutcome {
            completed_offsets,
            requeued_offsets,
            next_commit_offset,
            has_ready_messages,
        })
    }

    pub(crate) fn increment_reconsume_times(
        &self,
        generation: u64,
        offset: i64,
    ) -> Result<i32, ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_active(generation, &state)?;
        let Some(entry) = state.messages.get_mut(&offset) else {
            return Err(ProcessQueueError::UnknownBatch);
        };
        let mut message = entry.message.as_ref().clone();
        let reconsume_times = message.reconsume_times().saturating_add(1);
        message.set_reconsume_times(reconsume_times);
        entry.message = Arc::new(message);
        Ok(reconsume_times)
    }

    pub(crate) fn drop_assignment(&self, generation: u64) -> Result<bool, ProcessQueueError> {
        if generation != self.generation {
            return Err(ProcessQueueError::StaleGeneration);
        }
        let mut state = self.lock_state();
        if state.dropped {
            return Ok(false);
        }
        state.dropped = true;
        state.consuming = false;
        state.lock_attempt_id = state.lock_attempt_id.wrapping_add(1);
        Ok(true)
    }

    pub(crate) fn clear(&self) {
        let mut state = self.lock_state();
        state.messages.clear();
        state.in_flight_batches.clear();
        state.cached_count = 0;
        state.cached_bytes = 0;
        state.highest_seen_offset = None;
        state.consuming = false;
    }

    pub(crate) fn release_consume_dispatch(
        &self,
        generation: u64,
    ) -> Result<(), ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_generation(generation)?;
        state.consuming = false;
        Ok(())
    }

    pub(crate) fn snapshot(
        &self,
        generation: u64,
    ) -> Result<ProcessQueueSnapshot, ProcessQueueError> {
        self.ensure_generation(generation)?;
        let state = self.lock_state();
        let minimum_offset = state.messages.first_key_value().map(|(offset, _)| *offset);
        let maximum_offset = state.messages.last_key_value().map(|(offset, _)| *offset);
        let max_span = minimum_offset
            .zip(maximum_offset)
            .map_or(0, |(min, max)| max.wrapping_sub(min));
        let in_flight_count = state
            .messages
            .values()
            .filter(|message| matches!(message.state, MessageState::InFlight(_)))
            .count();
        Ok(ProcessQueueSnapshot {
            queue: self.queue.clone(),
            generation: self.generation,
            cached_count: state.cached_count,
            cached_bytes: state.cached_bytes,
            minimum_offset,
            maximum_offset,
            max_span,
            next_commit_offset: next_commit_offset(&state),
            in_flight_count,
            dropped: state.dropped,
            consuming: state.consuming,
            last_pull_timestamp_millis: state.last_pull_timestamp_millis,
            last_consume_timestamp_millis: state.last_consume_timestamp_millis,
            locked: state.locked,
            last_lock_timestamp_millis: state.last_lock_timestamp_millis,
            try_unlock_times: state.try_unlock_times,
        })
    }

    pub(crate) fn set_last_pull_timestamp(
        &self,
        generation: u64,
        timestamp_millis: i64,
    ) -> Result<(), ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_active(generation, &state)?;
        state.last_pull_timestamp_millis = timestamp_millis;
        Ok(())
    }

    pub(crate) fn set_lock_state(
        &self,
        generation: u64,
        locked: bool,
        timestamp_millis: i64,
    ) -> Result<(), ProcessQueueError> {
        self.set_lock_state_at(
            generation,
            locked,
            timestamp_millis,
            locked.then(Instant::now),
        )
    }

    /// Sets local lock state using the instant the Broker response was
    /// received, which may precede initial-offset lookup during assignment.
    pub(crate) fn set_lock_state_at(
        &self,
        generation: u64,
        locked: bool,
        timestamp_millis: i64,
        lock_instant: Option<Instant>,
    ) -> Result<(), ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_active(generation, &state)?;
        state.locked = locked;
        if locked {
            state.last_lock_timestamp_millis = timestamp_millis;
            state.last_lock_instant = lock_instant;
        }
        Ok(())
    }

    /// Starts a lock request tied to this queue generation. The returned token
    /// prevents an older response from overwriting a newer lock decision.
    pub(crate) fn begin_lock_attempt(&self, generation: u64) -> Result<u64, ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_active(generation, &state)?;
        state.lock_attempt_id = state.lock_attempt_id.wrapping_add(1).max(1);
        Ok(state.lock_attempt_id)
    }

    /// Applies a Broker lock result only while the request still owns the
    /// current, active assignment generation.
    pub(crate) fn complete_lock_attempt(
        &self,
        generation: u64,
        attempt_id: u64,
        locked: bool,
    ) -> Result<LockAttemptResult, ProcessQueueError> {
        self.complete_lock_attempt_at(
            generation,
            attempt_id,
            locked,
            epoch_millis(),
            Instant::now(),
        )
    }

    /// Applies a Broker lock result using the instant its response arrived.
    /// Batch requests may continue across other brokers before local state is
    /// reconciled, so recording the later batch-completion time would extend
    /// the earlier Broker lock's effective lifetime.
    pub(crate) fn complete_lock_attempt_at(
        &self,
        generation: u64,
        attempt_id: u64,
        locked: bool,
        timestamp_millis: i64,
        lock_instant: Instant,
    ) -> Result<LockAttemptResult, ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_generation(generation)?;
        if state.dropped {
            return Ok(LockAttemptResult::Dropped);
        }
        if state.lock_attempt_id != attempt_id {
            return Ok(LockAttemptResult::Superseded);
        }
        state.locked = locked;
        if locked {
            state.last_lock_timestamp_millis = timestamp_millis;
            state.last_lock_instant = Some(lock_instant);
        }
        Ok(LockAttemptResult::Applied)
    }

    /// Records a confirmed unlock. This remains legal after drop so cleanup
    /// can finish without making the old assignment active again.
    pub(crate) fn record_unlocked(&self, generation: u64) -> Result<(), ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_generation(generation)?;
        state.locked = false;
        state.lock_attempt_id = state.lock_attempt_id.wrapping_add(1).max(1);
        Ok(())
    }

    pub(crate) fn is_lock_fresh(
        &self,
        generation: u64,
        max_live_time_millis: i64,
    ) -> Result<bool, ProcessQueueError> {
        self.ensure_generation(generation)?;
        let state = self.lock_state();
        if state.dropped || !state.locked {
            return Ok(false);
        }
        let Some(last_lock_instant) = state.last_lock_instant else {
            return Ok(false);
        };
        let elapsed_millis = Instant::now()
            .saturating_duration_since(last_lock_instant)
            .as_millis();
        Ok(i128::try_from(elapsed_millis).unwrap_or(i128::MAX) <= i128::from(max_live_time_millis))
    }

    /// Reports whether the most recent confirmed lock is older than Java's
    /// dropped-queue unlock delay. The timestamp remains available after an
    /// unlock response, matching ProcessQueue's retained lastLockTimestamp.
    pub(crate) fn is_lock_older_than(
        &self,
        generation: u64,
        unlock_delay_millis: i64,
    ) -> Result<bool, ProcessQueueError> {
        self.ensure_generation(generation)?;
        let state = self.lock_state();
        let Some(last_lock_instant) = state.last_lock_instant else {
            return Ok(false);
        };
        let elapsed_millis = Instant::now()
            .saturating_duration_since(last_lock_instant)
            .as_millis();
        Ok(i128::try_from(elapsed_millis).unwrap_or(i128::MAX) > i128::from(unlock_delay_millis))
    }

    pub(crate) fn increment_try_unlock_times(
        &self,
        generation: u64,
    ) -> Result<u64, ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_generation(generation)?;
        state.try_unlock_times = state.try_unlock_times.saturating_add(1);
        Ok(state.try_unlock_times)
    }

    fn allocate_batch_id(state: &mut ProcessQueueState) -> u64 {
        loop {
            let batch_id = state.next_batch_id.max(1);
            state.next_batch_id = batch_id.wrapping_add(1).max(1);
            if !state.in_flight_batches.contains_key(&batch_id) {
                return batch_id;
            }
        }
    }

    fn ensure_active(
        &self,
        generation: u64,
        state: &ProcessQueueState,
    ) -> Result<(), ProcessQueueError> {
        self.ensure_generation(generation)?;
        if state.dropped {
            return Err(ProcessQueueError::Dropped);
        }
        Ok(())
    }

    fn ensure_generation(&self, generation: u64) -> Result<(), ProcessQueueError> {
        if generation == self.generation {
            Ok(())
        } else {
            Err(ProcessQueueError::StaleGeneration)
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, ProcessQueueState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn next_commit_offset(state: &ProcessQueueState) -> Option<i64> {
    state
        .messages
        .first_key_value()
        .map(|(offset, _)| *offset)
        .or_else(|| {
            state
                .highest_seen_offset
                .map(|offset| offset.wrapping_add(1))
        })
}

fn epoch_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().min(i64::MAX as u128) as i64
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_classic_common::message::{Message, MessageExt, MessageQueue};

    // Oracle: ProcessQueueTest.java at the pinned Java 5.5.1 commit
    // efa1be59d6fc747ad220f036c86030f05f0419f9, SHA-256
    // 67ff9bd9913b76944c3e7a75d6f219525799b135049f7c86a1b6cace9111c762.
    // Java cases cover cached count/size and ordered take/commit; generation
    // rejection and gap-safe commit checks enforce the Phase 10 ownership contract.

    fn process_queue(generation: u64) -> ProcessQueue {
        ProcessQueue::new(MessageQueue::new("orders", "broker-a", 0), generation)
    }

    fn message(offset: i64, body_size: usize) -> MessageExt {
        let mut message =
            crate::test_support::message_ext_from(Message::new("orders", vec![b'x'; body_size]));
        message.set_queue_offset(offset);
        message
    }

    #[test]
    fn deduplicates_offsets_and_keeps_cached_count_bytes_and_batch_order_exact() {
        let process_queue = process_queue(17);
        let inserted = process_queue
            .insert_messages(17, vec![message(12, 1), message(10, 3), message(11, 2)])
            .unwrap();
        assert_eq!(inserted.inserted_count, 3);
        assert!(inserted.dispatch_to_consume);

        let duplicate = process_queue
            .insert_messages(17, vec![message(10, 7)])
            .unwrap();
        assert_eq!(duplicate.inserted_count, 0);
        assert!(!duplicate.dispatch_to_consume);

        let snapshot = process_queue.snapshot(17).unwrap();
        assert_eq!(snapshot.cached_count, 3);
        assert_eq!(snapshot.cached_bytes, 10);
        assert_eq!(snapshot.max_span, 2);
        assert_eq!(snapshot.next_commit_offset, Some(10));

        let batch = process_queue.take_batch(17, 8).unwrap().unwrap();
        assert_eq!(batch.offsets(), &[10, 11, 12]);
        assert_eq!(batch.messages()[0].body().len(), 7);
        let in_flight = process_queue.snapshot(17).unwrap();
        assert_eq!(in_flight.cached_count, 3);
        assert_eq!(in_flight.cached_bytes, 10);
        assert_eq!(in_flight.in_flight_count, 3);

        let completed = process_queue
            .complete_batch(17, batch.id(), &[10, 11, 12])
            .unwrap();
        assert_eq!(completed.next_commit_offset, Some(13));
        assert_eq!(process_queue.snapshot(17).unwrap().cached_count, 0);
        assert_eq!(process_queue.snapshot(17).unwrap().cached_bytes, 0);
    }

    #[test]
    fn gaps_and_out_of_order_batches_never_commit_past_unresolved_messages() {
        let process_queue = process_queue(18);
        process_queue
            .insert_messages(18, vec![message(5, 1), message(7, 1), message(8, 1)])
            .unwrap();

        let first = process_queue.take_batch(18, 1).unwrap().unwrap();
        let later = process_queue.take_batch(18, 2).unwrap().unwrap();
        assert_eq!(first.offsets(), &[5]);
        assert_eq!(later.offsets(), &[7, 8]);

        let later_completed = process_queue
            .complete_batch(18, later.id(), &[7, 8])
            .unwrap();
        assert_eq!(later_completed.next_commit_offset, Some(5));
        assert_eq!(process_queue.snapshot(18).unwrap().cached_count, 1);

        let first_completed = process_queue.complete_batch(18, first.id(), &[5]).unwrap();
        assert_eq!(first_completed.next_commit_offset, Some(9));
        assert_eq!(process_queue.snapshot(18).unwrap().cached_count, 0);
    }

    #[test]
    fn partial_completion_requeues_only_unresolved_messages() {
        let process_queue = process_queue(19);
        process_queue
            .insert_messages(19, vec![message(20, 2), message(21, 3), message(22, 4)])
            .unwrap();
        let batch = process_queue.take_batch(19, 3).unwrap().unwrap();

        let partially_completed = process_queue
            .complete_batch(19, batch.id(), &[20, 22])
            .unwrap();
        assert_eq!(partially_completed.next_commit_offset, Some(21));
        assert_eq!(partially_completed.requeued_offsets, vec![21]);
        let snapshot = process_queue.snapshot(19).unwrap();
        assert_eq!(snapshot.cached_count, 1);
        assert_eq!(snapshot.cached_bytes, 3);

        let retry = process_queue.take_batch(19, 3).unwrap().unwrap();
        assert_eq!(retry.offsets(), &[21]);
        let completed = process_queue.complete_batch(19, retry.id(), &[21]).unwrap();
        assert_eq!(completed.next_commit_offset, Some(23));
    }

    #[test]
    fn orderly_success_holds_offsets_until_a_later_commit() {
        let process_queue = process_queue(23);
        process_queue
            .insert_messages(23, vec![message(10, 1), message(11, 1)])
            .unwrap();
        let first = process_queue.take_batch(23, 1).unwrap().unwrap();
        let held = process_queue
            .complete_orderly_batch(23, first.id(), OrderlyCompletion::HoldCurrent)
            .unwrap();
        assert_eq!(held.next_commit_offset, Some(10));
        assert_eq!(process_queue.snapshot(23).unwrap().cached_count, 2);

        let second = process_queue.take_batch(23, 1).unwrap().unwrap();
        assert_eq!(second.offsets(), &[11]);
        let committed = process_queue
            .complete_orderly_batch(23, second.id(), OrderlyCompletion::CommitAll)
            .unwrap();
        assert_eq!(committed.completed_offsets, vec![10, 11]);
        assert_eq!(committed.next_commit_offset, Some(12));
        assert_eq!(process_queue.snapshot(23).unwrap().cached_count, 0);
    }

    #[test]
    fn orderly_rollback_restores_held_and_current_messages_in_offset_order() {
        let process_queue = process_queue(24);
        process_queue
            .insert_messages(24, vec![message(10, 1), message(11, 1), message(12, 1)])
            .unwrap();
        let first = process_queue.take_batch(24, 1).unwrap().unwrap();
        process_queue
            .complete_orderly_batch(24, first.id(), OrderlyCompletion::HoldCurrent)
            .unwrap();
        let second = process_queue.take_batch(24, 1).unwrap().unwrap();
        let rolled_back = process_queue
            .complete_orderly_batch(24, second.id(), OrderlyCompletion::RollbackAll)
            .unwrap();
        assert_eq!(rolled_back.requeued_offsets, vec![10, 11]);

        let replay = process_queue.take_batch(24, 3).unwrap().unwrap();
        assert_eq!(replay.offsets(), &[10, 11, 12]);
    }

    #[test]
    fn stale_and_dropped_lock_responses_cannot_restore_queue_ownership() {
        let process_queue = process_queue(25);
        let first = process_queue.begin_lock_attempt(25).unwrap();
        let current = process_queue.begin_lock_attempt(25).unwrap();
        assert_eq!(
            process_queue.complete_lock_attempt(25, first, true),
            Ok(LockAttemptResult::Superseded)
        );
        assert_eq!(
            process_queue.complete_lock_attempt(25, current, true),
            Ok(LockAttemptResult::Applied)
        );
        assert!(process_queue.snapshot(25).unwrap().locked);

        let late = process_queue.begin_lock_attempt(25).unwrap();
        process_queue.drop_assignment(25).unwrap();
        assert_eq!(
            process_queue.complete_lock_attempt(25, late, true),
            Ok(LockAttemptResult::Dropped)
        );
        assert!(process_queue.snapshot(25).unwrap().dropped);
    }

    #[test]
    fn reset_clear_discards_cached_and_in_flight_queue_messages() {
        let process_queue = process_queue(28);
        process_queue
            .insert_messages(28, vec![message(30, 1), message(31, 2)])
            .unwrap();
        let batch = process_queue.take_batch(28, 2).unwrap().unwrap();
        assert_eq!(process_queue.snapshot(28).unwrap().in_flight_count, 2);

        process_queue.drop_assignment(28).unwrap();
        process_queue.clear();

        let snapshot = process_queue.snapshot(28).unwrap();
        assert_eq!(snapshot.cached_count, 0);
        assert_eq!(snapshot.cached_bytes, 0);
        assert_eq!(snapshot.in_flight_count, 0);
        assert_eq!(snapshot.next_commit_offset, None);
        assert!(matches!(
            process_queue.complete_batch(28, batch.id(), &[30, 31]),
            Err(ProcessQueueError::Dropped)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn orderly_lock_expires_only_after_the_java_max_live_boundary() {
        let process_queue = process_queue(26);
        process_queue.set_lock_state(26, true, 1_000).unwrap();
        assert!(process_queue.is_lock_fresh(26, 30_000).unwrap());
        tokio::time::advance(std::time::Duration::from_millis(30_000)).await;
        assert!(process_queue.is_lock_fresh(26, 30_000).unwrap());
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        assert!(!process_queue.is_lock_fresh(26, 30_000).unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn dropped_queue_unlock_delay_uses_the_java_strict_boundary() {
        let process_queue = process_queue(27);
        process_queue.set_lock_state(27, true, 1_000).unwrap();
        process_queue.record_unlocked(27).unwrap();
        assert!(!process_queue.is_lock_older_than(27, 20_000).unwrap());
        tokio::time::advance(std::time::Duration::from_millis(20_000)).await;
        assert!(!process_queue.is_lock_older_than(27, 20_000).unwrap());
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        assert!(process_queue.is_lock_older_than(27, 20_000).unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn never_locked_queue_unlock_delay_starts_at_process_queue_creation() {
        let process_queue = process_queue(28);
        assert!(!process_queue.is_lock_older_than(28, 20_000).unwrap());
        tokio::time::advance(std::time::Duration::from_millis(20_000)).await;
        assert!(!process_queue.is_lock_older_than(28, 20_000).unwrap());
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        assert!(process_queue.is_lock_older_than(28, 20_000).unwrap());
    }

    #[test]
    fn dropped_generation_rejects_late_work_without_affecting_replacement_queue() {
        let old_queue = process_queue(20);
        old_queue.insert_messages(20, vec![message(4, 4)]).unwrap();
        let batch = old_queue.take_batch(20, 1).unwrap().unwrap();
        assert!(old_queue.drop_assignment(20).unwrap());

        assert_eq!(
            old_queue.complete_batch(20, batch.id(), &[4]),
            Err(ProcessQueueError::Dropped)
        );
        assert_eq!(
            old_queue.insert_messages(21, vec![message(100, 1)]),
            Err(ProcessQueueError::StaleGeneration)
        );
        let dropped = old_queue.snapshot(20).unwrap();
        assert!(dropped.dropped);
        assert_eq!(dropped.cached_count, 1);
        assert_eq!(dropped.next_commit_offset, Some(4));

        let replacement = process_queue(21);
        replacement
            .insert_messages(21, vec![message(100, 1)])
            .unwrap();
        assert_eq!(
            replacement.snapshot(21).unwrap().next_commit_offset,
            Some(100)
        );
        assert_eq!(old_queue.snapshot(20).unwrap().cached_count, 1);
    }
}
