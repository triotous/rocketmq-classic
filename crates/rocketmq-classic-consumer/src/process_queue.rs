// P10-06 attaches these queue operations to bounded consume requests. Keep
// this owner private until the callback lifecycle is complete.
#![allow(dead_code)]

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Arc, Mutex, MutexGuard},
    time::{SystemTime, UNIX_EPOCH},
};

use rocketmq_classic_common::message::{MessageExt, MessageQueue};

/// A single queue assignment's retained messages and in-flight consume work.
/// Every operation that can change ownership state is scoped to its generation.
pub(crate) struct ProcessQueue {
    queue: MessageQueue,
    generation: u64,
    state: Mutex<ProcessQueueState>,
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
                try_unlock_times: 0,
            }),
        }
    }

    pub(crate) fn queue(&self) -> &MessageQueue {
        &self.queue
    }

    pub(crate) const fn generation(&self) -> u64 {
        self.generation
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
            let body_bytes = message.message().body().len() as u64;
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
                Some((MessageState::InFlight(_), _)) => {}
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
        Ok(true)
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
        let mut state = self.lock_state();
        self.ensure_active(generation, &state)?;
        state.locked = locked;
        state.last_lock_timestamp_millis = timestamp_millis;
        Ok(())
    }

    pub(crate) fn increment_try_unlock_times(
        &self,
        generation: u64,
    ) -> Result<u64, ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_active(generation, &state)?;
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
        let mut message = MessageExt::new(Message::new("orders", vec![b'x'; body_size]));
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
        assert_eq!(batch.messages()[0].message().body().len(), 7);
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
