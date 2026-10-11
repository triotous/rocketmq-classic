use std::{
    collections::HashMap,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use rocketmq_classic_common::message::{Message, MessageQueue};
use rocketmq_classic_core::api::producer::{SendMode, SendResult};
use tokio::{
    sync::{Mutex, oneshot},
    time::Instant,
};

use crate::ProducerError;

pub(crate) type BatchResponse = Result<SendResult, String>;
pub(crate) type BatchReceiver = oneshot::Receiver<BatchResponse>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BatchKey {
    pub(crate) topic: String,
    pub(crate) queue: Option<MessageQueue>,
    pub(crate) wait_store_msg_ok: bool,
    pub(crate) tag: Option<String>,
    pub(crate) mode: SendMode,
}

impl BatchKey {
    pub(crate) fn new(message: &Message, queue: Option<MessageQueue>, mode: SendMode) -> Self {
        Self {
            topic: message.topic().to_owned(),
            queue,
            wait_store_msg_ok: message.is_wait_store_msg_ok(),
            tag: message.tags().map(str::to_owned),
            mode,
        }
    }
}

struct PendingBatch {
    generation: u64,
    created_at: Instant,
    body_bytes: usize,
    messages: Vec<Message>,
    waiters: Vec<oneshot::Sender<BatchResponse>>,
}

pub(crate) struct ReadyBatch {
    pub(crate) key: BatchKey,
    pub(crate) messages: Vec<Message>,
    pub(crate) waiters: Vec<oneshot::Sender<BatchResponse>>,
    pub(crate) body_bytes: usize,
}

pub(crate) enum EnqueueResult {
    Bypass,
    Queued {
        receiver: BatchReceiver,
        key: BatchKey,
        generation: u64,
        flush_at: Instant,
        schedule_timer: bool,
    },
    Ready {
        receiver: BatchReceiver,
        batch: ReadyBatch,
    },
}

#[derive(Default)]
struct AccumulatorState {
    entries: HashMap<BatchKey, PendingBatch>,
    current_hold_size: usize,
    next_generation: u64,
    closed: bool,
}

/// Rust-native pending batches. Keys and flush thresholds mirror the pinned
/// Java `ProduceAccumulator`; each waiter receives its message's result.
#[derive(Default)]
pub(crate) struct AutoBatchAccumulator {
    state: Mutex<AccumulatorState>,
    closed: AtomicBool,
}

impl AutoBatchAccumulator {
    pub(crate) async fn enqueue(
        &self,
        message: Message,
        queue: Option<MessageQueue>,
        mode: SendMode,
        max_delay: Duration,
        max_bytes: usize,
        total_max_bytes: usize,
    ) -> Result<EnqueueResult, ProducerError> {
        let (sender, receiver) = oneshot::channel();
        let key = BatchKey::new(&message, queue, mode);
        let now = Instant::now();
        let message_size = message.body().len();
        let mut state = self.state.lock().await;
        if state.closed {
            return Err(ProducerError::Shutdown);
        }

        // Java checks the current hold size before adding the next message.
        if state.current_hold_size >= total_max_bytes {
            return Ok(EnqueueResult::Bypass);
        }
        state.current_hold_size = state.current_hold_size.saturating_add(message_size);

        let is_new = !state.entries.contains_key(&key);
        if is_new {
            state.next_generation = state.next_generation.wrapping_add(1);
        }
        let generation = state.next_generation;
        let pending = state
            .entries
            .entry(key.clone())
            .or_insert_with(|| PendingBatch {
                generation,
                created_at: now,
                body_bytes: 0,
                messages: Vec::new(),
                waiters: Vec::new(),
            });
        pending.body_bytes = pending.body_bytes.saturating_add(message_size);
        pending.messages.push(message);
        pending.waiters.push(sender);
        let generation = pending.generation;
        let flush_at = pending.created_at + max_delay;
        let should_flush = pending.body_bytes > max_bytes;

        if should_flush {
            let Some(pending) = state.entries.remove(&key) else {
                state.current_hold_size = state.current_hold_size.saturating_sub(message_size);
                return Err(ProducerError::AutoBatchSend(
                    "accumulator batch disappeared before flush".into(),
                ));
            };
            Ok(EnqueueResult::Ready {
                receiver,
                batch: ReadyBatch {
                    key,
                    messages: pending.messages,
                    waiters: pending.waiters,
                    body_bytes: pending.body_bytes,
                },
            })
        } else {
            Ok(EnqueueResult::Queued {
                receiver,
                key,
                generation,
                flush_at,
                schedule_timer: is_new,
            })
        }
    }

    pub(crate) async fn take_due(&self, key: &BatchKey, generation: u64) -> Option<ReadyBatch> {
        let mut state = self.state.lock().await;
        let is_due = state
            .entries
            .get(key)
            .is_some_and(|batch| batch.generation == generation);
        if !is_due {
            return None;
        }
        let batch = state.entries.remove(key)?;
        Some(ReadyBatch {
            key: key.clone(),
            messages: batch.messages,
            waiters: batch.waiters,
            body_bytes: batch.body_bytes,
        })
    }

    pub(crate) async fn release(&self, body_bytes: usize) {
        let mut state = self.state.lock().await;
        state.current_hold_size = state.current_hold_size.saturating_sub(body_bytes);
    }

    pub(crate) async fn close(&self) {
        self.closed.store(true, Ordering::Release);
        let mut state = self.state.lock().await;
        state.closed = true;
        state.current_hold_size = 0;
        state.entries.clear();
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_classic_common::message::Message;

    #[tokio::test]
    async fn batches_share_only_matching_route_tag_wait_store_and_mode() {
        let accumulator = AutoBatchAccumulator::default();
        let mut first = Message::new("orders", vec![1; 10]);
        first.set_tags("blue").unwrap();
        let first_result = accumulator
            .enqueue(
                first,
                None,
                SendMode::Sync,
                Duration::from_secs(1),
                1_000,
                10_000,
            )
            .await
            .unwrap();
        let EnqueueResult::Queued {
            key, generation, ..
        } = first_result
        else {
            panic!("first message should wait for batching");
        };

        let mut second = Message::new("orders", vec![2; 10]);
        second.set_tags("blue").unwrap();
        let second_result = accumulator
            .enqueue(
                second,
                None,
                SendMode::Sync,
                Duration::from_secs(1),
                1_000,
                10_000,
            )
            .await
            .unwrap();
        let EnqueueResult::Queued {
            key: second_key, ..
        } = second_result
        else {
            panic!("matching message should join the first batch");
        };
        assert_eq!(key, second_key);

        let mut differently_tagged = Message::new("orders", vec![3; 10]);
        differently_tagged.set_tags("green").unwrap();
        let different_result = accumulator
            .enqueue(
                differently_tagged,
                None,
                SendMode::Sync,
                Duration::from_secs(1),
                1_000,
                10_000,
            )
            .await
            .unwrap();
        let EnqueueResult::Queued {
            key: different_key, ..
        } = different_result
        else {
            panic!("different tag should create a distinct batch");
        };
        assert_ne!(key, different_key);

        let ready = accumulator.take_due(&key, generation).await.unwrap();
        assert_eq!(ready.messages.len(), 2);
        assert_eq!(ready.body_bytes, 20);
        assert_eq!(ready.waiters.len(), 2);
    }

    #[tokio::test]
    async fn byte_threshold_flushes_and_total_hold_limit_bypasses() {
        let accumulator = AutoBatchAccumulator::default();
        let first = accumulator
            .enqueue(
                Message::new("orders", vec![1; 8]),
                None,
                SendMode::Sync,
                Duration::from_secs(1),
                8,
                100,
            )
            .await
            .unwrap();
        assert!(matches!(first, EnqueueResult::Queued { .. }));
        let second = accumulator
            .enqueue(
                Message::new("orders", vec![2; 1]),
                None,
                SendMode::Sync,
                Duration::from_secs(1),
                8,
                100,
            )
            .await
            .unwrap();
        assert!(matches!(second, EnqueueResult::Ready { .. }));

        let full = accumulator
            .enqueue(
                Message::new("payments", vec![3; 1]),
                None,
                SendMode::Sync,
                Duration::from_secs(1),
                8,
                9,
            )
            .await
            .unwrap();
        assert!(matches!(full, EnqueueResult::Bypass));
    }

    #[tokio::test]
    async fn pending_batch_becomes_ready_at_its_configured_hold_deadline() {
        let accumulator = AutoBatchAccumulator::default();
        let EnqueueResult::Queued {
            key,
            generation,
            flush_at,
            ..
        } = accumulator
            .enqueue(
                Message::new("orders", vec![1; 8]),
                None,
                SendMode::Sync,
                Duration::from_millis(1),
                1_000,
                10_000,
            )
            .await
            .unwrap()
        else {
            panic!("message should wait for its hold deadline");
        };

        tokio::time::sleep_until(flush_at).await;
        let ready = accumulator.take_due(&key, generation).await.unwrap();
        assert_eq!(ready.messages.len(), 1);
        assert_eq!(ready.body_bytes, 8);
    }

    #[tokio::test]
    async fn close_fails_pending_waiters_and_rejects_new_messages() {
        let accumulator = AutoBatchAccumulator::default();
        let EnqueueResult::Queued { receiver, .. } = accumulator
            .enqueue(
                Message::new("orders", vec![1]),
                None,
                SendMode::Future,
                Duration::from_secs(1),
                100,
                100,
            )
            .await
            .unwrap()
        else {
            panic!("message should queue");
        };
        accumulator.close().await;
        assert!(receiver.await.is_err());
        assert!(matches!(
            accumulator
                .enqueue(
                    Message::new("orders", vec![1]),
                    None,
                    SendMode::Sync,
                    Duration::from_secs(1),
                    100,
                    100,
                )
                .await,
            Err(ProducerError::Shutdown)
        ));
    }
}
