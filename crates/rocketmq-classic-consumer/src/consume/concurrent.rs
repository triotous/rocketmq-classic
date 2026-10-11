use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::{sync::mpsc, task::JoinHandle, time::Instant};

use crate::{
    process_queue::{ConsumeBatch, ProcessQueue},
    push::MessageListenerConcurrently,
};

const CONSUME_REQUEST_QUEUE_CAPACITY: usize = 256;

pub(crate) struct ConsumeRequest {
    pub(crate) process_queue: Arc<ProcessQueue>,
    pub(crate) batch: ConsumeBatch,
}

pub(crate) enum ConsumeSubmitError {
    Full(ConsumeRequest),
    Closed(ConsumeRequest),
}

struct ConsumeWorkerSet {
    sender: Option<mpsc::Sender<ConsumeRequest>>,
    workers: Vec<JoinHandle<()>>,
}

/// Owns a bounded set of concurrent-listener workers and their pending work.
pub(crate) struct ConcurrentConsumeService {
    minimum_threads: usize,
    maximum_threads: usize,
    listener: Arc<dyn MessageListenerConcurrently>,
    workers: Mutex<Option<ConsumeWorkerSet>>,
    started: AtomicBool,
}

impl ConcurrentConsumeService {
    pub fn new(
        minimum_threads: i32,
        maximum_threads: i32,
        listener: Arc<dyn MessageListenerConcurrently>,
    ) -> Self {
        Self {
            minimum_threads: minimum_threads.max(1) as usize,
            maximum_threads: maximum_threads.max(1) as usize,
            listener,
            workers: Mutex::new(None),
            started: AtomicBool::new(false),
        }
    }

    pub async fn start(&self, consumer: std::sync::Weak<crate::push::PushConsumer>) {
        if self.started.swap(true, Ordering::AcqRel) {
            return;
        }
        let worker_count = self.minimum_threads.min(self.maximum_threads);
        let (sender, receiver) = mpsc::channel(CONSUME_REQUEST_QUEUE_CAPACITY);
        let receiver = Arc::new(tokio::sync::Mutex::new(receiver));
        let mut workers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let receiver = receiver.clone();
            let consumer = consumer.clone();
            let listener = self.listener.clone();
            workers.push(tokio::spawn(async move {
                loop {
                    let request = receiver.lock().await.recv().await;
                    let Some(request) = request else {
                        break;
                    };
                    let Some(consumer) = consumer.upgrade() else {
                        continue;
                    };
                    consumer
                        .run_consume_request(request, listener.clone())
                        .await;
                }
            }));
        }
        *self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ConsumeWorkerSet {
            sender: Some(sender),
            workers,
        });
    }

    pub(crate) fn try_submit(&self, request: ConsumeRequest) -> Result<(), ConsumeSubmitError> {
        let mut workers = self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(sender) = workers.as_mut().and_then(|workers| workers.sender.as_ref()) else {
            return Err(ConsumeSubmitError::Closed(request));
        };
        sender.try_send(request).map_err(|error| match error {
            mpsc::error::TrySendError::Full(request) => ConsumeSubmitError::Full(request),
            mpsc::error::TrySendError::Closed(request) => ConsumeSubmitError::Closed(request),
        })
    }

    pub async fn shutdown(&self, await_termination_millis: i64) {
        if !self.started.swap(false, Ordering::AcqRel) {
            return;
        }
        let Some(mut workers) = self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        else {
            return;
        };
        workers.sender.take();
        let deadline =
            Instant::now() + Duration::from_millis(await_termination_millis.max(0) as u64);
        for mut worker in workers.workers.drain(..) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || tokio::time::timeout(remaining, &mut worker).await.is_err() {
                worker.abort();
                let _ = worker.await;
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn worker_count(&self) -> usize {
        self.workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map_or(0, |workers| workers.workers.len())
    }
}

impl Drop for ConcurrentConsumeService {
    fn drop(&mut self) {
        if let Some(mut workers) = self
            .workers
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            workers.sender.take();
            for worker in workers.workers.drain(..) {
                worker.abort();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::push::{ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus};
    use rocketmq_classic_common::message::MessageExt;

    #[tokio::test]
    async fn service_starts_a_bounded_worker_set_and_joins_it_on_shutdown() {
        let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
            |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| async move {
                (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
            },
        );
        let service = ConcurrentConsumeService::new(3, 7, listener);
        service
            .start(std::sync::Weak::<crate::push::PushConsumer>::new())
            .await;
        assert_eq!(service.worker_count(), 3);
        service.shutdown(100).await;
        assert_eq!(service.worker_count(), 0);
    }
}
