use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Instant, timeout},
};

use crate::push::MessageListenerConcurrently;

/// Owns the initial concurrent-listener worker set created during Push startup.
/// P10-06 attaches bounded consume requests to these workers.
pub(crate) struct ConcurrentConsumeService {
    minimum_threads: usize,
    maximum_threads: usize,
    listener: Arc<dyn MessageListenerConcurrently>,
    cancellation: Mutex<Option<watch::Sender<bool>>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
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
            cancellation: Mutex::new(None),
            workers: Mutex::new(Vec::new()),
            started: AtomicBool::new(false),
        }
    }

    pub async fn start(&self) {
        if self.started.swap(true, Ordering::AcqRel) {
            return;
        }
        let (cancellation, receiver) = watch::channel(false);
        let worker_count = self.minimum_threads.min(self.maximum_threads);
        let mut workers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let mut receiver = receiver.clone();
            let listener = self.listener.clone();
            workers.push(tokio::spawn(async move {
                let _listener_lifetime = listener;
                loop {
                    if *receiver.borrow() || receiver.changed().await.is_err() {
                        break;
                    }
                }
            }));
        }
        *self
            .cancellation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(cancellation);
        *self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = workers;
    }

    pub async fn shutdown(&self, await_termination_millis: i64) {
        if !self.started.swap(false, Ordering::AcqRel) {
            return;
        }
        if let Some(cancellation) = self
            .cancellation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            cancellation.send_replace(true);
        }
        let mut workers = std::mem::take(
            &mut *self
                .workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        let wait = Duration::from_millis(await_termination_millis.max(0) as u64);
        let deadline = Instant::now() + wait;
        for mut worker in workers.drain(..) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let timed_out = remaining.is_zero() || timeout(remaining, &mut worker).await.is_err();
            if timed_out {
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
            .len()
    }
}

impl Drop for ConcurrentConsumeService {
    fn drop(&mut self) {
        if let Some(cancellation) = self
            .cancellation
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            cancellation.send_replace(true);
        }
        for worker in self
            .workers
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain(..)
        {
            worker.abort();
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
        service.start().await;
        assert_eq!(service.worker_count(), 3);
        service.shutdown(100).await;
        assert_eq!(service.worker_count(), 0);
    }
}
