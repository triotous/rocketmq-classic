use std::future::Future;
use std::sync::Arc;

use crate::{push::PushConsumer, rebalance::strategy::AllocateMessageQueueStrategy};

/// Serializes assignment work for one Push consumer. Wakeups and periodic
/// timing remain owned by the shared client runtime.
pub(crate) struct RebalanceService {
    strategy: Arc<dyn AllocateMessageQueueStrategy>,
    execution: tokio::sync::Mutex<()>,
}

impl RebalanceService {
    pub(crate) fn new(strategy: Arc<dyn AllocateMessageQueueStrategy>) -> Self {
        Self {
            strategy,
            execution: tokio::sync::Mutex::new(()),
        }
    }

    pub(crate) async fn rebalance(&self, consumer: &Arc<PushConsumer>) -> bool {
        self.execute_serialized(consumer.rebalance_once(self.strategy.as_ref()))
            .await
    }

    async fn execute_serialized<T>(&self, work: impl Future<Output = T>) -> T {
        let _execution = self.execution.lock().await;
        work.await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rebalance::AllocateMessageQueueAveragely;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{Duration, sleep};

    #[tokio::test]
    async fn concurrent_rebalance_triggers_run_one_at_a_time() {
        let service = Arc::new(RebalanceService::new(Arc::new(
            AllocateMessageQueueAveragely,
        )));
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let run = |service: Arc<RebalanceService>| {
            let active = active.clone();
            let maximum = maximum.clone();
            tokio::spawn(async move {
                service
                    .execute_serialized(async move {
                        let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                        maximum.fetch_max(current, Ordering::SeqCst);
                        sleep(Duration::from_millis(5)).await;
                        active.fetch_sub(1, Ordering::SeqCst);
                    })
                    .await;
            })
        };

        let first = run(service.clone());
        let second = run(service);
        first.await.unwrap();
        second.await.unwrap();
        assert_eq!(maximum.load(Ordering::SeqCst), 1);
    }
}
