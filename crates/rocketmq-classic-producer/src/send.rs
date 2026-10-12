use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::Arc,
    time::{Duration, Instant as StdInstant, SystemTime, UNIX_EPOCH},
};

use rocketmq_classic_common::{
    message::{Message, MessageQueue, constants::*, encode_properties},
    sysflag::message,
};
use rocketmq_classic_core::{
    api::producer::{SendKind, SendMode, SendResult, SendStatus},
    error::RpcError,
    route::{TopicSnapshot, broker_vip_channel},
    runtime::ClientRuntime,
};
use rocketmq_classic_protocol::header::SendMessageRequestHeaderV1;
use tokio::{sync::OwnedSemaphorePermit, time::Instant};

use crate::{
    Producer, ProducerError,
    accumulator::{BatchKey, EnqueueResult, ReadyBatch},
    batch::create_batch,
    validation,
};

#[cfg(test)]
use crate::producer::ScriptedSendAttempt;

struct PreparedMessage {
    topic: String,
    body: Vec<u8>,
    properties: Option<String>,
    unique_id: String,
    born_timestamp: i64,
    flag: i32,
    sys_flag: i32,
    reconsume_times: Option<i32>,
    max_reconsume_times: Option<i32>,
    batch: bool,
}

struct SendAttempt<'a> {
    runtime: &'a ClientRuntime,
    queue: &'a MessageQueue,
    broker: &'a str,
    address: &'a str,
    prepared: &'a PreparedMessage,
    mode: SendMode,
    deadline: Instant,
}

impl Producer {
    async fn acquire_async_backpressure(
        &self,
        message_size: usize,
        deadline: Instant,
    ) -> Result<Option<(OwnedSemaphorePermit, OwnedSemaphorePermit)>, ProducerError> {
        if !self.config.enable_backpressure_for_async_mode() {
            return Ok(None);
        }
        let count =
            tokio::time::timeout_at(deadline, self.backpressure_send_num.clone().acquire_owned())
                .await
                .map_err(|_| ProducerError::Timeout)?
                .map_err(|_| ProducerError::Shutdown)?;
        let size = u32::try_from(message_size.max(1)).map_err(|_| ProducerError::Timeout)?;
        let bytes = tokio::time::timeout_at(
            deadline,
            self.backpressure_send_size.clone().acquire_many_owned(size),
        )
        .await
        .map_err(|_| ProducerError::Timeout)?
        .map_err(|_| ProducerError::Shutdown)?;
        Ok(Some((count, bytes)))
    }

    /// Refreshes the topic route from NameServer and returns writable queues,
    /// matching Java's `fetchPublishMessageQueues` behavior.
    pub async fn fetch_publish_message_queues(
        &self,
        topic: &str,
    ) -> Result<Vec<MessageQueue>, ProducerError> {
        let runtime = self.started_runtime().await?;
        let timeout = Duration::from_millis(
            self.config.client_config().mq_client_api_timeout().max(0) as u64,
        );
        let deadline = deadline_after(timeout);
        let updated = runtime.update_topic_route(topic, deadline).await?;
        if !updated {
            return Err(ProducerError::NoRoute(topic.to_owned()));
        }
        let Some(info) = self.current_publish_info(&runtime, topic)? else {
            return Err(ProducerError::NoRoute(topic.to_owned()));
        };
        if info.publish_queues.is_empty() {
            return Err(ProducerError::NoRoute(topic.to_owned()));
        }
        Ok(info.publish_queues.clone())
    }

    /// Sync-mode send. The returned future waits for the Java sync-mode result
    /// and uses the configured synchronous retry budget.
    pub async fn send(&self, message: &Message) -> Result<SendResult, ProducerError> {
        self.send_auto_or_direct(message, None, SendMode::Sync)
            .await
    }

    pub async fn send_with_timeout(
        &self,
        message: &Message,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        self.send_default(message, false, SendMode::Sync, timeout)
            .await?
            .ok_or(ProducerError::Timeout)
    }

    /// Future-mode send. Like Java's callback send, it retries only transport
    /// failures; this Rust future resolves with the final callback outcome.
    pub async fn send_future(&self, message: &Message) -> Result<SendResult, ProducerError> {
        self.send_auto_or_direct(message, None, SendMode::Future)
            .await
    }

    pub async fn send_future_with_timeout(
        &self,
        message: &Message,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        self.send_default(message, false, SendMode::Future, timeout)
            .await?
            .ok_or(ProducerError::Timeout)
    }

    pub async fn send_oneway(&self, message: &Message) -> Result<(), ProducerError> {
        self.send_oneway_with_timeout(
            message,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
        )
        .await
    }

    pub async fn send_oneway_with_timeout(
        &self,
        message: &Message,
        timeout: Duration,
    ) -> Result<(), ProducerError> {
        self.send_default(message, false, SendMode::Oneway, timeout)
            .await
            .map(|_| ())
    }

    /// Sends to a caller-selected queue after validating membership in the
    /// current writable route. This overload makes one Broker attempt.
    pub async fn send_to_queue(
        &self,
        message: &Message,
        queue: &MessageQueue,
    ) -> Result<SendResult, ProducerError> {
        self.send_auto_or_direct(message, Some(queue.clone()), SendMode::Sync)
            .await
    }

    pub async fn send_to_queue_with_timeout(
        &self,
        message: &Message,
        queue: &MessageQueue,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        self.send_to_queue_mode(message, queue, false, SendMode::Sync, timeout)
            .await?
            .ok_or(ProducerError::Timeout)
    }

    /// Future-mode counterpart to Java's explicit-queue callback overload.
    /// It makes one Broker attempt, as the explicit-queue Java path does.
    pub async fn send_to_queue_future(
        &self,
        message: &Message,
        queue: &MessageQueue,
    ) -> Result<SendResult, ProducerError> {
        self.send_auto_or_direct(message, Some(queue.clone()), SendMode::Future)
            .await
    }

    pub async fn send_to_queue_future_with_timeout(
        &self,
        message: &Message,
        queue: &MessageQueue,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        self.send_to_queue_mode(message, queue, false, SendMode::Future, timeout)
            .await?
            .ok_or(ProducerError::Timeout)
    }

    pub async fn send_to_queue_oneway(
        &self,
        message: &Message,
        queue: &MessageQueue,
    ) -> Result<(), ProducerError> {
        self.send_to_queue_mode(
            message,
            queue,
            false,
            SendMode::Oneway,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
        )
        .await
        .map(|_| ())
    }

    /// Runs a custom queue selector against the current writable route and
    /// rejects a queue that does not belong to that route.
    pub async fn send_with_selector<F>(
        &self,
        message: &Message,
        selector: F,
    ) -> Result<SendResult, ProducerError>
    where
        F: FnOnce(&[MessageQueue], &Message) -> Option<MessageQueue> + Send,
    {
        self.send_with_selector_mode(
            message,
            selector,
            SendMode::Sync,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
            true,
        )
        .await?
        .ok_or(ProducerError::Timeout)
    }

    pub async fn send_with_selector_with_timeout<F>(
        &self,
        message: &Message,
        selector: F,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError>
    where
        F: FnOnce(&[MessageQueue], &Message) -> Option<MessageQueue> + Send,
    {
        self.send_with_selector_mode(message, selector, SendMode::Sync, timeout, false)
            .await?
            .ok_or(ProducerError::Timeout)
    }

    pub async fn send_future_with_selector<F>(
        &self,
        message: &Message,
        selector: F,
    ) -> Result<SendResult, ProducerError>
    where
        F: FnOnce(&[MessageQueue], &Message) -> Option<MessageQueue> + Send,
    {
        self.send_with_selector_mode(
            message,
            selector,
            SendMode::Future,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
            true,
        )
        .await?
        .ok_or(ProducerError::Timeout)
    }

    pub async fn send_future_with_selector_with_timeout<F>(
        &self,
        message: &Message,
        selector: F,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError>
    where
        F: FnOnce(&[MessageQueue], &Message) -> Option<MessageQueue> + Send,
    {
        self.send_with_selector_mode(message, selector, SendMode::Future, timeout, false)
            .await?
            .ok_or(ProducerError::Timeout)
    }

    pub async fn send_oneway_with_selector<F>(
        &self,
        message: &Message,
        selector: F,
    ) -> Result<(), ProducerError>
    where
        F: FnOnce(&[MessageQueue], &Message) -> Option<MessageQueue> + Send,
    {
        self.send_with_selector_mode(
            message,
            selector,
            SendMode::Oneway,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
            false,
        )
        .await
        .map(|_| ())
    }

    async fn send_with_selector_mode<F>(
        &self,
        message: &Message,
        selector: F,
        mode: SendMode,
        timeout: Duration,
        auto_batch_enabled: bool,
    ) -> Result<Option<SendResult>, ProducerError>
    where
        F: FnOnce(&[MessageQueue], &Message) -> Option<MessageQueue> + Send,
    {
        let deadline = deadline_after(timeout);
        let use_auto_batch =
            auto_batch_enabled && mode != SendMode::Oneway && self.config.auto_batch();
        let _backpressure = if mode == SendMode::Future && !use_auto_batch {
            self.acquire_async_backpressure(message.body().len(), deadline)
                .await?
        } else {
            None
        };
        let runtime = self.started_runtime().await?;
        validation::validate_message(Some(message), &self.config)?;
        let info = self
            .acquire_publish_info(&runtime, message.topic(), deadline)
            .await?;
        let queue =
            selector(&info.publish_queues, message).ok_or(ProducerError::NoQueueSelected)?;
        ensure_queue_member(&queue, &info)?;
        if use_auto_batch {
            return self
                .send_auto_or_direct(message, Some(queue), mode)
                .await
                .map(Some);
        }
        let prepared = self.prepare(message, false)?;
        self.send_one_attempt(&runtime, &info, &queue, &prepared, mode, deadline)
            .await
    }

    async fn send_auto_or_direct(
        &self,
        message: &Message,
        queue: Option<MessageQueue>,
        mode: SendMode,
    ) -> Result<SendResult, ProducerError> {
        let timeout = Duration::from_millis(self.config.send_timeout_millis().max(0) as u64);
        if mode == SendMode::Oneway || !self.config.auto_batch() || !can_accumulate(message) {
            return self
                .send_message_direct_with_mode(message, queue.as_ref(), mode, timeout)
                .await;
        }

        self.started_runtime().await?;
        validation::validate_message(Some(message), &self.config)?;
        let settings = self.config.accumulator_settings()?;
        let mut accumulated_message = message.clone();
        self.ids.set_if_missing(&mut accumulated_message);
        let queued = self
            .auto_batch_accumulator
            .enqueue(
                accumulated_message,
                queue.clone(),
                mode,
                Duration::from_millis(settings.max_delay_millis as u64),
                settings.max_bytes,
                settings.total_max_bytes,
            )
            .await?;

        match queued {
            EnqueueResult::Bypass => {
                self.send_message_direct_with_mode(message, queue.as_ref(), mode, timeout)
                    .await
            }
            EnqueueResult::Queued {
                receiver,
                key,
                generation,
                flush_at,
                schedule_timer,
            } => {
                if schedule_timer {
                    self.schedule_auto_batch_timer(key, generation, flush_at)
                        .await?;
                }
                receive_auto_batch_result(receiver).await
            }
            EnqueueResult::Ready { receiver, batch } => {
                self.schedule_auto_batch_flush(batch).await?;
                receive_auto_batch_result(receiver).await
            }
        }
    }

    async fn send_message_direct_with_mode(
        &self,
        message: &Message,
        queue: Option<&MessageQueue>,
        mode: SendMode,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        let result = if let Some(queue) = queue {
            self.send_to_queue_mode(message, queue, false, mode, timeout)
                .await?
        } else {
            self.send_default(message, false, mode, timeout).await?
        };
        result.ok_or(ProducerError::Timeout)
    }

    async fn schedule_auto_batch_timer(
        &self,
        key: BatchKey,
        generation: u64,
        flush_at: Instant,
    ) -> Result<(), ProducerError> {
        let producer = self
            .self_weak
            .get()
            .cloned()
            .ok_or(ProducerError::NotStarted)?;
        self.track_auto_batch_task(async move {
            tokio::time::sleep_until(flush_at).await;
            if let Some(producer) = producer.upgrade() {
                producer.flush_auto_batch_generation(&key, generation).await;
            }
        })
        .await;
        Ok(())
    }

    async fn schedule_auto_batch_flush(&self, batch: ReadyBatch) -> Result<(), ProducerError> {
        let producer = self
            .self_weak
            .get()
            .cloned()
            .ok_or(ProducerError::NotStarted)?;
        self.track_auto_batch_task(async move {
            if let Some(producer) = producer.upgrade() {
                producer.flush_auto_batch(batch).await;
            }
        })
        .await;
        Ok(())
    }

    async fn track_auto_batch_task(&self, task: impl Future<Output = ()> + Send + 'static) {
        let mut tasks = self.auto_batch_tasks.lock().await;
        if self.auto_batch_accumulator.is_closed() {
            return;
        }
        let handle = tokio::spawn(task);
        tasks.retain(|task| !task.is_finished());
        tasks.push(handle);
    }

    async fn flush_auto_batch_generation(&self, key: &BatchKey, generation: u64) {
        if let Some(batch) = self.auto_batch_accumulator.take_due(key, generation).await {
            self.flush_auto_batch(batch).await;
        }
    }

    async fn flush_auto_batch(&self, accumulated: ReadyBatch) {
        let count = accumulated.messages.len();
        let result = self
            .send_accumulated_batch(&accumulated)
            .await
            .and_then(|result| split_auto_batch_result(result, count));
        self.auto_batch_accumulator
            .release(accumulated.body_bytes)
            .await;

        match result {
            Ok(results) if results.len() == accumulated.waiters.len() => {
                for (waiter, result) in accumulated.waiters.into_iter().zip(results) {
                    let _ = waiter.send(Ok(result));
                }
            }
            Ok(_) => {
                for waiter in accumulated.waiters {
                    let _ = waiter.send(Err(ProducerError::InvalidAutoBatchResult.to_string()));
                }
            }
            Err(error) => {
                let message = error.to_string();
                for waiter in accumulated.waiters {
                    let _ = waiter.send(Err(message.clone()));
                }
            }
        }
    }

    async fn send_accumulated_batch(
        &self,
        accumulated: &ReadyBatch,
    ) -> Result<SendResult, ProducerError> {
        let mut batch = create_batch(&accumulated.messages, &self.config, &self.ids)?;
        let keys = accumulated
            .messages
            .iter()
            .filter_map(Message::keys)
            .flat_map(|keys| keys.split(KEY_SEPARATOR))
            .filter(|key| !key.is_empty())
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        if !keys.is_empty() {
            batch
                .set_keys(keys.into_iter().collect::<Vec<_>>().join(KEY_SEPARATOR))
                .map_err(|_| ProducerError::InvalidAutoBatchResult)?;
        }
        if let Some(tag) = &accumulated.key.tag {
            batch
                .set_tags(tag.clone())
                .map_err(|_| ProducerError::InvalidAutoBatchResult)?;
        }
        batch
            .set_wait_store_msg_ok(accumulated.key.wait_store_msg_ok)
            .map_err(|_| ProducerError::InvalidAutoBatchResult)?;

        let timeout = Duration::from_millis(self.config.send_timeout_millis().max(0) as u64);
        let result = if let Some(queue) = &accumulated.key.queue {
            self.send_to_queue_mode(&batch, queue, true, accumulated.key.mode, timeout)
                .await?
        } else {
            self.send_default(&batch, true, accumulated.key.mode, timeout)
                .await?
        };
        result.ok_or(ProducerError::Timeout)
    }

    /// Validates, Java-encodes, and sends a batch as one Broker message.
    pub async fn send_batch(&self, messages: &[Message]) -> Result<SendResult, ProducerError> {
        self.send_batch_with_timeout(
            messages,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
        )
        .await
    }

    pub async fn send_batch_with_timeout(
        &self,
        messages: &[Message],
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        let deadline = deadline_after(timeout);
        let batch = create_batch(messages, &self.config, &self.ids)?;
        self.send_default_until(&batch, true, SendMode::Sync, deadline)
            .await?
            .ok_or(ProducerError::Timeout)
    }

    pub async fn send_batch_future(
        &self,
        messages: &[Message],
    ) -> Result<SendResult, ProducerError> {
        self.send_batch_future_with_timeout(
            messages,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
        )
        .await
    }

    pub async fn send_batch_future_with_timeout(
        &self,
        messages: &[Message],
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        let deadline = deadline_after(timeout);
        let batch = create_batch(messages, &self.config, &self.ids)?;
        self.send_default_until(&batch, true, SendMode::Future, deadline)
            .await?
            .ok_or(ProducerError::Timeout)
    }

    pub async fn send_batch_to_queue(
        &self,
        messages: &[Message],
        queue: &MessageQueue,
    ) -> Result<SendResult, ProducerError> {
        self.send_batch_to_queue_with_timeout(
            messages,
            queue,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
        )
        .await
    }

    pub async fn send_batch_to_queue_with_timeout(
        &self,
        messages: &[Message],
        queue: &MessageQueue,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        self.send_batch_to_queue_mode(messages, queue, SendMode::Sync, timeout)
            .await?
            .ok_or(ProducerError::Timeout)
    }

    pub async fn send_batch_to_queue_future(
        &self,
        messages: &[Message],
        queue: &MessageQueue,
    ) -> Result<SendResult, ProducerError> {
        self.send_batch_to_queue_future_with_timeout(
            messages,
            queue,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
        )
        .await
    }

    pub async fn send_batch_to_queue_future_with_timeout(
        &self,
        messages: &[Message],
        queue: &MessageQueue,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        self.send_batch_to_queue_mode(messages, queue, SendMode::Future, timeout)
            .await?
            .ok_or(ProducerError::Timeout)
    }

    async fn send_batch_to_queue_mode(
        &self,
        messages: &[Message],
        queue: &MessageQueue,
        mode: SendMode,
        timeout: Duration,
    ) -> Result<Option<SendResult>, ProducerError> {
        let deadline = deadline_after(timeout);
        let batch = create_batch(messages, &self.config, &self.ids)?;
        self.send_to_queue_until(&batch, queue, true, mode, deadline)
            .await
    }

    async fn send_to_queue_mode(
        &self,
        message: &Message,
        queue: &MessageQueue,
        batch: bool,
        mode: SendMode,
        timeout: Duration,
    ) -> Result<Option<SendResult>, ProducerError> {
        let deadline = deadline_after(timeout);
        self.send_to_queue_until(message, queue, batch, mode, deadline)
            .await
    }

    async fn send_to_queue_until(
        &self,
        message: &Message,
        queue: &MessageQueue,
        batch: bool,
        mode: SendMode,
        deadline: Instant,
    ) -> Result<Option<SendResult>, ProducerError> {
        let _backpressure = if mode == SendMode::Future {
            self.acquire_async_backpressure(message.body().len(), deadline)
                .await?
        } else {
            None
        };
        let runtime = self.started_runtime().await?;
        validation::validate_message(Some(message), &self.config)?;
        let info = self
            .acquire_publish_info(&runtime, message.topic(), deadline)
            .await?;
        ensure_queue_member(queue, &info)?;
        let prepared = self.prepare(message, batch)?;
        self.send_one_attempt(&runtime, &info, queue, &prepared, mode, deadline)
            .await
    }

    async fn send_default(
        &self,
        message: &Message,
        batch: bool,
        mode: SendMode,
        timeout: Duration,
    ) -> Result<Option<SendResult>, ProducerError> {
        let deadline = deadline_after(timeout);
        self.send_default_until(message, batch, mode, deadline)
            .await
    }

    async fn send_default_until(
        &self,
        message: &Message,
        batch: bool,
        mode: SendMode,
        deadline: Instant,
    ) -> Result<Option<SendResult>, ProducerError> {
        let _backpressure = if mode == SendMode::Future {
            self.acquire_async_backpressure(message.body().len(), deadline)
                .await?
        } else {
            None
        };
        let runtime = self.started_runtime().await?;
        validation::validate_message(Some(message), &self.config)?;
        let mut info = self
            .acquire_publish_info(&runtime, message.topic(), deadline)
            .await?;
        let prepared = self.prepare(message, batch)?;
        let attempts = attempt_count(mode, &self.config);
        if attempts == 0 {
            return Err(ProducerError::NoSendAttempt);
        }

        let mut last_broker = None;
        let mut last_error = None;
        let mut last_send_result = None;
        for attempt in 0..attempts {
            if Instant::now() >= deadline {
                return last_send_result
                    .map(Some)
                    .ok_or_else(|| last_error.unwrap_or(ProducerError::Timeout));
            }
            let latency_fault_enabled = self.config.client_config().send_latency_enable();
            if attempt > 0 && latency_fault_enabled {
                self.selector.reset(message.topic());
            }
            let selected = self.fault.select(
                &info.publish_queues,
                last_broker.as_deref(),
                latency_fault_enabled,
                |length| self.selector.next_index(message.topic(), length),
                StdInstant::now(),
            );
            let Some(queue) = selected else {
                return last_send_result
                    .map(Some)
                    .ok_or_else(|| ProducerError::NoRoute(message.topic().to_owned()));
            };
            let Some(broker) = queue.broker_name().map(str::to_owned) else {
                return last_send_result
                    .map(Some)
                    .ok_or_else(|| ProducerError::NoRoute(message.topic().to_owned()));
            };
            last_broker = Some(broker.clone());

            let Some(address) = self.broker_address(&runtime, &info, &queue)? else {
                // Java refreshes publish info once from sendKernelImpl when the
                // broker address is absent, then lets the retry loop decide.
                let _ = runtime
                    .update_topic_route(message.topic(), deadline)
                    .await?;
                info = self
                    .current_publish_info(&runtime, message.topic())?
                    .unwrap_or(info);
                last_error = Some(ProducerError::NoRoute(message.topic().to_owned()));
                continue;
            };

            let request_deadline = self.request_deadline(mode, deadline, attempt, attempts);
            let started = StdInstant::now();
            let result = self
                .send_one_attempt_at(SendAttempt {
                    runtime: &runtime,
                    queue: &queue,
                    broker: &broker,
                    address: &address,
                    prepared: &prepared,
                    mode,
                    deadline: request_deadline,
                })
                .await;
            let latency = started.elapsed();
            match result {
                Ok(Some(send_result)) => {
                    self.update_fault(&broker, latency, false, true);
                    if mode == SendMode::Sync
                        && send_result.status != SendStatus::SendOk
                        && self.config.retry_another_broker_when_not_store_ok()
                        && attempt + 1 < attempts
                    {
                        last_send_result = Some(send_result);
                        continue;
                    }
                    return Ok(Some(send_result));
                }
                Ok(None) => {
                    self.update_fault(&broker, latency, false, true);
                    return Ok(None);
                }
                Err(error) => {
                    let (isolation, reachable, retry) = match &error {
                        ProducerError::Rpc(rpc) => {
                            let (isolation, reachable) = classify_fault(
                                rpc,
                                mode,
                                self.config.client_config().start_detector_enable(),
                            );
                            (
                                isolation,
                                reachable,
                                attempt + 1 < attempts && should_retry(mode, rpc, &self.config),
                            )
                        }
                        _ => (false, true, false),
                    };
                    self.update_fault(&broker, latency, isolation, reachable);
                    if retry {
                        last_error = Some(error);
                        continue;
                    }
                    if let Some(send_result) = last_send_result.take() {
                        return Ok(Some(send_result));
                    }
                    return Err(error);
                }
            }
        }
        if let Some(send_result) = last_send_result {
            return Ok(Some(send_result));
        }
        Err(last_error.unwrap_or_else(|| ProducerError::NoRoute(message.topic().to_owned())))
    }

    async fn send_one_attempt(
        &self,
        runtime: &ClientRuntime,
        info: &TopicSnapshot,
        queue: &MessageQueue,
        prepared: &PreparedMessage,
        mode: SendMode,
        deadline: Instant,
    ) -> Result<Option<SendResult>, ProducerError> {
        let broker = queue
            .broker_name()
            .ok_or_else(|| ProducerError::NoRoute(prepared.topic.clone()))?;
        let address = self
            .broker_address(runtime, info, queue)?
            .ok_or_else(|| ProducerError::NoRoute(prepared.topic.clone()))?;
        self.send_one_attempt_at(SendAttempt {
            runtime,
            queue,
            broker,
            address: &address,
            prepared,
            mode,
            deadline,
        })
        .await
    }

    async fn send_one_attempt_at(
        &self,
        attempt: SendAttempt<'_>,
    ) -> Result<Option<SendResult>, ProducerError> {
        if Instant::now() >= attempt.deadline {
            return Err(ProducerError::Timeout);
        }
        #[cfg(test)]
        {
            let mut scripted = self
                .scripted_transport
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            scripted.attempts.push(ScriptedSendAttempt {
                broker: attempt.broker.to_owned(),
                address: attempt.address.to_owned(),
                queue: attempt.queue.clone(),
                body: attempt.prepared.body.clone(),
                properties: attempt.prepared.properties.clone(),
                sys_flag: attempt.prepared.sys_flag,
                unique_id: attempt.prepared.unique_id.clone(),
                mode: attempt.mode,
                batch: attempt.prepared.batch,
            });
            if let Some(result) = scripted.responses.pop_front() {
                return result;
            }
        }
        let header = SendMessageRequestHeaderV1 {
            producer_group: self.config.producer_group().to_owned(),
            topic: attempt.prepared.topic.clone(),
            default_topic: self.config.create_topic_key().to_owned(),
            default_topic_queue_nums: self.config.default_topic_queue_nums(),
            queue_id: attempt.queue.queue_id(),
            sys_flag: attempt.prepared.sys_flag,
            born_timestamp: attempt.prepared.born_timestamp,
            flag: attempt.prepared.flag,
            properties: attempt.prepared.properties.clone(),
            reconsume_times: attempt.prepared.reconsume_times,
            unit_mode: Some(self.config.client_config().unit_mode()),
            max_reconsume_times: attempt.prepared.max_reconsume_times,
            batch: Some(attempt.prepared.batch),
        };
        let kind = if attempt.prepared.batch {
            SendKind::Batch
        } else {
            // Java 5.5.1's `sendSmartMsg` defaults to true.
            SendKind::V2
        };
        Ok(attempt
            .runtime
            .api()
            .send_message(
                attempt.address,
                attempt.broker,
                &header,
                attempt.prepared.body.clone(),
                &attempt.prepared.unique_id,
                kind,
                attempt.mode,
                attempt.deadline,
            )
            .await?)
    }

    async fn acquire_publish_info(
        &self,
        runtime: &ClientRuntime,
        topic: &str,
        deadline: Instant,
    ) -> Result<Arc<TopicSnapshot>, ProducerError> {
        if let Some(info) = self.current_publish_info(runtime, topic)? {
            if !info.publish_queues.is_empty() {
                return Ok(info);
            }
        }

        let _ = runtime.update_topic_route(topic, deadline).await?;
        if let Some(info) = self.current_publish_info(runtime, topic)? {
            if !info.publish_queues.is_empty() {
                return Ok(info);
            }
        }

        let _ = runtime
            .update_topic_route_from_default(
                topic,
                self.config.default_topic_queue_nums(),
                deadline,
            )
            .await?;
        if let Some(info) = self.current_publish_info(runtime, topic)? {
            if !info.publish_queues.is_empty() {
                return Ok(info);
            }
        }
        Err(ProducerError::NoRoute(topic.to_owned()))
    }

    fn current_publish_info(
        &self,
        runtime: &ClientRuntime,
        topic: &str,
    ) -> Result<Option<Arc<TopicSnapshot>>, ProducerError> {
        if let Some(info) = self.cached_publish_info(topic) {
            return Ok(Some(info));
        }
        let info = runtime.route_snapshot()?.topics.get(topic).cloned();
        if let Some(info) = &info {
            self.cache_publish_info(topic, info.clone());
        }
        Ok(info)
    }

    fn broker_address(
        &self,
        runtime: &ClientRuntime,
        info: &TopicSnapshot,
        queue: &MessageQueue,
    ) -> Result<Option<String>, ProducerError> {
        if let Some(address) = info.endpoints.get(queue) {
            return Ok(Some(address.clone()));
        }
        let Some(broker) = queue.broker_name() else {
            return Ok(None);
        };
        let snapshot = runtime.route_snapshot()?;
        let Some(address) = snapshot.publish_address(broker) else {
            return Ok(None);
        };
        Ok(Some(broker_vip_channel(
            self.config.client_config().vip_channel_enabled(),
            address,
        )?))
    }

    fn request_deadline(
        &self,
        mode: SendMode,
        deadline: Instant,
        attempt: usize,
        attempts: usize,
    ) -> Instant {
        self.request_deadline_at(mode, deadline, Instant::now(), attempt, attempts)
    }

    fn request_deadline_at(
        &self,
        mode: SendMode,
        deadline: Instant,
        now: Instant,
        attempt: usize,
        attempts: usize,
    ) -> Instant {
        let max_per_request = self.config.send_msg_max_timeout_per_request();
        if mode != SendMode::Sync || max_per_request <= -1 || attempt + 1 >= attempts {
            return deadline;
        }
        let per_request = now + Duration::from_millis(max_per_request.max(0) as u64);
        per_request.min(deadline)
    }

    fn prepare(&self, message: &Message, batch: bool) -> Result<PreparedMessage, ProducerError> {
        let mut copy = message.clone();
        let unique_id = self.ids.set_if_missing(&mut copy);
        let mut properties: BTreeMap<String, String> = copy.properties().clone();
        let retry_group = copy.topic().starts_with("%RETRY%");
        let reconsume_times = if retry_group {
            properties
                .remove(PROPERTY_RECONSUME_TIME)
                .map(|value| {
                    value
                        .parse()
                        .map_err(|_| ProducerError::InvalidRetryMetadata)
                })
                .transpose()?
        } else {
            None
        };
        let max_reconsume_times = if retry_group {
            properties
                .remove(PROPERTY_MAX_RECONSUME_TIMES)
                .map(|value| {
                    value
                        .parse()
                        .map_err(|_| ProducerError::InvalidRetryMetadata)
                })
                .transpose()?
        } else {
            None
        };

        let mut body = copy.body().to_vec();
        let mut sys_flag = 0;
        if !batch && body.len() as i64 >= self.config.compress_threshold() as i64 {
            if let Ok(compressed) = rocketmq_classic_common::compression::compress(
                self.config.compress_type(),
                &body,
                self.config.compress_level(),
            ) {
                body = compressed;
                sys_flag |=
                    message::COMPRESSED_FLAG | self.config.compress_type().get_compression_flag();
            }
        }
        if copy
            .properties()
            .get(PROPERTY_TRANSACTION_PREPARED)
            .is_some_and(|value| value.eq_ignore_ascii_case("true"))
        {
            sys_flag |= message::TRANSACTION_PREPARED_TYPE;
        }
        Ok(PreparedMessage {
            topic: copy.topic().to_owned(),
            body,
            properties: Some(encode_properties(&properties)),
            unique_id,
            born_timestamp: epoch_millis_now(),
            flag: copy.flag(),
            sys_flag,
            reconsume_times,
            max_reconsume_times,
            batch,
        })
    }

    fn update_fault(&self, broker: &str, latency: Duration, isolation: bool, reachable: bool) {
        if self.config.client_config().send_latency_enable() {
            self.fault
                .update(broker, latency, isolation, reachable, StdInstant::now());
        }
    }
}

fn ensure_queue_member(queue: &MessageQueue, info: &TopicSnapshot) -> Result<(), ProducerError> {
    if info.publish_queues.contains(queue) {
        Ok(())
    } else {
        Err(ProducerError::QueueNotInRoute)
    }
}

fn can_accumulate(message: &Message) -> bool {
    if message.topic().starts_with("%RETRY%")
        || message.properties().contains_key(PROPERTY_PRODUCER_GROUP)
    {
        return false;
    }
    [
        PROPERTY_DELAY_TIME_LEVEL,
        PROPERTY_TIMER_DELAY_MS,
        PROPERTY_TIMER_DELAY_SEC,
        PROPERTY_TIMER_DELIVER_MS,
    ]
    .into_iter()
    .all(|property| {
        message
            .properties()
            .get(property)
            .is_none_or(|value| value.parse::<i64>().is_ok_and(|delay| delay <= 0))
    })
}

async fn receive_auto_batch_result(
    receiver: crate::accumulator::BatchReceiver,
) -> Result<SendResult, ProducerError> {
    match receiver.await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(error)) => Err(ProducerError::AutoBatchSend(error)),
        Err(_) => Err(ProducerError::Shutdown),
    }
}

fn split_auto_batch_result(
    result: SendResult,
    message_count: usize,
) -> Result<Vec<SendResult>, ProducerError> {
    if !result.msg_id.contains(',') {
        return Ok(vec![result; message_count]);
    }
    let message_ids = result.msg_id.split(',').collect::<Vec<_>>();
    let offset_message_ids = result.offset_msg_id.split(',').collect::<Vec<_>>();
    if message_ids.len() != message_count || offset_message_ids.len() != message_count {
        return Err(ProducerError::InvalidAutoBatchResult);
    }
    Ok((0..message_count)
        .map(|index| {
            let mut per_message = result.clone();
            per_message.msg_id = message_ids[index].to_owned();
            per_message.offset_msg_id = offset_message_ids[index].to_owned();
            per_message.queue_offset = result.queue_offset.wrapping_add(index as i64);
            per_message
        })
        .collect())
}

fn deadline_after(timeout: Duration) -> Instant {
    Instant::now() + timeout
}

fn epoch_millis_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

fn attempt_count(mode: SendMode, config: &crate::ProducerConfig) -> usize {
    match mode {
        SendMode::Sync => {
            usize::try_from(1_i64 + i64::from(config.retry_times_when_send_failed())).unwrap_or(0)
        }
        SendMode::Future => 1 + config.retry_times_when_send_async_failed().max(0) as usize,
        SendMode::Oneway => 1,
    }
}

fn should_retry(mode: SendMode, error: &RpcError, config: &crate::ProducerConfig) -> bool {
    match mode {
        SendMode::Sync => match error {
            RpcError::Response { code, .. } => config.retry_response_codes().contains(code),
            _ => true,
        },
        SendMode::Future => {
            matches!(error, RpcError::Transport(error) if *error != rocketmq_classic_remoting::TransportError::TooManyRequests)
        }
        SendMode::Oneway => false,
    }
}

fn classify_fault(error: &RpcError, mode: SendMode, detector_enabled: bool) -> (bool, bool) {
    match mode {
        SendMode::Future => match error {
            RpcError::Response { .. }
            | RpcError::MalformedResponse { .. }
            | RpcError::Header(_)
            | RpcError::Transport(_) => (true, true),
            _ => (false, true),
        },
        SendMode::Sync | SendMode::Oneway => match error {
            RpcError::Response { .. } => (true, false),
            RpcError::Transport(_) => (true, !detector_enabled),
            _ => (false, true),
        },
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_classic_common::message::decode_properties;
    use rocketmq_classic_core::api::producer::SendStatus;
    use rocketmq_classic_core::error::RpcError;
    use rocketmq_classic_protocol::codes::response;
    use rocketmq_classic_remoting::TransportError;

    use super::*;

    fn scripted_result(msg_id: &str, offset_msg_id: &str, queue_offset: i64) -> SendResult {
        SendResult {
            status: SendStatus::SendOk,
            msg_id: msg_id.into(),
            offset_msg_id: offset_msg_id.into(),
            topic: "orders".into(),
            broker_name: "scripted-broker".into(),
            queue_id: 0,
            queue_offset,
            transaction_id: None,
            recall_handle: None,
            raw_resp_body: None,
            region_id: "DefaultRegion".into(),
            trace_on: true,
        }
    }

    async fn scripted_producer(
        config: crate::ProducerConfig,
    ) -> (Arc<Producer>, [MessageQueue; 2]) {
        let manager = rocketmq_classic_core::runtime::ClientRuntimeManager::new();
        let producer = Arc::new(Producer::with_runtime_manager(config, manager));
        producer.start().await.unwrap();
        let queues = [
            MessageQueue::new("orders", "broker-a", 0),
            MessageQueue::new("orders", "broker-b", 0),
        ];
        let snapshot = Arc::new(TopicSnapshot {
            route: Arc::new(
                serde_json::from_str::<rocketmq_classic_protocol::route::TopicRouteData>("{}")
                    .unwrap(),
            ),
            publish_queues: queues.to_vec(),
            subscribe_queues: Default::default(),
            endpoints: BTreeMap::from([
                (queues[0].clone(), "10.0.0.1:10911".into()),
                (queues[1].clone(), "10.0.0.2:10911".into()),
            ]),
            order_topic: false,
        });
        producer.cache_publish_info("orders", snapshot);
        (producer, queues)
    }

    #[tokio::test]
    async fn scripted_two_broker_retry_preserves_body_and_unique_id() {
        let mut config = crate::ProducerConfig::new("scripted-retry").unwrap();
        config.set_retry_times_when_send_failed(1);
        let (producer, _) = scripted_producer(config).await;
        {
            let mut scripted = producer.scripted_transport.lock().unwrap();
            scripted.responses.extend([
                Err(ProducerError::Rpc(RpcError::Response {
                    request_code: 10,
                    peer: "broker".into(),
                    code: response::SERVICE_NOT_AVAILABLE,
                    remark: Some("retry this request".into()),
                })),
                Ok(Some(scripted_result("msg-1", "offset-1", 7))),
            ]);
        }

        let message = Message::new("orders", b"same immutable body".to_vec());
        let result = producer
            .send_with_timeout(&message, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(result.msg_id, "msg-1");
        assert_eq!(message.body(), b"same immutable body");
        assert!(
            !message
                .properties()
                .contains_key(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
        );

        let attempts = {
            let mut scripted = producer.scripted_transport.lock().unwrap();
            std::mem::take(&mut scripted.attempts)
        };
        assert_eq!(attempts.len(), 2);
        assert_ne!(attempts[0].broker, attempts[1].broker);
        assert_ne!(attempts[0].address, attempts[1].address);
        assert_eq!(attempts[0].body, attempts[1].body);
        assert_eq!(attempts[0].unique_id, attempts[1].unique_id);
        assert_eq!(attempts[0].mode, SendMode::Sync);
        assert_eq!(attempts[1].mode, SendMode::Sync);
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn scripted_async_and_oneway_failures_keep_their_java_retry_policies() {
        let config = crate::ProducerConfig::new("scripted-mode-policy").unwrap();
        let (producer, queues) = scripted_producer(config).await;
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Err(ProducerError::Rpc(RpcError::Response {
                request_code: 10,
                peer: "broker".into(),
                code: response::SERVICE_NOT_AVAILABLE,
                remark: None,
            })));
        assert!(matches!(
            producer
                .send_future_with_timeout(
                    &Message::new("orders", b"async".to_vec()),
                    Duration::from_secs(1)
                )
                .await,
            Err(ProducerError::Rpc(RpcError::Response { .. }))
        ));
        assert_eq!(
            producer.scripted_transport.lock().unwrap().attempts.len(),
            1
        );

        {
            let mut scripted = producer.scripted_transport.lock().unwrap();
            scripted.responses.clear();
            scripted.responses.push_back(Ok(None));
        }
        producer
            .send_to_queue_oneway(&Message::new("orders", b"oneway".to_vec()), &queues[0])
            .await
            .unwrap();
        {
            let scripted = producer.scripted_transport.lock().unwrap();
            assert_eq!(scripted.attempts.len(), 2);
            assert_eq!(scripted.attempts[1].mode, SendMode::Oneway);
            assert_eq!(scripted.attempts[1].queue, queues[0]);
        }

        {
            let mut scripted = producer.scripted_transport.lock().unwrap();
            scripted.responses.clear();
            scripted.responses.push_back(Ok(Some(scripted_result(
                "selected-msg",
                "selected-offset",
                9,
            ))));
        }
        producer
            .send_with_selector_with_timeout(
                &Message::new("orders", b"selected".to_vec()),
                |publish_queues, _| Some(publish_queues[1].clone()),
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(
            producer.scripted_transport.lock().unwrap().attempts[2].queue,
            queues[1]
        );

        {
            let mut scripted = producer.scripted_transport.lock().unwrap();
            scripted.responses.clear();
            scripted.responses.push_back(Ok(Some(scripted_result(
                "queue-msg",
                "queue-offset",
                10,
            ))));
        }
        producer
            .send_to_queue_with_timeout(
                &Message::new("orders", b"queue".to_vec()),
                &queues[0],
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        {
            let scripted = producer.scripted_transport.lock().unwrap();
            assert_eq!(scripted.attempts.len(), 4);
            assert_eq!(scripted.attempts[3].queue, queues[0]);
        }
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn auto_batch_flushes_one_message_batch_and_fans_out_results() {
        let mut config = crate::ProducerConfig::new("scripted-auto-batch").unwrap();
        config.set_auto_batch(true);
        config.set_batch_max_delay_millis(15);
        let (producer, _) = scripted_producer(config).await;
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result(
                "msg-a,msg-b",
                "offset-a,offset-b",
                20,
            ))));

        let mut first = Message::new("orders", b"first".to_vec());
        first.set_tags("blue").unwrap();
        first.set_keys("key-a").unwrap();
        let mut second = Message::new("orders", b"second".to_vec());
        second.set_tags("blue").unwrap();
        second.set_keys("key-b").unwrap();
        let (first_result, second_result) =
            tokio::join!(producer.send(&first), producer.send(&second));
        let first_result = first_result.unwrap();
        let second_result = second_result.unwrap();
        assert_eq!(first_result.msg_id, "msg-a");
        assert_eq!(second_result.msg_id, "msg-b");
        assert_eq!(first_result.queue_offset, 20);
        assert_eq!(second_result.queue_offset, 21);

        let attempts = {
            let mut scripted = producer.scripted_transport.lock().unwrap();
            std::mem::take(&mut scripted.attempts)
        };
        assert_eq!(attempts.len(), 1);
        assert!(attempts[0].batch);
        assert_eq!(attempts[0].mode, SendMode::Sync);
        let properties = decode_properties(attempts[0].properties.as_deref().unwrap());
        assert_eq!(
            properties.get(PROPERTY_TAGS).map(String::as_str),
            Some("blue")
        );
        assert_eq!(
            properties.get(PROPERTY_KEYS).map(String::as_str),
            Some("key-a key-b")
        );
        assert_eq!(first.body(), b"first");
        assert_eq!(second.body(), b"second");
        producer.shutdown().await.unwrap();
    }

    #[test]
    fn auto_batch_eligibility_matches_java_direct_send_fallbacks() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        for row in fixture["autoBatchEligibility"].as_array().unwrap() {
            let message = match row["input"].as_str().unwrap() {
                "regular" => Message::new("orders", b"body".to_vec()),
                "delayed" => {
                    let mut message = Message::new("orders", b"body".to_vec());
                    message
                        .set_system_property(PROPERTY_TIMER_DELAY_MS, "100")
                        .unwrap();
                    message
                }
                "retry-topic" => Message::new("%RETRY%workers", b"body".to_vec()),
                "producer-group" => {
                    let mut message = Message::new("orders", b"body".to_vec());
                    message
                        .set_system_property(PROPERTY_PRODUCER_GROUP, "transaction-producer")
                        .unwrap();
                    message
                }
                other => panic!("unknown Java auto-batch fixture case: {other}"),
            };
            assert_eq!(
                can_accumulate(&message),
                row["valid"].as_bool().unwrap(),
                "{row}"
            );
        }
    }

    #[test]
    fn auto_batch_result_fanout_preserves_java_ids_and_offsets() {
        let mut result = SendResult {
            status: SendStatus::SendOk,
            msg_id: "msg-a,msg-b".into(),
            offset_msg_id: "offset-a,offset-b".into(),
            topic: "orders".into(),
            broker_name: "broker-a".into(),
            queue_id: 2,
            queue_offset: 40,
            transaction_id: None,
            recall_handle: None,
            raw_resp_body: None,
            region_id: "region-a".into(),
            trace_on: true,
        };
        let split = split_auto_batch_result(result.clone(), 2).unwrap();
        assert_eq!(split[0].msg_id, "msg-a");
        assert_eq!(split[1].msg_id, "msg-b");
        assert_eq!(split[0].offset_msg_id, "offset-a");
        assert_eq!(split[1].offset_msg_id, "offset-b");
        assert_eq!(split[0].queue_offset, 40);
        assert_eq!(split[1].queue_offset, 41);

        result.msg_id = "batch-msg-id".into();
        result.offset_msg_id = "batch-offset-id".into();
        let repeated = split_auto_batch_result(result, 2).unwrap();
        assert_eq!(repeated[0], repeated[1]);

        let mut inconsistent = split[0].clone();
        inconsistent.msg_id = "msg-a,msg-b".into();
        inconsistent.offset_msg_id = "offset-a".into();
        assert!(matches!(
            split_auto_batch_result(inconsistent, 2),
            Err(ProducerError::InvalidAutoBatchResult)
        ));
    }

    #[test]
    fn mode_specific_attempts_match_java_retry_budgets() {
        let config = crate::ProducerConfig::new("producer").unwrap();
        assert_eq!(attempt_count(SendMode::Sync, &config), 3);
        assert_eq!(attempt_count(SendMode::Future, &config), 3);
        assert_eq!(attempt_count(SendMode::Oneway, &config), 1);

        let mut no_attempts = config.clone();
        no_attempts.set_retry_times_when_send_failed(-1);
        assert_eq!(attempt_count(SendMode::Sync, &no_attempts), 0);
    }

    #[test]
    fn sync_retries_configured_broker_codes_while_future_retries_transport_only() {
        let config = crate::ProducerConfig::new("producer").unwrap();
        let retryable = RpcError::Response {
            request_code: 10,
            peer: "broker".into(),
            code: response::SERVICE_NOT_AVAILABLE,
            remark: None,
        };
        assert!(should_retry(SendMode::Sync, &retryable, &config));
        assert!(!should_retry(SendMode::Future, &retryable, &config));
        let transport = RpcError::Transport(TransportError::InvalidConfiguration("test".into()));
        assert!(should_retry(SendMode::Sync, &transport, &config));
        assert!(should_retry(SendMode::Future, &transport, &config));
        assert!(!should_retry(
            SendMode::Future,
            &RpcError::Transport(TransportError::TooManyRequests),
            &config
        ));
        assert!(!should_retry(SendMode::Oneway, &transport, &config));
    }

    #[test]
    fn default_sync_retry_response_codes_match_the_java_fixture() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let config = crate::ProducerConfig::new("producer").unwrap();
        for code in fixture["defaults"]["retryResponseCodes"]
            .as_array()
            .unwrap()
        {
            let code = code.as_i64().unwrap() as i32;
            let error = RpcError::Response {
                request_code: 10,
                peer: "broker".into(),
                code,
                remark: None,
            };
            assert!(should_retry(SendMode::Sync, &error, &config), "code {code}");
        }
        let non_retryable = RpcError::Response {
            request_code: 10,
            peer: "broker".into(),
            code: i32::MAX,
            remark: None,
        };
        assert!(!should_retry(SendMode::Sync, &non_retryable, &config));
    }

    #[test]
    fn fault_classification_matches_detector_reachability_switch() {
        let transport = RpcError::Transport(TransportError::InvalidConfiguration("test".into()));
        assert_eq!(
            classify_fault(&transport, SendMode::Sync, false),
            (true, true)
        );
        assert_eq!(
            classify_fault(&transport, SendMode::Sync, true),
            (true, false)
        );
        let response = RpcError::Response {
            request_code: 10,
            peer: "broker".into(),
            code: response::TOPIC_NOT_EXIST,
            remark: None,
        };
        assert_eq!(
            classify_fault(&response, SendMode::Sync, false),
            (true, false)
        );
        assert_eq!(
            classify_fault(&transport, SendMode::Future, true),
            (true, true)
        );
        assert_eq!(
            classify_fault(&response, SendMode::Future, true),
            (true, true)
        );
    }

    #[test]
    fn per_request_timeout_only_limits_sync_attempts_that_have_a_retry_after_them() {
        let mut config = crate::ProducerConfig::new("producer").unwrap();
        config.set_send_msg_max_timeout_per_request(500);
        let producer = crate::Producer::new(config);
        let now = Instant::now();
        let deadline = now + Duration::from_secs(5);
        assert_eq!(
            producer.request_deadline_at(SendMode::Sync, deadline, now, 0, 3),
            now + Duration::from_millis(500)
        );
        assert_eq!(
            producer.request_deadline_at(SendMode::Sync, deadline, now, 2, 3),
            deadline
        );
        assert_eq!(
            producer.request_deadline_at(SendMode::Future, deadline, now, 0, 3),
            deadline
        );
    }

    #[test]
    fn compression_threshold_matches_java_and_batch_payloads_stay_uncompressed() {
        let config = crate::ProducerConfig::new("compression-test").unwrap();
        let producer = crate::Producer::new(config);
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let compression = &fixture["compression"];
        assert_eq!(
            compression["threshold"].as_i64().unwrap(),
            i64::from(producer.config.compress_threshold())
        );
        for case in compression["cases"].as_array().unwrap() {
            let length = case["bodyLength"].as_u64().unwrap() as usize;
            let expected = case["compressed"].as_bool().unwrap();
            let message = Message::new("orders", vec![b'x'; length]);
            let prepared = producer.prepare(&message, false).unwrap();
            assert_eq!(
                message::has_compressed_flag(prepared.sys_flag),
                expected,
                "Java threshold fixture case {case}"
            );
            if expected {
                assert_eq!(
                    rocketmq_classic_common::compression::decompress(
                        producer.config.compress_type(),
                        &prepared.body
                    )
                    .unwrap(),
                    message.body()
                );
            } else {
                assert_eq!(prepared.body, message.body());
            }
            assert_eq!(message.body().len(), length);

            let batch_prepared = producer.prepare(&message, true).unwrap();
            assert_eq!(batch_prepared.sys_flag, 0);
            assert_eq!(batch_prepared.body, message.body());
        }
    }

    #[tokio::test]
    async fn async_backpressure_uses_java_minimums_and_releases_permits_on_drop() {
        let mut config = crate::ProducerConfig::new("backpressure-producer").unwrap();
        config.set_enable_backpressure_for_async_mode(true);
        config.set_backpressure_async_send_num(1);
        config.set_backpressure_async_send_size(1);
        let producer = Producer::new(config);
        let mut permits = Vec::new();
        for _ in 0..10 {
            permits.push(
                producer
                    .acquire_async_backpressure(1, Instant::now() + Duration::from_secs(1))
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        assert!(matches!(
            producer
                .acquire_async_backpressure(1, Instant::now() + Duration::from_millis(5))
                .await,
            Err(ProducerError::Timeout)
        ));
        drop(permits);
        assert!(
            producer
                .acquire_async_backpressure(1, Instant::now() + Duration::from_secs(1))
                .await
                .unwrap()
                .is_some()
        );
    }
}
