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
use tokio::time::Instant;

use crate::{
    AsyncSenderTask, Producer, ProducerError,
    accumulator::{BatchKey, EnqueueResult, ReadyBatch},
    backpressure::AdjustablePermit,
    batch::create_batch,
    hooks::{CheckForbiddenContext, SendMessageContext, SendMessageType},
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

type AsyncBackpressurePermit = (AdjustablePermit, AdjustablePermit);

impl Producer {
    async fn acquire_async_backpressure(
        &self,
        message_size: usize,
        deadline: Instant,
    ) -> Result<(bool, Option<AsyncBackpressurePermit>), ProducerError> {
        let enabled = self.enable_backpressure_for_async_mode();
        if !enabled {
            return Ok((false, None));
        }
        let count = self.backpressure_send_num.acquire(1, deadline).await?;
        let bytes = self
            .backpressure_send_size
            .acquire(message_size.max(1), deadline)
            .await?;
        Ok((true, Some((count, bytes))))
    }

    /// Refreshes the topic route from NameServer and returns writable queues,
    /// matching Java's `fetchPublishMessageQueues` behavior.
    pub async fn fetch_publish_message_queues(
        &self,
        topic: &str,
    ) -> Result<Vec<MessageQueue>, ProducerError> {
        let runtime = self.started_runtime().await?;
        let topic = self.topic_with_namespace(topic)?;
        self.remember_publish_topic(&topic);
        let timeout = Duration::from_millis(
            self.config.client_config().mq_client_api_timeout().max(0) as u64,
        );
        let deadline = deadline_after(timeout);
        let updated = runtime.update_topic_route(&topic, deadline).await?;
        if !updated {
            return Err(ProducerError::NoRoute(topic.to_owned()));
        }
        let Some(info) = self.current_publish_info(&runtime, &topic)? else {
            return Err(ProducerError::NoRoute(topic.to_owned()));
        };
        if info.publish_queues.is_empty() {
            return Err(ProducerError::NoRoute(topic.to_owned()));
        }
        info.publish_queues
            .iter()
            .map(|queue| self.queue_without_namespace(queue))
            .collect()
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

    /// Sends one message directly, bypassing the producer auto-batch
    /// accumulator, like Java's `DefaultMQProducer.sendDirect` helper.
    pub async fn send_direct(&self, message: &Message) -> Result<SendResult, ProducerError> {
        self.send_direct_with_timeout(
            message,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
        )
        .await
    }

    pub async fn send_direct_with_timeout(
        &self,
        message: &Message,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        self.send_message_direct_with_mode(message, None, SendMode::Sync, timeout)
            .await
    }

    /// Future-mode counterpart to [`Self::send_direct`].
    pub async fn send_direct_future(&self, message: &Message) -> Result<SendResult, ProducerError> {
        self.send_direct_future_with_timeout(
            message,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
        )
        .await
    }

    pub async fn send_direct_future_with_timeout(
        &self,
        message: &Message,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        self.send_message_direct_with_mode(message, None, SendMode::Future, timeout)
            .await
    }

    /// Future-mode send. Like Java's callback send, it retries asynchronous
    /// failures up to the configured budget and resolves with the terminal result.
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

    /// Sends to a caller-selected queue using its Broker address from the
    /// current route. This overload makes one Broker attempt.
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

    /// Sends directly to a selected queue, bypassing auto-batching.
    pub async fn send_to_queue_direct(
        &self,
        message: &Message,
        queue: &MessageQueue,
    ) -> Result<SendResult, ProducerError> {
        self.send_to_queue_direct_with_timeout(
            message,
            queue,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
        )
        .await
    }

    pub async fn send_to_queue_direct_with_timeout(
        &self,
        message: &Message,
        queue: &MessageQueue,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        self.send_message_direct_with_mode(message, Some(queue), SendMode::Sync, timeout)
            .await
    }

    /// Future-mode direct send to a selected queue.
    pub async fn send_to_queue_direct_future(
        &self,
        message: &Message,
        queue: &MessageQueue,
    ) -> Result<SendResult, ProducerError> {
        self.send_to_queue_direct_future_with_timeout(
            message,
            queue,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
        )
        .await
    }

    pub async fn send_to_queue_direct_future_with_timeout(
        &self,
        message: &Message,
        queue: &MessageQueue,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        self.send_message_direct_with_mode(message, Some(queue), SendMode::Future, timeout)
            .await
    }

    /// Sends one message through the accumulator even when `auto_batch` is
    /// disabled. Ineligible or over-capacity messages follow the direct path.
    pub async fn send_by_accumulator(
        &self,
        message: &Message,
    ) -> Result<SendResult, ProducerError> {
        self.send_accumulator_or_direct(message, None, SendMode::Sync)
            .await
    }

    /// Future-mode counterpart to [`Self::send_by_accumulator`].
    pub async fn send_by_accumulator_future(
        &self,
        message: &Message,
    ) -> Result<SendResult, ProducerError> {
        self.send_accumulator_or_direct(message, None, SendMode::Future)
            .await
    }

    /// Sends one message to a selected queue through the accumulator even when
    /// `auto_batch` is disabled.
    pub async fn send_to_queue_by_accumulator(
        &self,
        message: &Message,
        queue: &MessageQueue,
    ) -> Result<SendResult, ProducerError> {
        self.send_accumulator_or_direct(message, Some(queue.clone()), SendMode::Sync)
            .await
    }

    /// Future-mode counterpart to [`Self::send_to_queue_by_accumulator`].
    pub async fn send_to_queue_by_accumulator_future(
        &self,
        message: &Message,
        queue: &MessageQueue,
    ) -> Result<SendResult, ProducerError> {
        self.send_accumulator_or_direct(message, Some(queue.clone()), SendMode::Future)
            .await
    }

    /// Future-mode counterpart to Java's explicit-queue callback overload.
    /// Failed attempts retry the selected Broker up to the configured budget.
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

    /// Invokes a queue selector for the current writable route without sending.
    /// The selector receives unnamespaced queues and the caller's message;
    /// the returned queue is namespaced for use with this producer. Values
    /// captured by the closure serve the role of Java's selector `arg`.
    pub async fn invoke_message_queue_selector<F>(
        &self,
        message: &Message,
        selector: F,
        timeout: Duration,
    ) -> Result<MessageQueue, ProducerError>
    where
        F: FnOnce(&[MessageQueue], &Message) -> Option<MessageQueue> + Send,
    {
        let started = Instant::now();
        let deadline = deadline_after(timeout);
        let runtime = self.started_runtime().await?;
        let wire_message = self.message_with_namespace(message)?;
        validation::validate_message(Some(&wire_message), &self.config)?;
        let info = self
            .acquire_publish_info(&runtime, wire_message.topic(), deadline)
            .await?;
        let selected = self.select_queue_with_selector(&info, message, selector)?;
        if Instant::now().duration_since(started) > timeout {
            return Err(ProducerError::Timeout);
        }
        selected
            .map(|(_, queue)| queue)
            .ok_or(ProducerError::NoQueueSelected)
    }

    /// Runs a custom queue selector against the current writable route and
    /// sends to the returned queue when its Broker address is known.
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
        let wire_message = self.message_with_namespace(message)?;
        let use_auto_batch = auto_batch_enabled && mode != SendMode::Oneway && self.auto_batch();
        let (backpressure_enabled, backpressure) = if mode == SendMode::Future && !use_auto_batch {
            self.acquire_async_backpressure(wire_message.body().len(), deadline)
                .await?
        } else {
            (false, None)
        };
        let runtime = self.started_runtime().await?;
        validation::validate_message(Some(&wire_message), &self.config)?;
        let info = self
            .acquire_publish_info(&runtime, wire_message.topic(), deadline)
            .await?;
        let (logical_queue, mut queue) = self
            .select_queue_with_selector(&info, message, selector)?
            .ok_or(ProducerError::NoQueueSelected)?;
        if use_auto_batch {
            return self
                .send_auto_or_direct(message, Some(logical_queue), mode)
                .await
                .map(Some);
        }
        let broker = queue
            .broker_name()
            .ok_or_else(|| ProducerError::NoRoute(wire_message.topic().to_owned()))?
            .to_owned();
        let address = self
            .broker_address(&runtime, &info, &queue)?
            .ok_or_else(|| ProducerError::NoRoute(wire_message.topic().to_owned()))?;
        if mode == SendMode::Future {
            let mut hooked_message = wire_message;
            return self
                .send_future_attempts(
                    runtime.clone(),
                    &mut queue,
                    &broker,
                    &address,
                    &mut hooked_message,
                    false,
                    None,
                    deadline,
                    backpressure,
                    backpressure_enabled,
                )
                .await;
        }
        let mut hooked_message = wire_message;
        self.send_one_attempt_with_hooks(
            &runtime,
            &mut queue,
            &broker,
            &address,
            &mut hooked_message,
            false,
            mode,
            deadline,
        )
        .await
    }

    async fn send_auto_or_direct(
        &self,
        message: &Message,
        queue: Option<MessageQueue>,
        mode: SendMode,
    ) -> Result<SendResult, ProducerError> {
        self.send_with_accumulator_policy(message, queue, mode, false)
            .await
    }

    async fn send_accumulator_or_direct(
        &self,
        message: &Message,
        queue: Option<MessageQueue>,
        mode: SendMode,
    ) -> Result<SendResult, ProducerError> {
        self.send_with_accumulator_policy(message, queue, mode, true)
            .await
    }

    async fn send_with_accumulator_policy(
        &self,
        message: &Message,
        queue: Option<MessageQueue>,
        mode: SendMode,
        force_accumulate: bool,
    ) -> Result<SendResult, ProducerError> {
        let wire_message = self.message_with_namespace(message)?;
        let timeout = Duration::from_millis(self.config.send_timeout_millis().max(0) as u64);
        if mode == SendMode::Oneway
            || (!force_accumulate && !self.auto_batch())
            || !can_accumulate(&wire_message)
        {
            return self
                .send_message_direct_with_mode(&wire_message, queue.as_ref(), mode, timeout)
                .await;
        }

        self.started_runtime().await?;
        validation::validate_message(Some(&wire_message), &self.config)?;
        let settings = self.config.accumulator_settings()?;
        let mut accumulated_message = wire_message.clone();
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
                self.send_message_direct_with_mode(&wire_message, queue.as_ref(), mode, timeout)
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
        let namespaced_messages = self.messages_with_namespace(messages)?;
        let batch = create_batch(&namespaced_messages, &self.config, &self.ids)?;
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
        let namespaced_messages = self.messages_with_namespace(messages)?;
        let batch = create_batch(&namespaced_messages, &self.config, &self.ids)?;
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
        let namespaced_messages = self.messages_with_namespace(messages)?;
        let batch = create_batch(&namespaced_messages, &self.config, &self.ids)?;
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
        let (backpressure_enabled, backpressure) = if mode == SendMode::Future {
            self.acquire_async_backpressure(message.body().len(), deadline)
                .await?
        } else {
            (false, None)
        };
        let wire_message = self.message_with_namespace(message)?;
        let mut wire_queue = self.queue_with_namespace(queue)?;
        let runtime = self.started_runtime().await?;
        validation::validate_message(Some(&wire_message), &self.config)?;
        let info = self
            .acquire_publish_info(&runtime, wire_message.topic(), deadline)
            .await?;
        let broker = wire_queue
            .broker_name()
            .ok_or_else(|| ProducerError::NoRoute(wire_message.topic().to_owned()))?
            .to_owned();
        let address = self
            .broker_address(&runtime, &info, &wire_queue)?
            .ok_or_else(|| ProducerError::NoRoute(wire_message.topic().to_owned()))?;
        if mode == SendMode::Future {
            let mut hooked_message = wire_message;
            return self
                .send_future_attempts(
                    runtime.clone(),
                    &mut wire_queue,
                    &broker,
                    &address,
                    &mut hooked_message,
                    batch,
                    None,
                    deadline,
                    backpressure,
                    backpressure_enabled,
                )
                .await;
        }
        let mut hooked_message = wire_message;
        self.send_one_attempt_with_hooks(
            &runtime,
            &mut wire_queue,
            &broker,
            &address,
            &mut hooked_message,
            batch,
            mode,
            deadline,
        )
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
        let wire_message = self.message_with_namespace(message)?;
        let (backpressure_enabled, backpressure) = if mode == SendMode::Future {
            self.acquire_async_backpressure(wire_message.body().len(), deadline)
                .await?
        } else {
            (false, None)
        };
        let runtime = self.started_runtime().await?;
        validation::validate_message(Some(&wire_message), &self.config)?;
        let mut info = self
            .acquire_publish_info(&runtime, wire_message.topic(), deadline)
            .await?;
        let mut hooked_message = wire_message;
        self.ids.set_if_missing(&mut hooked_message);
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
            let topic = hooked_message.topic().to_owned();
            let latency_fault_enabled = self.send_latency_fault_enable();
            if attempt > 0 && latency_fault_enabled {
                self.reset_publish_queue_index(&topic);
            }
            if let Some(current_info) = self.current_publish_info(&runtime, &topic)? {
                info = current_info;
            }
            if !info
                .publish_queues
                .iter()
                .any(|queue| queue.topic() == Some(topic.as_str()))
            {
                info = self
                    .acquire_publish_info(&runtime, &topic, deadline)
                    .await?;
            }
            let selected = self.fault.select(
                &info.publish_queues,
                last_broker.as_deref(),
                latency_fault_enabled,
                |length| self.next_publish_queue_index(&topic, length),
                StdInstant::now(),
            );
            let Some(mut queue) = selected else {
                return last_send_result
                    .map(Some)
                    .ok_or_else(|| ProducerError::NoRoute(topic.clone()));
            };
            let Some(broker) = queue.broker_name().map(str::to_owned) else {
                return last_send_result
                    .map(Some)
                    .ok_or_else(|| ProducerError::NoRoute(topic.clone()));
            };
            last_broker = Some(broker.clone());

            let Some(address) = self.broker_address(&runtime, &info, &queue)? else {
                // Java refreshes publish info once from sendKernelImpl when the
                // broker address is absent, then lets the retry loop decide.
                let _ = runtime.update_topic_route(&topic, deadline).await?;
                info = self.current_publish_info(&runtime, &topic)?.unwrap_or(info);
                last_error = Some(ProducerError::NoRoute(topic));
                continue;
            };

            if mode == SendMode::Future {
                return self
                    .send_future_attempts(
                        runtime.clone(),
                        &mut queue,
                        &broker,
                        &address,
                        &mut hooked_message,
                        batch,
                        Some(&info),
                        deadline,
                        backpressure,
                        backpressure_enabled,
                    )
                    .await;
            }

            let request_deadline = self.request_deadline(mode, deadline, attempt, attempts);
            let started = StdInstant::now();
            let result = self
                .send_one_attempt_with_hooks(
                    &runtime,
                    &mut queue,
                    &broker,
                    &address,
                    &mut hooked_message,
                    batch,
                    mode,
                    request_deadline,
                )
                .await;
            let latency = started.elapsed();
            match result {
                Ok(Some(send_result)) => {
                    self.update_fault(&broker, latency, false, true)?;
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
                    self.update_fault(&broker, latency, false, true)?;
                    return Ok(None);
                }
                Err(error) => {
                    let (isolation, reachable, retry) = match &error {
                        ProducerError::Rpc(rpc) => {
                            let (isolation, reachable) =
                                classify_fault(rpc, mode, self.start_detector_enable());
                            (
                                isolation,
                                reachable,
                                attempt + 1 < attempts && should_retry(mode, rpc, &self.config),
                            )
                        }
                        _ => (false, true, false),
                    };
                    self.update_fault(&broker, latency, isolation, reachable)?;
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
        Err(last_error.unwrap_or_else(|| ProducerError::NoRoute(hooked_message.topic().to_owned())))
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_one_attempt_with_hooks(
        &self,
        runtime: &ClientRuntime,
        queue: &mut MessageQueue,
        broker: &str,
        address: &str,
        message: &mut Message,
        batch: bool,
        mode: SendMode,
        deadline: Instant,
    ) -> Result<Option<SendResult>, ProducerError> {
        self.ids.set_if_missing(message);
        let mut original_body = Some(message.body().to_vec());
        let compression_sys_flag = self.compress_message_before_hook(message, batch);
        self.execute_check_forbidden_hooks(message, queue, address, mode)?;
        let hooks = self.send_message_hook_snapshot();
        let mut context = self.send_message_context(message, queue, address, mode);
        Self::execute_send_message_hook_before_snapshot(&hooks, &mut context);
        // Java passes the selected MessageQueue object itself through the hook
        // context, so in-place queue edits affect the ensuing request.
        *queue = context.queue.clone();

        let prepared_result = self.prepare(context.message, batch, compression_sys_flag);
        if mode == SendMode::Future {
            context.message.set_body(
                original_body
                    .take()
                    .expect("original message body is retained"),
            );
        }
        let mut result = match prepared_result {
            Ok(prepared) => {
                self.send_one_attempt_at(SendAttempt {
                    runtime,
                    queue,
                    broker,
                    address,
                    prepared: &prepared,
                    mode,
                    deadline,
                })
                .await
            }
            Err(error) => Err(error),
        };

        if let Ok(Some(send_result)) = &mut result {
            send_result.topic = self.topic_without_namespace(&send_result.topic)?;
        }

        match &result {
            Ok(Some(send_result)) => context.send_result = Some(send_result.clone()),
            Err(error) => context.exception = Some(error),
            Ok(None) => {}
        }
        if !matches!(&result, Err(ProducerError::InvalidRetryMetadata)) {
            Self::execute_send_message_hook_after_snapshot(&hooks, &mut context);
        }
        if let Some(original_body) = original_body {
            context.message.set_body(original_body);
        }
        let hook_result = context.send_result.take();
        drop(context);
        if let (Ok(Some(send_result)), Some(hook_result)) = (&mut result, hook_result) {
            *send_result = hook_result.clone();
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_future_attempts(
        &self,
        runtime: Arc<ClientRuntime>,
        queue: &mut MessageQueue,
        broker: &str,
        address: &str,
        message: &mut Message,
        batch: bool,
        publish_info: Option<&TopicSnapshot>,
        deadline: Instant,
        backpressure: Option<AsyncBackpressurePermit>,
        backpressure_enabled: bool,
    ) -> Result<Option<SendResult>, ProducerError> {
        let executor = self.async_sender_executor();
        let producer = self
            .self_weak
            .get()
            .and_then(std::sync::Weak::upgrade)
            .ok_or(ProducerError::NotStarted)?;
        let mut queue = queue.clone();
        let broker = broker.to_owned();
        let address = address.to_owned();
        let mut message = message.clone();
        let publish_info = publish_info.cloned();
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let task: AsyncSenderTask = Box::pin(async move {
            let backpressure = backpressure;
            let result = producer
                .send_future_attempts_inline(
                    &runtime,
                    &mut queue,
                    &broker,
                    &address,
                    &mut message,
                    batch,
                    publish_info.as_ref(),
                    deadline,
                )
                .await;
            drop(backpressure);
            let _ = result_tx.send(result);
        });

        match executor.execute(task) {
            Ok(()) => match tokio::time::timeout_at(deadline, result_rx).await {
                Ok(result) => result.map_err(|_| ProducerError::AsyncSenderExecutorDropped)?,
                Err(_) => Err(ProducerError::Timeout),
            },
            Err(task) if backpressure_enabled => {
                task.await;
                result_rx
                    .await
                    .map_err(|_| ProducerError::AsyncSenderExecutorDropped)?
            }
            Err(_task) => Err(ProducerError::AsyncSenderExecutorRejected),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_future_attempts_inline(
        &self,
        runtime: &ClientRuntime,
        queue: &mut MessageQueue,
        broker: &str,
        address: &str,
        message: &mut Message,
        batch: bool,
        publish_info: Option<&TopicSnapshot>,
        deadline: Instant,
    ) -> Result<Option<SendResult>, ProducerError> {
        if Instant::now() >= deadline {
            return Err(ProducerError::Timeout);
        }

        self.ids.set_if_missing(message);
        let original_body = message.body().to_vec();
        let compression_sys_flag = self.compress_message_before_hook(message, batch);
        self.execute_check_forbidden_hooks(message, queue, address, SendMode::Future)?;
        let hooks = self.send_message_hook_snapshot();
        let mut context = self.send_message_context(message, queue, address, SendMode::Future);
        Self::execute_send_message_hook_before_snapshot(&hooks, &mut context);
        // Preserve Java's shared MessageQueue mutation semantics before the
        // asynchronous submission and any retry setup.
        *queue = context.queue.clone();

        let prepared = self.prepare(context.message, batch, compression_sys_flag);
        context.message.set_body(original_body);
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(ProducerError::InvalidRetryMetadata) => {
                return Err(ProducerError::InvalidRetryMetadata);
            }
            Err(error) => {
                context.exception = Some(&error);
                Self::execute_send_message_hook_after_snapshot(&hooks, &mut context);
                drop(context);
                return Err(error);
            }
        };
        if let Ok(topic) = self.topic_without_namespace(context.message.topic()) {
            context.message.set_topic(topic);
        }

        let attempts = attempt_count(SendMode::Future, &self.config);
        let request_queue = queue.clone();
        let mut current_broker = broker.to_owned();
        let mut current_address = address.to_owned();
        let mut outcome = Err(ProducerError::NoSendAttempt);
        let mut pending_after_called = false;
        for attempt in 0..attempts {
            if Instant::now() >= deadline {
                outcome = Err(ProducerError::Timeout);
                break;
            }
            if !pending_after_called {
                Self::execute_send_message_hook_after_snapshot(&hooks, &mut context);
                pending_after_called = true;
            }
            let started = StdInstant::now();
            let result = self
                .send_one_attempt_at(SendAttempt {
                    runtime,
                    queue: &request_queue,
                    broker: &current_broker,
                    address: &current_address,
                    prepared: &prepared,
                    mode: SendMode::Future,
                    deadline,
                })
                .await;
            let latency = started.elapsed();
            match result {
                Ok(Some(mut send_result)) => {
                    send_result.topic = match self.topic_without_namespace(&send_result.topic) {
                        Ok(topic) => topic,
                        Err(error) => {
                            outcome = Err(error);
                            break;
                        }
                    };
                    self.update_fault(&current_broker, latency, false, true)?;
                    outcome = Ok(Some(send_result));
                    break;
                }
                Ok(None) => {
                    self.update_fault(&current_broker, latency, false, true)?;
                    outcome = Ok(None);
                    break;
                }
                Err(error) => {
                    if let ProducerError::Rpc(rpc) = &error {
                        let (isolation, reachable) =
                            classify_fault(rpc, SendMode::Future, self.start_detector_enable());
                        self.update_fault(&current_broker, latency, isolation, reachable)?;
                    }
                    outcome = Err(error);
                    if attempt + 1 >= attempts
                        || !matches!(
                            &outcome,
                            Err(ProducerError::Rpc(rpc))
                                if should_retry(SendMode::Future, rpc, &self.config)
                        )
                    {
                        break;
                    }

                    let topic = prepared.topic.clone();
                    let current_info = publish_info.and_then(|original| {
                        self.cached_publish_info(&topic)
                            .or_else(|| Some(Arc::new(original.clone())))
                    });
                    if let Some(info) = current_info.as_deref() {
                        let selected = self.fault.select(
                            &info.publish_queues,
                            Some(&current_broker),
                            self.send_latency_fault_enable(),
                            |length| self.next_publish_queue_index(&topic, length),
                            StdInstant::now(),
                        );
                        let Some(selected) = selected else {
                            break;
                        };
                        let Some(next_broker) = selected.broker_name().map(str::to_owned) else {
                            break;
                        };
                        let next_address = match self.broker_address(runtime, info, &selected) {
                            Ok(Some(address)) => address,
                            Ok(None) => break,
                            Err(error) => {
                                outcome = Err(error);
                                break;
                            }
                        };
                        current_broker = next_broker;
                        current_address = next_address;
                    }
                }
            }
        }

        match &outcome {
            Ok(Some(send_result)) => context.send_result = Some(send_result.clone()),
            Err(error) => context.exception = Some(error),
            Ok(None) => {}
        }
        Self::execute_send_message_hook_after_snapshot(&hooks, &mut context);
        let hook_result = context.send_result.take();
        drop(context);
        if let (Ok(Some(send_result)), Some(hook_result)) = (&mut outcome, hook_result) {
            *send_result = hook_result.clone();
        }
        outcome
    }

    fn execute_check_forbidden_hooks(
        &self,
        message: &mut Message,
        queue: &mut MessageQueue,
        address: &str,
        communication_mode: SendMode,
    ) -> Result<(), ProducerError> {
        let hooks = self.check_forbidden_hook_snapshot();
        if hooks.is_empty() {
            return Ok(());
        }
        let mut context = CheckForbiddenContext {
            name_srv_addr: self.config.client_config().get_namesrv_addr(),
            group: self.producer_group().to_owned(),
            message,
            queue,
            broker_addr: address.to_owned(),
            communication_mode: communication_mode.into(),
            send_result: None,
            exception: None,
            arg: None,
            unit_mode: self.config.client_config().unit_mode(),
        };
        Self::execute_check_forbidden_hook_snapshot(&hooks, &mut context)
    }

    fn send_message_context<'a>(
        &'a self,
        message: &'a mut Message,
        queue: &MessageQueue,
        address: &str,
        mode: SendMode,
    ) -> SendMessageContext<'a> {
        let mut client_config = self.config.client_config().clone();
        let namespace = client_config
            .get_namespace()
            .ok()
            .flatten()
            .map(str::to_owned);
        let properties = message.properties();
        let is_transaction_half = properties
            .get(PROPERTY_TRANSACTION_PREPARED)
            .is_some_and(|value| value == "true");
        let is_delay = properties.contains_key(PROPERTY_DELAY_TIME_LEVEL)
            || properties.contains_key("__STARTDELIVERTIME")
            || properties.contains_key(PROPERTY_TIMER_DELIVER_MS)
            || properties.contains_key(PROPERTY_TIMER_DELAY_SEC)
            || properties.contains_key(PROPERTY_TIMER_DELAY_MS);
        let message_type = if is_delay {
            SendMessageType::Delay
        } else if is_transaction_half {
            SendMessageType::TransactionHalf
        } else {
            SendMessageType::Normal
        };
        SendMessageContext {
            producer_group: self.producer_group().to_owned(),
            producer: self,
            message,
            queue: queue.clone(),
            broker_addr: address.to_owned(),
            born_host: self.config.client_config().client_ip().map(str::to_owned),
            communication_mode: mode.into(),
            send_result: None,
            exception: None,
            message_type,
            namespace,
            trace_context: None,
            properties: None,
        }
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
                topic: attempt.prepared.topic.clone(),
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
            producer_group: self.producer_group().to_owned(),
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
        self.remember_publish_topic(topic);
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

    fn select_queue_with_selector<F>(
        &self,
        info: &TopicSnapshot,
        message: &Message,
        selector: F,
    ) -> Result<Option<(MessageQueue, MessageQueue)>, ProducerError>
    where
        F: FnOnce(&[MessageQueue], &Message) -> Option<MessageQueue>,
    {
        let logical_queues = info
            .publish_queues
            .iter()
            .map(|queue| self.queue_without_namespace(queue))
            .collect::<Result<Vec<_>, _>>()?;
        selector(&logical_queues, message)
            .map(|logical_queue| {
                self.queue_with_namespace(&logical_queue)
                    .map(|queue| (logical_queue, queue))
            })
            .transpose()
    }

    fn next_publish_queue_index(&self, topic: &str, queue_count: usize) -> usize {
        self.cached_publish_info_value(topic).map_or_else(
            || self.selector.next_index(topic, queue_count),
            |info| info.next_index(queue_count),
        )
    }

    fn reset_publish_queue_index(&self, topic: &str) {
        if let Some(info) = self.cached_publish_info_value(topic) {
            info.reset_index();
        } else {
            self.selector.reset(topic);
        }
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

    fn message_with_namespace(&self, message: &Message) -> Result<Message, ProducerError> {
        let mut namespaced = message.clone();
        let mut config = self.config.client_config().clone();
        if let Some(topic) = config.with_namespace(Some(message.topic()))? {
            namespaced.set_topic(topic);
        }
        Ok(namespaced)
    }

    fn messages_with_namespace(&self, messages: &[Message]) -> Result<Vec<Message>, ProducerError> {
        messages
            .iter()
            .map(|message| self.message_with_namespace(message))
            .collect()
    }

    fn topic_with_namespace(&self, topic: &str) -> Result<String, ProducerError> {
        let mut config = self.config.client_config().clone();
        Ok(config
            .with_namespace(Some(topic))?
            .unwrap_or_else(|| topic.to_owned()))
    }

    fn queue_with_namespace(&self, queue: &MessageQueue) -> Result<MessageQueue, ProducerError> {
        let mut config = self.config.client_config().clone();
        Ok(config.queue_with_namespace(queue)?)
    }

    fn queue_without_namespace(&self, queue: &MessageQueue) -> Result<MessageQueue, ProducerError> {
        let mut config = self.config.client_config().clone();
        let mut logical = queue.clone();
        if let Some(topic) = config.without_namespace(queue.topic())? {
            logical.set_topic(Some(topic));
        }
        Ok(logical)
    }

    fn topic_without_namespace(&self, topic: &str) -> Result<String, ProducerError> {
        let mut config = self.config.client_config().clone();
        Ok(config
            .without_namespace(Some(topic))?
            .unwrap_or_else(|| topic.to_owned()))
    }

    fn compress_message_before_hook(&self, message: &mut Message, batch: bool) -> i32 {
        if batch || (message.body().len() as i64) < self.config.compress_threshold() as i64 {
            return 0;
        }
        match rocketmq_classic_common::compression::compress(
            self.config.compress_type(),
            message.body(),
            self.config.compress_level(),
        ) {
            Ok(compressed) => {
                message.set_body(compressed);
                message::COMPRESSED_FLAG | self.config.compress_type().get_compression_flag()
            }
            Err(_) => 0,
        }
    }

    fn prepare(
        &self,
        message: &Message,
        batch: bool,
        compression_sys_flag: i32,
    ) -> Result<PreparedMessage, ProducerError> {
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

        let body = copy.body().to_vec();
        let mut sys_flag = compression_sys_flag;
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

    fn update_fault(
        &self,
        broker: &str,
        latency: Duration,
        isolation: bool,
        reachable: bool,
    ) -> Result<(), ProducerError> {
        if self.send_latency_fault_enable() {
            self.fault
                .update(broker, latency, isolation, reachable, StdInstant::now())
                .map_err(|error| {
                    ProducerError::FaultStrategy(error.java_exception_name().to_owned())
                })?;
        }
        Ok(())
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
        SendMode::Future => !matches!(
            error,
            RpcError::Transport(rocketmq_classic_remoting::TransportError::TooManyRequests)
        ),
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
    use crate::{
        CheckForbiddenContext, CheckForbiddenHook, CommunicationMode, ProducerQueueIndex,
        SendMessageHook,
    };
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
        let mut client_config = config.client_config().clone();
        let topic = client_config
            .with_namespace(Some("orders"))
            .unwrap()
            .unwrap_or_else(|| "orders".into());
        let manager = rocketmq_classic_core::runtime::ClientRuntimeManager::new();
        let producer = Arc::new(Producer::with_runtime_manager(config, manager));
        producer.start().await.unwrap();
        let queues = [
            MessageQueue::new(&topic, "broker-a", 0),
            MessageQueue::new(&topic, "broker-b", 0),
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
        producer.cache_publish_info(&topic, snapshot);
        (producer, queues)
    }

    #[tokio::test]
    async fn publish_info_queue_list_mutation_changes_the_next_default_send_queue() {
        let (producer, queues) =
            scripted_producer(crate::ProducerConfig::new("publish-info-live-send").unwrap()).await;
        let info = producer
            .get_topic_publish_info_table()
            .get("orders")
            .unwrap();
        let selected_queue = queues[1].clone();
        info.message_queue_list()
            .unwrap()
            .with_mut(|queues| *queues = vec![selected_queue]);
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result("live-route", "offset", 1))));

        producer
            .send(&Message::new("orders", b"live route table".to_vec()))
            .await
            .unwrap();

        assert_eq!(
            producer.scripted_transport.lock().unwrap().attempts[0].queue,
            queues[1]
        );
        producer.shutdown().await.unwrap();
    }

    #[test]
    fn live_publish_info_mutations_feed_send_routes_and_queue_cursor() {
        let producer = Producer::new(crate::ProducerConfig::new("publish-info-send").unwrap());
        let topic = "orders";
        let first = MessageQueue::new(topic, "broker-a", 0);
        let second = MessageQueue::new(topic, "broker-b", 0);
        let snapshot = Arc::new(TopicSnapshot {
            route: Arc::new(rocketmq_classic_protocol::route::TopicRouteData::default()),
            publish_queues: vec![first, second.clone()],
            subscribe_queues: Default::default(),
            endpoints: BTreeMap::new(),
            order_topic: false,
        });
        producer.cache_publish_info(topic, snapshot);
        let table = producer.get_topic_publish_info_table();
        let info = table.get(topic).unwrap();
        let list = info.message_queue_list().unwrap();
        list.with_mut(|queues| *queues = vec![second.clone()]);
        assert_eq!(
            producer.cached_publish_info(topic).unwrap().publish_queues,
            [second]
        );

        let cursor = ProducerQueueIndex::default();
        info.set_send_which_queue(cursor.clone());
        assert!(cursor.current().is_none());
        let index = producer.next_publish_queue_index(topic, 2);
        assert_eq!(
            index,
            (cursor.current().unwrap() & i32::MAX) as usize % 2,
            "fault selection advances the live TopicPublishInfo ThreadLocalIndex"
        );
    }

    #[tokio::test]
    async fn namespaced_send_uses_wire_names_and_unwraps_the_result() {
        let mut config = crate::ProducerConfig::new("namespace-send").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".into()));
        let (producer, queues) = scripted_producer(config).await;
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        type HookEvents = Arc<std::sync::Mutex<Vec<(String, Option<String>, String)>>>;
        struct Hook(HookEvents);
        impl SendMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "namespace-test"
            }

            fn send_message_before(&self, context: &mut SendMessageContext<'_>) {
                self.0.lock().unwrap().push((
                    context.message.topic().to_owned(),
                    context.namespace.clone(),
                    context.queue.topic().unwrap_or_default().to_owned(),
                ));
            }
        }
        producer.register_send_message_hook(Arc::new(Hook(seen.clone())));
        let mut response = scripted_result("msg-ns", "offset-ns", 7);
        response.topic = "tenant%orders".into();
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(response)));

        assert_eq!(producer.producer_group(), "tenant%namespace-send");
        let message = Message::new("orders", b"namespaced body".to_vec());
        let result = producer
            .send_with_timeout(&message, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(message.topic(), "orders");
        assert_eq!(result.topic, "orders");
        {
            let transport = producer.scripted_transport.lock().unwrap();
            assert_eq!(transport.attempts[0].topic, "tenant%orders");
            assert_eq!(transport.attempts[0].queue.topic(), Some("tenant%orders"));
        }
        assert_eq!(queues[0].topic(), Some("tenant%orders"));
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            &[(
                "tenant%orders".to_owned(),
                Some("tenant".to_owned()),
                "tenant%orders".to_owned(),
            )]
        );

        let logical_queue = MessageQueue::new("orders", "broker-a", 0);
        let mut response = scripted_result("msg-ns-queue", "offset-ns-queue", 8);
        response.topic = "tenant%orders".into();
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(response)));
        let queue_result = producer
            .send_to_queue_with_timeout(&message, &logical_queue, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(logical_queue.topic(), Some("orders"));
        assert_eq!(queue_result.topic, "orders");

        let mut response = scripted_result("msg-ns-selector", "offset-ns-selector", 9);
        response.topic = "tenant%orders".into();
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(response)));
        let selector_result = producer
            .send_with_selector_with_timeout(
                &message,
                |logical_queues, selected_message| {
                    assert_eq!(selected_message.topic(), "orders");
                    assert_eq!(logical_queues[0].topic(), Some("orders"));
                    Some(logical_queues[0].clone())
                },
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(selector_result.topic, "orders");
        {
            let transport = producer.scripted_transport.lock().unwrap();
            assert_eq!(transport.attempts.len(), 3);
            assert!(
                transport
                    .attempts
                    .iter()
                    .all(|attempt| attempt.topic == "tenant%orders")
            );
            assert!(
                transport
                    .attempts
                    .iter()
                    .all(|attempt| attempt.queue.topic() == Some("tenant%orders"))
            );
        }
        assert_eq!(seen.lock().unwrap().len(), 3);
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn configured_async_sender_executor_runs_future_send_attempts() {
        struct TokioSender(Arc<std::sync::atomic::AtomicUsize>);

        impl crate::AsyncSenderExecutor for TokioSender {
            fn execute(&self, task: AsyncSenderTask) -> Result<(), AsyncSenderTask> {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::spawn(task);
                Ok(())
            }
        }

        let (producer, _) =
            scripted_producer(crate::ProducerConfig::new("async-executor").unwrap()).await;
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result("async-executor", "offset", 1))));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        producer.set_async_sender_executor(Some(Arc::new(TokioSender(calls.clone()))));

        let result = producer
            .send_future(&Message::new("orders", b"executor task".to_vec()))
            .await
            .unwrap();

        assert_eq!(result.msg_id, "async-executor");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            producer.scripted_transport.lock().unwrap().attempts[0].mode,
            SendMode::Future
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_future_after_async_submission_does_not_cancel_the_send() {
        struct StartedHook(std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>);
        impl crate::SendMessageHook for StartedHook {
            fn hook_name(&self) -> &str {
                "started-hook"
            }

            fn send_message_before(&self, _context: &mut crate::SendMessageContext<'_>) {
                if let Some(started) = self.0.lock().unwrap().take() {
                    let _ = started.send(());
                }
            }
        }

        let (producer, _) =
            scripted_producer(crate::ProducerConfig::new("async-future-drop").unwrap()).await;
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result("detached-send", "offset", 1))));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        producer.register_send_message_hook(Arc::new(StartedHook(std::sync::Mutex::new(Some(
            started_tx,
        )))));

        let sending_producer = producer.clone();
        let send_future = tokio::spawn(async move {
            sending_producer
                .send_future(&Message::new("orders", b"drop result future".to_vec()))
                .await
        });
        started_rx.await.unwrap();
        send_future.abort();
        let _ = send_future.await;

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if !producer
                    .scripted_transport
                    .lock()
                    .unwrap()
                    .attempts
                    .is_empty()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("submitted send continues after its result future is dropped");
        assert_eq!(
            producer.scripted_transport.lock().unwrap().attempts.len(),
            1
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn rejected_async_sender_executor_follows_java_backpressure_fallback() {
        struct RejectSender;
        impl crate::AsyncSenderExecutor for RejectSender {
            fn execute(&self, task: AsyncSenderTask) -> Result<(), AsyncSenderTask> {
                Err(task)
            }
        }

        let (producer, _) =
            scripted_producer(crate::ProducerConfig::new("async-reject").unwrap()).await;
        producer.set_async_sender_executor(Some(Arc::new(RejectSender)));
        let error = producer
            .send_future(&Message::new("orders", b"rejected".to_vec()))
            .await
            .unwrap_err();
        assert!(matches!(error, ProducerError::AsyncSenderExecutorRejected));
        assert!(
            producer
                .scripted_transport
                .lock()
                .unwrap()
                .attempts
                .is_empty()
        );
        producer.shutdown().await.unwrap();

        let mut config = crate::ProducerConfig::new("async-reject-backpressure").unwrap();
        config.set_enable_backpressure_for_async_mode(true);
        let (producer, _) = scripted_producer(config).await;
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result("inline-fallback", "offset", 2))));
        producer.set_async_sender_executor(Some(Arc::new(RejectSender)));
        let result = producer
            .send_future(&Message::new("orders", b"inline fallback".to_vec()))
            .await
            .unwrap();
        assert_eq!(result.msg_id, "inline-fallback");
        assert_eq!(
            producer.scripted_transport.lock().unwrap().attempts.len(),
            1
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn queued_async_sender_work_obeys_the_send_deadline() {
        struct HeldSender(Arc<std::sync::Mutex<Option<AsyncSenderTask>>>);
        impl crate::AsyncSenderExecutor for HeldSender {
            fn execute(&self, task: AsyncSenderTask) -> Result<(), AsyncSenderTask> {
                *self.0.lock().unwrap() = Some(task);
                Ok(())
            }
        }

        let mut config = crate::ProducerConfig::new("async-executor-deadline").unwrap();
        config.set_enable_backpressure_for_async_mode(true);
        let (producer, _) = scripted_producer(config).await;
        let held_task = Arc::new(std::sync::Mutex::new(None));
        producer.set_async_sender_executor(Some(Arc::new(HeldSender(held_task.clone()))));
        let initial_count_permits = producer.backpressure_send_num.available_permits();
        let initial_size_permits = producer.backpressure_send_size.available_permits();
        let message = Message::new("orders", b"deadline before executor runs".to_vec());
        let message_size = message.body().len().max(1);

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            producer.send_future_with_timeout(&message, std::time::Duration::from_millis(15)),
        )
        .await
        .expect("the send should observe its deadline without executor progress");
        let error = result.unwrap_err();

        assert!(matches!(error, ProducerError::Timeout));
        assert_eq!(
            producer.backpressure_send_num.available_permits(),
            initial_count_permits - 1
        );
        assert_eq!(
            producer.backpressure_send_size.available_permits(),
            initial_size_permits - i64::try_from(message_size).unwrap()
        );
        assert!(
            producer
                .scripted_transport
                .lock()
                .unwrap()
                .attempts
                .is_empty()
        );

        let delayed_task = held_task.lock().unwrap().take().unwrap();
        delayed_task.await;
        assert_eq!(
            producer.backpressure_send_num.available_permits(),
            initial_count_permits
        );
        assert_eq!(
            producer.backpressure_send_size.available_permits(),
            initial_size_permits
        );
        assert!(
            producer
                .scripted_transport
                .lock()
                .unwrap()
                .attempts
                .is_empty()
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn producer_callback_executor_forwards_to_shared_remoting_client() {
        struct InlineCallback;
        impl rocketmq_classic_remoting::client::CallbackExecutor for InlineCallback {
            fn execute(
                &self,
                task: Box<dyn FnOnce() + Send + 'static>,
            ) -> Result<(), Box<dyn FnOnce() + Send + 'static>> {
                task();
                Ok(())
            }
        }

        let (producer, _) =
            scripted_producer(crate::ProducerConfig::new("callback-executor").unwrap()).await;
        let runtime = producer.started_runtime().await.unwrap();
        let executor: Arc<dyn rocketmq_classic_remoting::client::CallbackExecutor> =
            Arc::new(InlineCallback);
        producer
            .set_callback_executor(Some(executor.clone()))
            .await
            .unwrap();
        let current = runtime.remoting().get_callback_executor().unwrap();
        assert!(Arc::ptr_eq(&executor, &current));

        producer.set_callback_executor(None).await.unwrap();
        assert!(runtime.remoting().get_callback_executor().is_some());
        producer.shutdown().await.unwrap();
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
    async fn send_message_hooks_run_in_order_for_each_attempt_and_keep_context_state() {
        struct Hook(Arc<std::sync::Mutex<Vec<String>>>);

        impl SendMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "send-hook-test"
            }

            fn send_message_before(&self, context: &mut SendMessageContext<'_>) {
                context.message.set_tags("hooked-tag").unwrap();
                context
                    .properties
                    .get_or_insert_with(Default::default)
                    .insert("hook-state".into(), "retained".into());
                context.trace_context = Some(Box::new(17_u8));
                self.0.lock().unwrap().push("before".into());
            }

            fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
                assert_eq!(
                    context
                        .properties
                        .as_ref()
                        .and_then(|values| values.get("hook-state"))
                        .map(String::as_str),
                    Some("retained")
                );
                assert_eq!(
                    context
                        .trace_context
                        .as_deref()
                        .and_then(|value| value.downcast_ref::<u8>()),
                    Some(&17)
                );
                self.0
                    .lock()
                    .unwrap()
                    .push(if context.send_result.is_some() {
                        "after-success".into()
                    } else {
                        assert!(context.exception.is_some());
                        "after-error".into()
                    });
            }
        }

        let mut config = crate::ProducerConfig::new("send-hook-lifecycle").unwrap();
        config.set_retry_times_when_send_failed(1);
        let (producer, _) = scripted_producer(config).await;
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        producer.register_send_message_hook(Arc::new(Hook(events.clone())));
        {
            let mut scripted = producer.scripted_transport.lock().unwrap();
            scripted.responses.extend([
                Err(ProducerError::Rpc(RpcError::Response {
                    request_code: 10,
                    peer: "broker".into(),
                    code: response::SERVICE_NOT_AVAILABLE,
                    remark: Some("retry this request".into()),
                })),
                Ok(Some(scripted_result("hooked-msg", "hooked-offset", 9))),
            ]);
        }

        let message = Message::new("orders", b"hook body".to_vec());
        producer
            .send_with_timeout(&message, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["before", "after-error", "before", "after-success"]
        );
        let attempts = {
            let scripted = producer.scripted_transport.lock().unwrap();
            assert_eq!(scripted.attempts.len(), 2);
            scripted.attempts.clone()
        };
        for attempt in &attempts {
            let properties = decode_properties(attempt.properties.as_deref().unwrap());
            assert_eq!(
                properties.get(PROPERTY_TAGS).map(String::as_str),
                Some("hooked-tag")
            );
        }
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn send_message_hook_panics_are_isolated_like_java() {
        struct PanickingHook;

        impl SendMessageHook for PanickingHook {
            fn hook_name(&self) -> &str {
                "panicking-send-hook"
            }

            fn send_message_before(&self, _context: &mut SendMessageContext<'_>) {
                panic!("before hook failure");
            }

            fn send_message_after(&self, _context: &mut SendMessageContext<'_>) {
                panic!("after hook failure");
            }
        }

        struct RecordingHook(Arc<std::sync::Mutex<Vec<String>>>);

        impl SendMessageHook for RecordingHook {
            fn hook_name(&self) -> &str {
                "recording-send-hook"
            }

            fn send_message_before(&self, _context: &mut SendMessageContext<'_>) {
                self.0.lock().unwrap().push("before".into());
            }

            fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
                assert!(context.send_result.is_some());
                self.0.lock().unwrap().push("after".into());
            }
        }

        let (producer, _) =
            scripted_producer(crate::ProducerConfig::new("send-hook-panic").unwrap()).await;
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        producer.register_send_message_hook(Arc::new(PanickingHook));
        producer.register_send_message_hook(Arc::new(RecordingHook(events.clone())));
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result("panic-isolated", "offset", 1))));

        let result = producer
            .send_with_timeout(
                &Message::new("orders", b"hook body".to_vec()),
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(result.msg_id, "panic-isolated");
        assert_eq!(events.lock().unwrap().as_slice(), ["before", "after"]);
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn send_message_hook_queue_mutation_changes_the_request_queue() {
        struct Hook;
        impl SendMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "send-hook-queue-mutation"
            }

            fn send_message_before(&self, context: &mut SendMessageContext<'_>) {
                context.queue.set_queue_id(7);
            }
        }

        let config = crate::ProducerConfig::new("send-hook-queue-mutation").unwrap();
        let (producer, queues) = scripted_producer(config).await;
        producer.register_send_message_hook(Arc::new(Hook));
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result("hook-queue", "hook-offset", 1))));

        producer
            .send_with_timeout(
                &Message::new("orders", b"hook queue".to_vec()),
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        {
            let scripted = producer.scripted_transport.lock().unwrap();
            assert_eq!(scripted.attempts.len(), 1);
            assert_eq!(scripted.attempts[0].queue.queue_id(), 7);
        }
        assert_eq!(queues[0].queue_id(), 0);
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn send_message_hook_result_mutation_reaches_the_returned_result() {
        struct Hook;
        impl SendMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "send-hook-result-mutation"
            }

            fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
                if let Some(result) = &mut context.send_result {
                    result.msg_id = "hook-mutated-id".into();
                }
            }
        }

        let config = crate::ProducerConfig::new("send-hook-result-mutation").unwrap();
        let (producer, _) = scripted_producer(config).await;
        producer.register_send_message_hook(Arc::new(Hook));
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result("original-id", "offset", 1))));

        let result = producer
            .send_with_timeout(
                &Message::new("orders", b"hook result".to_vec()),
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        assert_eq!(result.msg_id, "hook-mutated-id");
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn check_forbidden_hook_runs_before_send_hooks_and_can_adjust_message_and_queue() {
        struct Forbidden(Arc<std::sync::Mutex<Vec<&'static str>>>);

        impl CheckForbiddenHook for Forbidden {
            fn hook_name(&self) -> &str {
                "forbidden-context-test"
            }

            fn check_forbidden(
                &self,
                context: &mut CheckForbiddenContext<'_>,
            ) -> Result<(), ProducerError> {
                assert_eq!(context.name_srv_addr, None);
                assert_eq!(context.group, "check-forbidden-order");
                assert_eq!(context.communication_mode, CommunicationMode::Sync);
                assert_eq!(context.broker_addr, "10.0.0.1:10911");
                assert_eq!(context.queue.broker_name(), Some("broker-a"));
                assert_eq!(context.queue.queue_id(), 0);
                assert_eq!(context.send_result, None);
                assert!(context.exception.is_none());
                assert!(context.arg.is_none());
                assert!(!context.unit_mode);
                context.message.set_tags("checked").unwrap();
                context.queue.set_queue_id(1);
                self.0.lock().unwrap().push("forbidden");
                Ok(())
            }
        }

        struct Send(Arc<std::sync::Mutex<Vec<&'static str>>>);

        impl SendMessageHook for Send {
            fn hook_name(&self) -> &str {
                "send-after-forbidden-test"
            }

            fn send_message_before(&self, context: &mut SendMessageContext<'_>) {
                assert_eq!(context.message.tags(), Some("checked"));
                self.0.lock().unwrap().push("send-before");
            }

            fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
                assert!(context.send_result.is_some());
                self.0.lock().unwrap().push("send-after");
            }
        }

        let (producer, queues) =
            scripted_producer(crate::ProducerConfig::new("check-forbidden-order").unwrap()).await;
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        producer.register_check_forbidden_hook(Arc::new(Forbidden(events.clone())));
        producer.register_send_message_hook(Arc::new(Send(events.clone())));
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result(
                "checked-message",
                "checked-offset",
                3,
            ))));

        producer
            .send_to_queue_with_timeout(
                &Message::new("orders", b"checked body".to_vec()),
                &queues[0],
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["forbidden", "send-before", "send-after"]
        );
        let attempts = producer.scripted_transport.lock().unwrap().attempts.clone();
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].queue.queue_id(), 1);
        assert_eq!(
            decode_properties(attempts[0].properties.as_deref().unwrap())
                .get(PROPERTY_TAGS)
                .map(String::as_str),
            Some("checked")
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn rejecting_check_forbidden_hook_aborts_before_rpc_and_send_hooks() {
        struct Reject;

        impl CheckForbiddenHook for Reject {
            fn hook_name(&self) -> &str {
                "reject-send-test"
            }

            fn check_forbidden(
                &self,
                _context: &mut CheckForbiddenContext<'_>,
            ) -> Result<(), ProducerError> {
                Err(ProducerError::ForbiddenTopic)
            }
        }

        let (producer, queues) =
            scripted_producer(crate::ProducerConfig::new("reject-send").unwrap()).await;
        producer.register_check_forbidden_hook(Arc::new(Reject));
        let error = producer
            .send_to_queue_with_timeout(
                &Message::new("orders", b"blocked body".to_vec()),
                &queues[0],
                Duration::from_secs(1),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, ProducerError::ForbiddenTopic));
        assert!(
            producer
                .scripted_transport
                .lock()
                .unwrap()
                .attempts
                .is_empty()
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn send_hooks_observe_compressed_body_before_and_after_sync_attempt() {
        type BodyLengths = Arc<std::sync::Mutex<Vec<usize>>>;
        struct Hook(BodyLengths);
        impl SendMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "compressed-body-hook"
            }

            fn send_message_before(&self, context: &mut SendMessageContext<'_>) {
                self.0.lock().unwrap().push(context.message.body().len());
            }

            fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
                self.0.lock().unwrap().push(context.message.body().len());
            }
        }

        let mut config = crate::ProducerConfig::new("compressed-body-hook").unwrap();
        config.set_compress_threshold(1);
        let (producer, _) = scripted_producer(config).await;
        let body_lengths = Arc::new(std::sync::Mutex::new(Vec::new()));
        producer.register_send_message_hook(Arc::new(Hook(body_lengths.clone())));
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result(
                "compressed-hook-msg",
                "compressed-hook-offset",
                11,
            ))));

        let body = vec![b'x'; 2048];
        producer
            .send_with_timeout(
                &Message::new("orders", body.clone()),
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        let attempt = producer.scripted_transport.lock().unwrap().attempts[0].clone();
        {
            let body_lengths = body_lengths.lock().unwrap();
            assert_eq!(body_lengths.len(), 2);
            assert!(
                body_lengths[0] < body.len(),
                "before hook sees compressed body"
            );
            assert_eq!(body_lengths[0], body_lengths[1]);
            assert_eq!(body_lengths[1], attempt.body.len());
        }
        assert_ne!(attempt.sys_flag & message::COMPRESSED_FLAG, 0);
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn async_send_retries_share_one_hook_context_until_terminal_result() {
        struct Hook(Arc<std::sync::Mutex<Vec<&'static str>>>);

        impl SendMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "async-retry-hook"
            }

            fn send_message_before(&self, context: &mut SendMessageContext<'_>) {
                context
                    .properties
                    .get_or_insert_with(Default::default)
                    .insert("hook-state".into(), "retained".into());
                self.0.lock().unwrap().push("before");
            }

            fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
                assert_eq!(
                    context
                        .properties
                        .as_ref()
                        .and_then(|properties| properties.get("hook-state"))
                        .map(String::as_str),
                    Some("retained")
                );
                self.0
                    .lock()
                    .unwrap()
                    .push(if context.send_result.is_some() {
                        "after-success"
                    } else if context.exception.is_some() {
                        "after-error"
                    } else {
                        "after-pending"
                    });
            }
        }

        let mut config = crate::ProducerConfig::new("async-retry-hook").unwrap();
        config.set_retry_times_when_send_async_failed(1);
        let (producer, _) = scripted_producer(config).await;
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        producer.register_send_message_hook(Arc::new(Hook(events.clone())));
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .extend([
                Err(ProducerError::Rpc(RpcError::Response {
                    request_code: 10,
                    peer: "broker".into(),
                    code: response::SERVICE_NOT_AVAILABLE,
                    remark: Some("retry this request".into()),
                })),
                Ok(Some(scripted_result(
                    "async-retry-msg",
                    "async-retry-offset",
                    9,
                ))),
            ]);

        producer
            .send_future_with_timeout(
                &Message::new("orders", b"async body".to_vec()),
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["before", "after-pending", "after-success"]
        );
        let attempts = producer.scripted_transport.lock().unwrap().attempts.clone();
        assert_eq!(attempts.len(), 2);
        assert_ne!(attempts[0].broker, attempts[1].broker);
        assert_eq!(attempts[0].body, attempts[1].body);
        assert_eq!(attempts[0].unique_id, attempts[1].unique_id);
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn explicit_queue_future_retries_on_the_same_broker_with_one_hook_lifecycle() {
        struct Hook(Arc<std::sync::Mutex<Vec<&'static str>>>);

        impl SendMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "explicit-async-retry-hook"
            }

            fn send_message_before(&self, _context: &mut SendMessageContext<'_>) {
                self.0.lock().unwrap().push("before");
            }

            fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
                self.0
                    .lock()
                    .unwrap()
                    .push(if context.send_result.is_some() {
                        "after-success"
                    } else if context.exception.is_some() {
                        "after-error"
                    } else {
                        "after-pending"
                    });
            }
        }

        let mut config = crate::ProducerConfig::new("explicit-async-retry-hook").unwrap();
        config.set_retry_times_when_send_async_failed(1);
        let (producer, queues) = scripted_producer(config).await;
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        producer.register_send_message_hook(Arc::new(Hook(events.clone())));
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .extend([
                Err(ProducerError::Rpc(RpcError::Response {
                    request_code: 10,
                    peer: "broker".into(),
                    code: response::SERVICE_NOT_AVAILABLE,
                    remark: Some("retry this request".into()),
                })),
                Ok(Some(scripted_result(
                    "explicit-async-msg",
                    "explicit-async-offset",
                    9,
                ))),
            ]);

        producer
            .send_to_queue_future_with_timeout(
                &Message::new("orders", b"async body".to_vec()),
                &queues[0],
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["before", "after-pending", "after-success"]
        );
        let attempts = producer.scripted_transport.lock().unwrap().attempts.clone();
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0].broker, attempts[1].broker);
        assert_eq!(attempts[0].address, attempts[1].address);
        assert_eq!(attempts[0].queue, queues[0]);
        assert_eq!(attempts[1].queue, queues[0]);
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn async_send_hooks_report_submission_and_terminal_failure_once_each() {
        struct Hook(Arc<std::sync::Mutex<Vec<&'static str>>>);

        impl SendMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "async-terminal-failure-hook"
            }

            fn send_message_before(&self, _context: &mut SendMessageContext<'_>) {
                self.0.lock().unwrap().push("before");
            }

            fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
                self.0
                    .lock()
                    .unwrap()
                    .push(if context.send_result.is_some() {
                        "after-success"
                    } else if context.exception.is_some() {
                        "after-error"
                    } else {
                        "after-pending"
                    });
            }
        }

        let mut config = crate::ProducerConfig::new("async-terminal-failure-hook").unwrap();
        config.set_retry_times_when_send_async_failed(1);
        let (producer, _) = scripted_producer(config).await;
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        producer.register_send_message_hook(Arc::new(Hook(events.clone())));
        let unavailable = || {
            Err(ProducerError::Rpc(RpcError::Response {
                request_code: 10,
                peer: "broker".into(),
                code: response::SERVICE_NOT_AVAILABLE,
                remark: Some("still unavailable".into()),
            }))
        };
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .extend([unavailable(), unavailable()]);

        assert!(matches!(
            producer
                .send_future_with_timeout(
                    &Message::new("orders", b"async body".to_vec()),
                    Duration::from_secs(1)
                )
                .await,
            Err(ProducerError::Rpc(RpcError::Response { .. }))
        ));
        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["before", "after-pending", "after-error"]
        );
        assert_eq!(
            producer.scripted_transport.lock().unwrap().attempts.len(),
            2
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn scripted_async_and_oneway_failures_keep_their_java_retry_policies() {
        let mut config = crate::ProducerConfig::new("scripted-mode-policy").unwrap();
        config.set_retry_times_when_send_async_failed(0);
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
    async fn oneway_send_hook_receives_submission_and_empty_result_context() {
        struct Hook(Arc<std::sync::Mutex<Vec<(String, bool, bool)>>>);

        impl SendMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "oneway-send-hook"
            }

            fn send_message_before(&self, context: &mut SendMessageContext<'_>) {
                self.0.lock().unwrap().push((
                    format!("before:{}", context.communication_mode.java_name()),
                    context.send_result.is_some(),
                    context.exception.is_some(),
                ));
            }

            fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
                self.0.lock().unwrap().push((
                    format!("after:{}", context.communication_mode.java_name()),
                    context.send_result.is_some(),
                    context.exception.is_some(),
                ));
            }
        }

        let (producer, queues) =
            scripted_producer(crate::ProducerConfig::new("oneway-send-hook").unwrap()).await;
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        producer.register_send_message_hook(Arc::new(Hook(events.clone())));
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(None));

        producer
            .send_to_queue_oneway(&Message::new("orders", b"oneway".to_vec()), &queues[0])
            .await
            .unwrap();
        assert_eq!(
            events.lock().unwrap().as_slice(),
            [
                ("before:ONEWAY".into(), false, false),
                ("after:ONEWAY".into(), false, false),
            ]
        );
        assert_eq!(
            producer.scripted_transport.lock().unwrap().attempts.len(),
            1
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn explicit_batch_send_runs_hooks_for_one_batch_request() {
        struct Hook(Arc<std::sync::Mutex<Vec<(String, usize, SendMessageType)>>>);

        impl SendMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "batch-send-hook"
            }

            fn send_message_before(&self, context: &mut SendMessageContext<'_>) {
                self.0.lock().unwrap().push((
                    "before".into(),
                    context.message.body().len(),
                    context.message_type,
                ));
            }

            fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
                self.0.lock().unwrap().push((
                    "after".into(),
                    context.message.body().len(),
                    context.message_type,
                ));
            }
        }

        let (producer, _) =
            scripted_producer(crate::ProducerConfig::new("batch-send-hook").unwrap()).await;
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        producer.register_send_message_hook(Arc::new(Hook(events.clone())));
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result("batch-result", "batch-offset", 1))));

        let messages = [
            Message::new("orders", b"first".to_vec()),
            Message::new("orders", b"second".to_vec()),
        ];
        producer.send_batch(&messages).await.unwrap();
        {
            let events = events.lock().unwrap();
            assert_eq!(events.len(), 2);
            assert_eq!(events[0].0, "before");
            assert_eq!(events[1].0, "after");
            assert!(events[0].1 > 0);
            assert_eq!(events[0].1, events[1].1);
            assert_eq!(events[0].2, SendMessageType::Normal);
            assert_eq!(events[1].2, SendMessageType::Normal);
        }
        {
            let transport = producer.scripted_transport.lock().unwrap();
            assert_eq!(transport.attempts.len(), 1);
            assert!(transport.attempts[0].batch);
        }
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn auto_batch_flushes_one_message_batch_and_fans_out_results() {
        let mut config = crate::ProducerConfig::new("scripted-auto-batch").unwrap();
        config.set_batch_max_delay_millis(15);
        let (producer, _) = scripted_producer(config).await;
        assert!(!producer.auto_batch());
        producer.set_auto_batch(true);
        assert!(producer.auto_batch());
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
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result("msg-c", "offset-c", 22))));

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

        producer.set_auto_batch(false);
        assert!(!producer.auto_batch());
        let third = Message::new("orders", b"third".to_vec());
        let third_result = producer.send(&third).await.unwrap();
        assert_eq!(third_result.msg_id, "msg-c");
        assert_eq!(third_result.queue_offset, 22);

        let attempts = {
            let mut scripted = producer.scripted_transport.lock().unwrap();
            std::mem::take(&mut scripted.attempts)
        };
        assert_eq!(attempts.len(), 2);
        assert!(attempts[0].batch);
        assert_eq!(attempts[0].mode, SendMode::Sync);
        assert!(!attempts[1].batch);
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

    #[tokio::test]
    async fn direct_and_forced_accumulator_helpers_keep_java_path_choices() {
        let mut config = crate::ProducerConfig::new("scripted-explicit-batch-paths").unwrap();
        config.set_batch_max_delay_millis(10);
        let (producer, _) = scripted_producer(config).await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        {
            let mut scripted = producer.scripted_transport.lock().unwrap();
            for (msg_id, offset) in [
                ("direct-default", 10),
                ("direct-queue", 11),
                ("accumulator-default", 12),
                ("accumulator-queue", 13),
            ] {
                scripted.responses.push_back(Ok(Some(scripted_result(
                    msg_id,
                    &offset.to_string(),
                    offset,
                ))));
            }
        }

        producer.set_auto_batch(true);
        producer
            .send_direct(&Message::new("orders", b"direct".to_vec()))
            .await
            .unwrap();
        producer
            .send_to_queue_direct_future(&Message::new("orders", b"direct queue".to_vec()), &queue)
            .await
            .unwrap();

        producer.set_auto_batch(false);
        producer
            .send_by_accumulator(&Message::new("orders", b"accumulate".to_vec()))
            .await
            .unwrap();
        producer
            .send_to_queue_by_accumulator_future(
                &Message::new("orders", b"accumulate queue".to_vec()),
                &queue,
            )
            .await
            .unwrap();

        let attempts = {
            let scripted = producer.scripted_transport.lock().unwrap();
            scripted.attempts.clone()
        };
        assert_eq!(attempts.len(), 4);
        assert_eq!(attempts[0].mode, SendMode::Sync);
        assert!(
            !attempts[0].batch,
            "direct must bypass enabled auto-batching"
        );
        assert_eq!(attempts[1].mode, SendMode::Future);
        assert!(!attempts[1].batch);
        assert_eq!(attempts[1].queue, queue);
        assert_eq!(attempts[2].mode, SendMode::Sync);
        assert!(
            attempts[2].batch,
            "forced accumulation must ignore auto_batch=false"
        );
        assert_eq!(attempts[3].mode, SendMode::Future);
        assert!(attempts[3].batch);
        assert_eq!(attempts[3].queue, queue);

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
    fn sync_and_future_retry_broker_responses_using_java_policies() {
        let config = crate::ProducerConfig::new("producer").unwrap();
        let retryable = RpcError::Response {
            request_code: 10,
            peer: "broker".into(),
            code: response::SERVICE_NOT_AVAILABLE,
            remark: None,
        };
        assert!(should_retry(SendMode::Sync, &retryable, &config));
        assert!(should_retry(SendMode::Future, &retryable, &config));
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
    fn send_hook_message_type_matches_java_transaction_and_timer_precedence() {
        let producer = Producer::new(crate::ProducerConfig::new("send-hook-type").unwrap());
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let defaults = &fixture["sendMessageContext"]["defaults"];
        let mut normal = Message::new("orders", b"normal".to_vec());
        let normal_context =
            producer.send_message_context(&mut normal, &queue, "broker-a:10911", SendMode::Sync);
        assert_eq!(
            normal_context.message_type.java_name(),
            defaults["messageType"]
        );
        assert_eq!(
            normal_context.send_result.is_none(),
            defaults["sendResultIsNull"]
        );
        assert_eq!(
            normal_context.exception.is_none(),
            defaults["exceptionIsNull"]
        );
        assert_eq!(
            normal_context.trace_context.is_none(),
            defaults["traceContextIsNull"]
        );
        assert_eq!(normal_context.properties.is_none(), defaults["propsIsNull"]);
        assert_eq!(
            normal_context.namespace.is_none(),
            defaults["namespaceIsNull"]
        );

        let mut transaction = Message::new("orders", b"transaction".to_vec());
        transaction
            .set_system_property(PROPERTY_TRANSACTION_PREPARED, "true")
            .unwrap();
        assert_eq!(
            producer
                .send_message_context(&mut transaction, &queue, "broker-a:10911", SendMode::Sync)
                .message_type,
            SendMessageType::TransactionHalf
        );

        for property in [
            PROPERTY_DELAY_TIME_LEVEL,
            "__STARTDELIVERTIME",
            PROPERTY_TIMER_DELIVER_MS,
            PROPERTY_TIMER_DELAY_SEC,
            PROPERTY_TIMER_DELAY_MS,
        ] {
            let mut delayed = Message::new("orders", b"delayed".to_vec());
            delayed
                .set_system_property(PROPERTY_TRANSACTION_PREPARED, "true")
                .unwrap();
            delayed.set_system_property(property, "1").unwrap();
            assert_eq!(
                producer
                    .send_message_context(&mut delayed, &queue, "broker-a:10911", SendMode::Sync)
                    .message_type,
                SendMessageType::Delay,
                "{property} must override transaction message type like Java"
            );
        }
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
            let mut message_for_send = message.clone();
            let compression_sys_flag =
                producer.compress_message_before_hook(&mut message_for_send, false);
            let prepared = producer
                .prepare(&message_for_send, false, compression_sys_flag)
                .unwrap();
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

            let batch_prepared = producer.prepare(&message, true, 0).unwrap();
            assert_eq!(batch_prepared.sys_flag, 0);
            assert_eq!(batch_prepared.body, message.body());
        }
    }

    #[test]
    fn runtime_compression_settings_match_java_and_config_clones_are_snapshots() {
        let config = crate::ProducerConfig::new("runtime-compression-test").unwrap();
        let snapshot = config.clone();
        let producer = crate::Producer::new(config);
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let runtime = &fixture["runtimeCompressionSettings"];

        let threshold = runtime["threshold"].as_i64().unwrap() as i32;
        producer.set_compress_threshold(threshold);
        assert_eq!(producer.compress_threshold(), threshold);
        for case in runtime["thresholdCases"].as_array().unwrap() {
            let length = case["bodyLength"].as_u64().unwrap() as usize;
            let expected = case["compressed"].as_bool().unwrap();
            let original = Message::new("orders", vec![b'x'; length]);
            let mut message = original.clone();
            let flag = producer.compress_message_before_hook(&mut message, false);
            let prepared = producer.prepare(&message, false, flag).unwrap();
            assert_eq!(
                message::has_compressed_flag(prepared.sys_flag),
                expected,
                "Java runtime compression threshold case {case}"
            );
        }

        let updated_threshold = runtime["thresholdAfterUpdate"].as_i64().unwrap() as i32;
        producer.set_compress_threshold(updated_threshold);
        assert_eq!(producer.compress_threshold(), updated_threshold);

        let body = vec![b'x'; 16_384];
        let mut compressed_by_level = Vec::new();
        for level_name in ["level1", "level9"] {
            let level = runtime[level_name]["level"].as_i64().unwrap() as i32;
            producer.set_compress_level(level);
            assert_eq!(producer.compress_level(), level);
            let original = Message::new("orders", body.clone());
            let mut message = original.clone();
            let flag = producer.compress_message_before_hook(&mut message, false);
            let prepared = producer.prepare(&message, false, flag).unwrap();
            assert_eq!(
                message::has_compressed_flag(prepared.sys_flag),
                runtime[level_name]["compressed"].as_bool().unwrap()
            );
            assert_eq!(
                rocketmq_classic_common::compression::decompress(
                    rocketmq_classic_common::sysflag::message::CompressionType::Zlib,
                    &prepared.body
                )
                .unwrap(),
                body
            );
            compressed_by_level.push(prepared.body);
        }
        assert_ne!(
            compressed_by_level[0], compressed_by_level[1],
            "runtime compression level changes subsequent compressed output"
        );
        assert_ne!(
            runtime["level1"]["compressedBodyLength"], runtime["level9"]["compressedBodyLength"],
            "the invoked Java fixture confirms these levels produce distinct output"
        );

        assert_eq!(snapshot.compress_threshold(), 4_096);
        assert_eq!(snapshot.compress_level(), 5);
    }

    #[test]
    fn runtime_fault_strategy_settings_match_java_and_change_later_selection() {
        let producer =
            crate::Producer::new(crate::ProducerConfig::new("runtime-fault-settings").unwrap());
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let runtime = &fixture["runtimeFaultStrategy"];
        let defaults = &fixture["faultStrategy"];
        let values = |name: &str| {
            runtime[name]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_i64().unwrap())
                .collect::<Vec<_>>()
        };

        let latency_input = crate::ProducerLongArray::new(values("initialLatencyMaxMillis"));
        let duration_input =
            crate::ProducerLongArray::new(values("initialNotAvailableDurationMillis"));
        assert_eq!(
            producer.latency_max_millis().snapshot(),
            Some(
                defaults["latencyMaxMillis"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|value| value.as_i64().unwrap())
                    .collect::<Vec<_>>(),
            )
        );
        assert_eq!(
            producer.not_available_duration_millis().snapshot(),
            Some(
                defaults["notAvailableDurationMillis"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|value| value.as_i64().unwrap())
                    .collect::<Vec<_>>(),
            )
        );
        producer.set_latency_max_millis(latency_input.clone());
        producer.set_not_available_duration_millis(duration_input.clone());
        producer.set_send_latency_fault_enable(true);
        producer.set_start_detector_enable(true);
        let mutation = |name: &str| {
            let mutation = &runtime[name];
            (
                mutation["index"].as_u64().unwrap() as usize,
                mutation["value"].as_i64().unwrap(),
            )
        };
        let (index, value) = mutation("latencyInputMutation");
        let previous = latency_input.get(index);
        assert_eq!(latency_input.set(index, value), previous);
        let (index, value) = mutation("latencyGetterMutation");
        let latency_getter = producer.latency_max_millis();
        let previous = latency_getter.get(index);
        assert_eq!(latency_getter.set(index, value), previous);
        let (index, value) = mutation("durationInputMutation");
        let previous = duration_input.get(index);
        assert_eq!(duration_input.set(index, value), previous);
        let (index, value) = mutation("durationGetterMutation");
        let duration_getter = producer.not_available_duration_millis();
        let previous = duration_getter.get(index);
        assert_eq!(duration_getter.set(index, value), previous);
        assert_eq!(producer.latency_max_millis(), latency_input);
        assert_eq!(producer.not_available_duration_millis(), duration_input);
        assert_eq!(latency_getter.snapshot(), Some(values("latencyMaxMillis")));
        assert_eq!(
            duration_getter.snapshot(),
            Some(values("notAvailableDurationMillis"))
        );
        assert!(runtime["latencyArrayIdentityRetained"].as_bool().unwrap());
        assert!(runtime["durationArrayIdentityRetained"].as_bool().unwrap());
        assert!(runtime["latencyGetterIdentityRetained"].as_bool().unwrap());
        assert!(runtime["durationGetterIdentityRetained"].as_bool().unwrap());
        assert_eq!(
            producer.send_latency_fault_enable(),
            runtime["sendLatencyFaultEnableAfterToggle"]
        );
        assert_eq!(
            producer.start_detector_enable(),
            runtime["startDetectorEnableAfterToggle"]
        );

        producer
            .update_fault("enabled-broker", Duration::from_millis(18), false, true)
            .unwrap();
        let available_queues = [
            MessageQueue::new("orders", "enabled-broker", 0),
            MessageQueue::new("orders", "fallback-broker", 0),
        ];
        let mut index = 0;
        let selected = producer
            .fault
            .select(
                &available_queues,
                None,
                producer.send_latency_fault_enable(),
                |length| {
                    let current = index;
                    index += 1;
                    current % length
                },
                StdInstant::now(),
            )
            .unwrap();
        assert_eq!(
            selected.broker_name(),
            Some(if runtime["enabledBrokerAvailable"].as_bool().unwrap() {
                "enabled-broker"
            } else {
                "fallback-broker"
            })
        );

        producer.set_send_latency_fault_enable(false);
        producer
            .update_fault("disabled-broker", Duration::from_millis(15), false, true)
            .unwrap();
        assert_eq!(
            producer.send_latency_fault_enable(),
            runtime["sendLatencyFaultEnableAfterDisable"]
        );
        producer.set_send_latency_fault_enable(true);
        let disabled_queues = [
            MessageQueue::new("orders", "disabled-broker", 0),
            MessageQueue::new("orders", "fallback-broker", 0),
        ];
        assert_eq!(
            producer
                .fault
                .select(
                    &disabled_queues,
                    None,
                    producer.send_latency_fault_enable(),
                    |_| 0,
                    StdInstant::now(),
                )
                .unwrap()
                .broker_name(),
            Some(if runtime["disabledBrokerAvailable"].as_bool().unwrap() {
                "disabled-broker"
            } else {
                "fallback-broker"
            })
        );

        let prior_latency = producer.latency_max_millis();
        let replacement_latency = crate::ProducerLongArray::new(vec![20]);
        producer.set_latency_max_millis(replacement_latency.clone());
        assert!(prior_latency.set(0, 0).is_some());
        assert_eq!(
            replacement_latency.snapshot(),
            Some(values("latencyReplacementValues"))
        );
        assert!(
            runtime["latencyReplacementIdentityRetained"]
                .as_bool()
                .unwrap()
        );
        let prior_duration = producer.not_available_duration_millis();
        let replacement_duration = crate::ProducerLongArray::new(vec![0]);
        producer.set_not_available_duration_millis(replacement_duration.clone());
        assert!(prior_duration.set(0, 999).is_some());
        assert_eq!(
            replacement_duration.snapshot(),
            Some(values("durationReplacementValues"))
        );
        assert!(
            runtime["durationReplacementIdentityRetained"]
                .as_bool()
                .unwrap()
        );
        producer
            .update_fault(
                "updated-table-broker",
                Duration::from_millis(15),
                false,
                true,
            )
            .unwrap();
        let updated_table_queues = [
            MessageQueue::new("orders", "updated-table-broker", 0),
            MessageQueue::new("orders", "fallback-broker", 0),
        ];
        assert_eq!(
            producer
                .fault
                .select(
                    &updated_table_queues,
                    None,
                    producer.send_latency_fault_enable(),
                    |_| 0,
                    StdInstant::now(),
                )
                .unwrap()
                .broker_name(),
            Some(
                if runtime["updatedTableBrokerAvailable"].as_bool().unwrap() {
                    "updated-table-broker"
                } else {
                    "fallback-broker"
                }
            )
        );
        assert_eq!(
            producer.send_latency_fault_enable(),
            runtime["sendLatencyFaultEnableAfterReenable"]
        );
        producer.set_start_detector_enable(false);
        assert_eq!(
            producer.start_detector_enable(),
            runtime["startDetectorEnableAfterDisable"]
        );
    }

    #[test]
    fn runtime_fault_strategy_null_and_mismatched_arrays_match_java() {
        let producer =
            crate::Producer::new(crate::ProducerConfig::new("fault-table-null-inputs").unwrap());
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let runtime = &fixture["runtimeFaultStrategyNullArrays"];
        producer.set_send_latency_fault_enable(true);

        producer.set_latency_max_millis(None);
        let null_latency_max = producer.latency_max_millis();
        assert!(null_latency_max.is_null());
        assert_eq!(null_latency_max.snapshot(), None);
        assert_eq!(null_latency_max.len(), 0);
        assert!(null_latency_max.is_empty());
        assert!(null_latency_max.get(0).is_none());
        assert!(null_latency_max.set(0, 10).is_none());
        assert_eq!(
            producer
                .update_fault("null-latency-max", Duration::from_millis(10), false, true)
                .unwrap_err()
                .to_string(),
            runtime["nullLatencyMaxUpdateFailure"].as_str().unwrap()
        );

        producer.set_latency_max_millis(vec![10]);
        producer.set_not_available_duration_millis(None);
        let null_duration = producer.not_available_duration_millis();
        assert!(null_duration.is_null());
        assert_eq!(null_duration.snapshot(), None);
        assert!(
            producer
                .update_fault(
                    "null-duration-unmatched",
                    Duration::from_millis(9),
                    false,
                    true
                )
                .is_ok()
        );
        assert_eq!(
            producer
                .update_fault(
                    "null-duration-matched",
                    Duration::from_millis(10),
                    false,
                    true
                )
                .unwrap_err()
                .to_string(),
            runtime["matchedLatencyWithNullDurationFailure"]
                .as_str()
                .unwrap()
        );

        producer.set_latency_max_millis(vec![10, 20]);
        producer.set_not_available_duration_millis(vec![100]);
        assert_eq!(
            producer
                .update_fault(
                    "short-duration-table",
                    Duration::from_millis(20),
                    false,
                    true
                )
                .unwrap_err()
                .to_string(),
            runtime["shortDurationTableFailure"].as_str().unwrap()
        );

        producer.set_latency_max_millis(vec![20]);
        producer.set_not_available_duration_millis(vec![100, 200]);
        assert!(
            producer
                .update_fault(
                    "extra-duration-rows",
                    Duration::from_millis(20),
                    false,
                    true
                )
                .is_ok()
        );
        assert_eq!(
            runtime["latencyMaxGetterIsNull"],
            serde_json::Value::Bool(true)
        );
        assert_eq!(
            runtime["notAvailableDurationGetterIsNull"],
            serde_json::Value::Bool(true)
        );
        assert_eq!(runtime["unmatchedLatencyWithNullDurationFailure"], "none");
        assert_eq!(runtime["extraDurationRowsFailure"], "none");
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
            let (enabled, permit) = producer
                .acquire_async_backpressure(1, Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            assert!(enabled);
            permits.push(permit.unwrap());
        }
        assert!(matches!(
            producer
                .acquire_async_backpressure(1, Instant::now() + Duration::from_millis(5))
                .await,
            Err(ProducerError::Timeout)
        ));
        drop(permits);
        let (enabled, permit) = producer
            .acquire_async_backpressure(1, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(enabled);
        assert!(permit.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn producer_runtime_backpressure_settings_update_live_limits_and_mode() {
        let producer = Producer::new(crate::ProducerConfig::new("runtime-backpressure").unwrap());
        assert!(!producer.enable_backpressure_for_async_mode());
        let (enabled, permit) = producer
            .acquire_async_backpressure(1, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(!enabled);
        assert!(permit.is_none());

        producer.set_enable_backpressure_for_async_mode(true);
        producer.set_backpressure_async_send_num(1);
        producer.set_backpressure_async_send_size(1);
        assert!(producer.enable_backpressure_for_async_mode());
        assert!(producer.config().enable_backpressure_for_async_mode());
        assert_eq!(producer.backpressure_async_send_num(), 10);
        assert_eq!(producer.backpressure_async_send_size(), 1024 * 1024);
        drop(producer.acquire_backpressure_for_async_send_num_lock());
        drop(producer.acquire_backpressure_for_async_send_size_lock());

        let (enabled, permit) = producer
            .acquire_async_backpressure(1, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(enabled);
        assert!(permit.is_some());
        producer.set_backpressure_async_send_num(12);
        assert_eq!(producer.backpressure_send_num.available_permits(), 11);
        producer.set_backpressure_async_send_num_inside_adjust(13);
        producer.set_backpressure_async_send_size_inside_adjust(2 * 1024 * 1024 + 1);
        assert_eq!(producer.backpressure_async_send_num(), 13);
        assert_eq!(producer.backpressure_async_send_size(), 2 * 1024 * 1024 + 1);
        producer.set_backpressure_async_send_num(12);
        producer.set_backpressure_async_send_size(2 * 1024 * 1024);

        producer.set_enable_backpressure_for_async_mode(false);
        assert!(!producer.enable_backpressure_for_async_mode());
        assert!(!producer.config().enable_backpressure_for_async_mode());
        let (enabled, permit) = producer
            .acquire_async_backpressure(1, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(!enabled);
        assert!(permit.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn runtime_resize_preserves_in_flight_count_like_the_java_fixture() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let runtime = &fixture["runtimeSettings"];
        let mut config = crate::ProducerConfig::new("runtime-resize-fixture").unwrap();
        config.set_enable_backpressure_for_async_mode(true);
        config.set_backpressure_async_send_num(20);
        config.set_backpressure_async_send_size(2 * 1024 * 1024);
        let producer = Producer::new(config);
        let mut held_count_permits = Vec::new();

        for _ in 0..7 {
            let (enabled, permits) = producer
                .acquire_async_backpressure(1, Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            assert!(enabled);
            let (count, size) = permits.unwrap();
            held_count_permits.push(count);
            drop(size);
        }

        producer.set_backpressure_async_send_num(12);
        assert_eq!(
            i64::from(producer.semaphore_async_send_num_available_permits()),
            runtime["sendNumPermitsAfterResize"].as_i64().unwrap()
        );
        assert_eq!(
            i64::from(producer.semaphore_async_send_size_available_permits()),
            runtime["sendSizePermitsAfterUpdate"].as_i64().unwrap()
        );
        drop(held_count_permits);
        assert_eq!(
            i64::from(producer.semaphore_async_send_num_available_permits()),
            runtime["sendNumPermitsAfterInFlightRelease"]
                .as_i64()
                .unwrap()
        );
    }

    #[test]
    fn producer_retry_response_codes_are_live_for_sync_retry_decisions() {
        let producer = Producer::new(crate::ProducerConfig::new("runtime-retry-codes").unwrap());
        let code = 9_001;
        let error = RpcError::Response {
            request_code: 10,
            peer: "broker".into(),
            code,
            remark: None,
        };
        let live_codes = producer.retry_response_codes();
        assert!(!live_codes.contains(&code));
        assert!(!should_retry(SendMode::Sync, &error, producer.config()));

        producer.add_retry_response_code(code);
        assert!(live_codes.contains(&code));
        assert!(should_retry(SendMode::Sync, &error, producer.config()));

        live_codes.remove(&code);
        assert!(!should_retry(SendMode::Sync, &error, producer.config()));
    }
}
