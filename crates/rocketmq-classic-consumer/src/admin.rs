use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::offset::OffsetStore;
use rocketmq_classic_common::{
    message::{DecodeOptions, MessageExt, MessageQueue, decode_one},
    topic::TopicValidator,
};
use rocketmq_classic_core::{
    api::admin::{AdminQueryError, QueryResult},
    config::ClientConfig,
    error::RpcError,
    route::broker_vip_channel,
    runtime::{ClientRuntime, RuntimeError},
};
use rocketmq_classic_protocol::{
    SharedValue,
    body::{GetConsumerStatusBody, ResetOffsetBody},
    codes::{request, response},
    header::{
        CommandHeader, ConsumeMessageDirectlyResultRequestHeader, CreateTopicRequestHeader,
        GetConsumerRunningInfoRequestHeader, GetConsumerStatusRequestHeader,
        ResetOffsetRequestHeader,
    },
};
use rocketmq_classic_remoting::{RemotingCommand, client::RequestProcessor};
use std::sync::Weak;
use tokio::time::Instant;

const ADMIN_TIMEOUT: Duration = Duration::from_millis(6_000);
const UNIQUE_KEY_LOOKBACK_MILLIS: i64 = 3 * 24 * 60 * 60 * 1_000;

pub(crate) fn unique_key_lookup_begin_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
        .saturating_sub(UNIQUE_KEY_LOOKBACK_MILLIS)
}

pub(crate) fn first_query_message(result: QueryResult) -> Option<MessageExt> {
    let messages = result.get_message_list()?;
    let message = messages.snapshot().into_iter().next()?;
    message.map(|message| message.snapshot())
}

pub(crate) fn validate_topic(topic: &str) -> Result<(), String> {
    let validation = TopicValidator::validate_topic(Some(topic));
    if !validation.is_valid() {
        return Err(validation.remark().unwrap_or_default().to_owned());
    }
    if TopicValidator::is_system_topic(topic) {
        return Err(format!("The specified topic: {topic}, is a system topic."));
    }
    Ok(())
}

pub(crate) async fn create_topic(
    runtime: &ClientRuntime,
    key: &str,
    topic: &str,
    queue_num: i32,
    topic_sys_flag: i32,
) -> Result<(), AdminQueryError> {
    let deadline = Instant::now() + ADMIN_TIMEOUT;
    let route = runtime
        .api()
        .get_topic_route(key, deadline)
        .await?
        .ok_or_else(|| no_broker_error(key))?;
    let rows = route
        .broker_datas_snapshot()
        .ok_or(RuntimeError::InvalidRoute(
            "null broker list during consumer admin topic creation",
        ))?;
    let mut brokers = Vec::with_capacity(rows.len());
    for broker in rows {
        let broker = broker.ok_or(RuntimeError::InvalidRoute(
            "null broker data during consumer admin topic creation",
        ))?;
        if broker.broker_name.is_none() {
            return Err(RuntimeError::InvalidRoute(
                "null broker name during consumer admin topic creation",
            )
            .into());
        }
        brokers.push(broker);
    }
    if brokers.is_empty() {
        return Err(no_broker_error(key).into());
    }
    brokers.sort_by(|left, right| {
        left.broker_name
            .as_deref()
            .unwrap_or_default()
            .encode_utf16()
            .cmp(
                right
                    .broker_name
                    .as_deref()
                    .unwrap_or_default()
                    .encode_utf16(),
            )
    });

    let header = CreateTopicRequestHeader {
        topic: topic.to_owned(),
        default_topic: key.to_owned(),
        read_queue_nums: queue_num,
        write_queue_nums: queue_num,
        perm: 6,
        topic_filter_type: "SINGLE_TAG".into(),
        topic_sys_flag: Some(topic_sys_flag),
        order: Some(false),
        attributes: Some(String::new()),
        force: Some(false),
        ..Default::default()
    };
    let vip_enabled = runtime
        .config()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .vip_channel_enabled();
    let mut created = false;
    let mut last_error = None;
    for broker in brokers {
        let addresses = broker
            .broker_addrs
            .as_ref()
            .ok_or(RuntimeError::InvalidRoute(
                "null broker address map during consumer admin topic creation",
            ))?;
        let master = addresses.with(|entries| entries.get(&Some(0)).and_then(Clone::clone));
        let Some(master) = master else {
            continue;
        };
        let address = broker_vip_channel(vip_enabled, &master)?;
        for _ in 0..5 {
            match runtime
                .api()
                .create_topic(&address, &header, Instant::now() + ADMIN_TIMEOUT)
                .await
            {
                Ok(()) => {
                    created = true;
                    break;
                }
                Err(error) => last_error = Some(error),
            }
        }
    }
    if !created {
        if let Some(error) = last_error {
            return Err(error.into());
        }
    }
    Ok(())
}

pub(crate) async fn fetch_subscribe_queues(
    runtime: &ClientRuntime,
    topic: &str,
    timeout: Duration,
) -> Result<BTreeSet<MessageQueue>, AdminQueryError> {
    if !runtime.route_snapshot()?.topics.contains_key(topic) {
        runtime
            .update_topic_route(topic, Instant::now() + timeout)
            .await?;
    }
    runtime
        .route_snapshot()?
        .topics
        .get(topic)
        .map(|snapshot| snapshot.subscribe_queues.clone())
        .ok_or_else(|| no_broker_error(topic).into())
}

fn no_broker_error(topic: &str) -> RpcError {
    RpcError::Response {
        request_code: request::GET_ROUTEINFO_BY_TOPIC,
        peer: topic.to_owned(),
        code: response::TOPIC_NOT_EXIST,
        remark: Some(format!("The topic[{topic}] not exist")),
    }
}

pub(crate) struct ConsumerStatusProcessor {
    offset_store: Weak<dyn OffsetStore>,
}

pub(crate) struct ResetOffsetProcessor {
    consumer: Weak<crate::push::PushConsumer>,
    runtime: Weak<ClientRuntime>,
}

pub(crate) struct DirectConsumeProcessor {
    consumer: Weak<crate::push::PushConsumer>,
    runtime: Weak<ClientRuntime>,
}

enum RunningInfoConsumer {
    Push {
        consumer: Weak<crate::push::PushConsumer>,
        offset_store: Weak<dyn OffsetStore>,
    },
    Pull(Weak<crate::pull::PullConsumer>),
}

pub(crate) struct ConsumerRunningInfoProcessor {
    consumer: RunningInfoConsumer,
    runtime: Weak<ClientRuntime>,
}

impl DirectConsumeProcessor {
    pub(crate) fn new(
        consumer: Weak<crate::push::PushConsumer>,
        runtime: Weak<ClientRuntime>,
    ) -> Self {
        Self { consumer, runtime }
    }
}

impl ConsumerRunningInfoProcessor {
    pub(crate) fn for_push(
        consumer: Weak<crate::push::PushConsumer>,
        offset_store: Weak<dyn OffsetStore>,
        runtime: Weak<ClientRuntime>,
    ) -> Self {
        Self {
            consumer: RunningInfoConsumer::Push {
                consumer,
                offset_store,
            },
            runtime,
        }
    }

    pub(crate) fn for_pull(
        consumer: Weak<crate::pull::PullConsumer>,
        runtime: Weak<ClientRuntime>,
    ) -> Self {
        Self {
            consumer: RunningInfoConsumer::Pull(consumer),
            runtime,
        }
    }
}

impl RequestProcessor for ConsumerRunningInfoProcessor {
    fn process(&self, command: RemotingCommand) -> Result<Option<RemotingCommand>, String> {
        let fields = command
            .ext_fields
            .as_ref()
            .ok_or_else(|| "consumer running info request has no ext fields".to_owned())?;
        let header = GetConsumerRunningInfoRequestHeader::decode_fields(fields)
            .map_err(|error| error.to_string())?;
        let Some(runtime) = self.runtime.upgrade() else {
            return Ok(Some(group_not_found_error(&header.consumer_group)));
        };
        let jstack_enabled = header.jstack_enable;
        let body = match &self.consumer {
            RunningInfoConsumer::Push {
                consumer,
                offset_store,
            } => consumer.upgrade().map(|consumer| {
                let jstack = jstack_enabled.then(crate::stack::dump_threads);
                consumer.running_info_body(&runtime, jstack, offset_store.upgrade())
            }),
            RunningInfoConsumer::Pull(consumer) => consumer.upgrade().map(|consumer| {
                let jstack = jstack_enabled.then(crate::stack::dump_threads);
                consumer.running_info_body(&runtime, jstack)
            }),
        };
        let Some(body) = body else {
            return Ok(Some(group_not_found_error(&header.consumer_group)));
        };
        let body = body.encode_java_json().map_err(|error| error.to_string())?;
        Ok(Some(RemotingCommand {
            code: response::SUCCESS,
            body: Some(body),
            ..Default::default()
        }))
    }
}

pub(crate) fn push_running_info_properties(
    config: &crate::push::PushConsumerConfig,
    runtime: &ClientRuntime,
    start_timestamp: i64,
    consume_orderly: bool,
    core_pool_size: i32,
) -> BTreeMap<String, String> {
    let mut properties = BTreeMap::new();
    if let Some(consumer_group) = config.consumer_group() {
        properties.insert("consumerGroup".into(), consumer_group);
    }
    properties.insert("messageModel".into(), config.message_model().name().into());
    properties.insert(
        "consumeFromWhere".into(),
        config.consume_from_where().name().into(),
    );
    properties.insert(
        "consumeTimestamp".into(),
        config.consume_timestamp().to_owned(),
    );
    properties.insert(
        "consumeThreadMin".into(),
        config.consume_thread_min().to_string(),
    );
    properties.insert(
        "consumeThreadMax".into(),
        config.consume_thread_max().to_string(),
    );
    properties.insert(
        "adjustThreadPoolNumsThreshold".into(),
        config.adjust_thread_pool_nums_threshold().to_string(),
    );
    properties.insert(
        "consumeConcurrentlyMaxSpan".into(),
        config.consume_concurrently_max_span().to_string(),
    );
    properties.insert(
        "pullThresholdForQueue".into(),
        config.pull_threshold_for_queue().to_string(),
    );
    properties.insert(
        "popThresholdForQueue".into(),
        config.pop_threshold_for_queue().to_string(),
    );
    properties.insert(
        "pullThresholdSizeForQueue".into(),
        config.pull_threshold_size_for_queue_mib().to_string(),
    );
    properties.insert(
        "pullThresholdForTopic".into(),
        config.pull_threshold_for_topic().to_string(),
    );
    properties.insert(
        "pullThresholdSizeForTopic".into(),
        config.pull_threshold_size_for_topic_mib().to_string(),
    );
    properties.insert(
        "pullInterval".into(),
        config.pull_interval_millis().to_string(),
    );
    properties.insert(
        "consumeMessageBatchMaxSize".into(),
        config.consume_message_batch_max_size().to_string(),
    );
    properties.insert("pullBatchSize".into(), config.pull_batch_size().to_string());
    properties.insert(
        "pullBatchSizeInBytes".into(),
        config.pull_batch_size_in_bytes().to_string(),
    );
    properties.insert(
        "postSubscriptionWhenPull".into(),
        config.post_subscription_when_pull().to_string(),
    );
    properties.insert("unitMode".into(), config.unit_mode().to_string());
    properties.insert(
        "maxReconsumeTimes".into(),
        config.max_reconsume_times().to_string(),
    );
    properties.insert(
        "suspendCurrentQueueTimeMillis".into(),
        config.suspend_current_queue_time_millis().to_string(),
    );
    properties.insert(
        "consumeTimeout".into(),
        config.consume_timeout_minutes().to_string(),
    );
    properties.insert(
        "popInvisibleTime".into(),
        config.pop_invisible_time_millis().to_string(),
    );
    properties.insert("popBatchNums".into(), config.pop_batch_nums().to_string());
    properties.insert(
        "awaitTerminationMillisWhenShutdown".into(),
        config.await_termination_millis_when_shutdown().to_string(),
    );
    properties.insert(
        "clientRebalance".into(),
        config.client_rebalance().to_string(),
    );
    append_client_running_info_properties(config.client_config(), runtime, &mut properties);
    properties.insert("PROP_CONSUME_ORDERLY".into(), consume_orderly.to_string());
    properties.insert(
        "PROP_THREADPOOL_CORE_SIZE".into(),
        core_pool_size.to_string(),
    );
    properties.insert(
        "PROP_CONSUMER_START_TIMESTAMP".into(),
        start_timestamp.to_string(),
    );
    properties.insert("PROP_CONSUME_TYPE".into(), "CONSUME_PASSIVELY".into());
    properties.insert("PROP_CLIENT_VERSION".into(), "V5_5_1".into());
    properties
}

pub(crate) fn pull_running_info_properties(
    config: &crate::config::PullConsumerConfig,
    runtime: &ClientRuntime,
    start_timestamp: i64,
) -> BTreeMap<String, String> {
    let mut properties = pull_consumer_running_info_properties(config, start_timestamp);
    append_client_runtime_properties(runtime, &mut properties);
    properties.insert("PROP_CONSUME_TYPE".into(), "CONSUME_ACTIVELY".into());
    properties.insert("PROP_CLIENT_VERSION".into(), "V5_5_1".into());
    properties
}

pub(crate) fn pull_consumer_running_info_properties(
    config: &crate::config::PullConsumerConfig,
    start_timestamp: i64,
) -> BTreeMap<String, String> {
    let mut properties = BTreeMap::new();
    properties.insert("consumerGroup".into(), config.consumer_group().to_owned());
    properties.insert(
        "brokerSuspendMaxTimeMillis".into(),
        config.broker_suspend_max_time_millis().to_string(),
    );
    properties.insert(
        "consumerTimeoutMillisWhenSuspend".into(),
        config.consumer_timeout_millis_when_suspend().to_string(),
    );
    properties.insert(
        "consumerPullTimeoutMillis".into(),
        config.consumer_pull_timeout_millis().to_string(),
    );
    properties.insert("messageModel".into(), config.message_model().name().into());
    // MixAll.object2Properties walks DefaultMQPullConsumer before ClientConfig;
    // the inherited ClientConfig.unitMode field therefore wins this duplicate.
    properties.insert(
        "unitMode".into(),
        config.client_config().unit_mode.to_string(),
    );
    // Java 5.5.1's DefaultMQPullConsumer field is fixed at 16.
    properties.insert(
        "maxReconsumeTimes".into(),
        config.max_reconsume_times().to_string(),
    );
    properties.insert(
        "enableRebalance".into(),
        config.enable_rebalance().to_string(),
    );
    properties.extend(client_config_running_info_properties(
        config.client_config(),
    ));
    properties.insert(
        "PROP_CONSUMER_START_TIMESTAMP".into(),
        start_timestamp.to_string(),
    );
    properties
}

fn append_client_running_info_properties(
    config: &ClientConfig,
    runtime: &ClientRuntime,
    properties: &mut BTreeMap<String, String>,
) {
    properties.extend(client_config_running_info_properties(config));
    append_client_runtime_properties(runtime, properties);
}

fn client_config_running_info_properties(config: &ClientConfig) -> BTreeMap<String, String> {
    let mut properties = BTreeMap::new();
    insert_optional_property(
        &mut properties,
        "namesrvAddr",
        config.namesrv_addr.as_deref(),
    );
    insert_optional_property(&mut properties, "clientIP", config.client_ip.as_deref());
    insert_optional_property(
        &mut properties,
        "instanceName",
        config.instance_name.as_deref(),
    );
    properties.insert(
        "clientCallbackExecutorThreads".into(),
        config.client_callback_executor_threads.to_string(),
    );
    insert_optional_property(&mut properties, "namespace", config.namespace.as_deref());
    insert_optional_property(
        &mut properties,
        "namespaceV2",
        config.namespace_v2.as_deref(),
    );
    if let Some(access_channel) = config.access_channel {
        properties.insert("accessChannel".into(), access_channel.name().into());
    }
    properties.insert(
        "pollNameServerInterval".into(),
        config.poll_name_server_interval.to_string(),
    );
    properties.insert(
        "heartbeatBrokerInterval".into(),
        config.heartbeat_broker_interval.to_string(),
    );
    properties.insert(
        "persistConsumerOffsetInterval".into(),
        config.persist_consumer_offset_interval.to_string(),
    );
    properties.insert(
        "pullTimeDelayMillsWhenException".into(),
        config.pull_time_delay_millis_when_exception.to_string(),
    );
    properties.insert(
        "traceMsgBatchNum".into(),
        config.trace_msg_batch_num.to_string(),
    );
    properties.insert("unitMode".into(), config.unit_mode.to_string());
    insert_optional_property(&mut properties, "unitName", config.unit_name.as_deref());
    properties.insert("decodeReadBody".into(), config.decode_read_body.to_string());
    properties.insert(
        "decodeDecompressBody".into(),
        config.decode_decompress_body.to_string(),
    );
    properties.insert(
        "vipChannelEnabled".into(),
        config.vip_channel_enabled.to_string(),
    );
    properties.insert("useHeartbeatV2".into(), config.use_heartbeat_v2.to_string());
    properties.insert("useTLS".into(), config.use_tls.to_string());
    insert_optional_property(
        &mut properties,
        "socksProxyConfig",
        config.socks_proxy_config.as_deref(),
    );
    properties.insert(
        "mqClientApiTimeout".into(),
        config.mq_client_api_timeout.to_string(),
    );
    properties.insert("detectTimeout".into(), config.detect_timeout.to_string());
    properties.insert("detectInterval".into(), config.detect_interval.to_string());
    if let Some(language) = config.language {
        properties.insert("language".into(), language.name().into());
    }
    properties.insert(
        "enableStreamRequestType".into(),
        config.enable_stream_request_type.to_string(),
    );
    properties.insert(
        "sendLatencyEnable".into(),
        config.send_latency_enable.to_string(),
    );
    properties.insert(
        "startDetectorEnable".into(),
        config.start_detector_enable.to_string(),
    );
    properties.insert(
        "enableHeartbeatChannelEventListener".into(),
        config.enable_heartbeat_channel_event_listener.to_string(),
    );
    properties.insert(
        "enableConcurrentHeartbeat".into(),
        config.enable_concurrent_heartbeat.to_string(),
    );
    properties.insert(
        "concurrentHeartbeatThreadPoolSize".into(),
        config.concurrent_heartbeat_thread_pool_size.to_string(),
    );
    properties.insert("enableTrace".into(), config.enable_trace.to_string());
    insert_optional_property(&mut properties, "traceTopic", config.trace_topic.as_deref());
    properties.insert(
        "maxPageSizeInGetMetadata".into(),
        config.max_page_size_in_get_metadata.to_string(),
    );
    properties
}

fn append_client_runtime_properties(
    runtime: &ClientRuntime,
    properties: &mut BTreeMap<String, String>,
) {
    let nameserver_addresses = runtime
        .remoting()
        .get_name_server_address_list()
        .unwrap_or_default();
    let mut nameserver_address = String::new();
    for address in &nameserver_addresses {
        nameserver_address.push_str(address);
        nameserver_address.push(';');
    }
    properties.insert("PROP_NAMESERVER_ADDR".into(), nameserver_address);
}

fn insert_optional_property(
    properties: &mut BTreeMap<String, String>,
    key: &str,
    value: Option<&str>,
) {
    if let Some(value) = value {
        properties.insert(key.to_owned(), value.to_owned());
    }
}

impl RequestProcessor for DirectConsumeProcessor {
    fn process(&self, command: RemotingCommand) -> Result<Option<RemotingCommand>, String> {
        let fields = command
            .ext_fields
            .as_ref()
            .ok_or_else(|| "direct consume request has no ext fields".to_owned())?;
        let header = ConsumeMessageDirectlyResultRequestHeader::decode_fields(fields)
            .map_err(|error| error.to_string())?;
        let body = command
            .body
            .as_deref()
            .ok_or_else(|| "direct consume request has no message body".to_owned())?;
        let (message, _) =
            decode_one(body, DecodeOptions::default()).map_err(|error| error.to_string())?;
        let (Some(consumer), Some(runtime)) = (self.consumer.upgrade(), self.runtime.upgrade())
        else {
            return Ok(Some(group_not_found_error(&header.consumer_group)));
        };
        let broker_name = header.broker_name;
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        runtime
            .execute_task(async move {
                let _ = sender.send(
                    consumer
                        .consume_message_directly(message, broker_name)
                        .await,
                );
            })
            .map_err(|error| error.to_string())?;
        let Some(result) = receiver.recv().map_err(|error| error.to_string())? else {
            return Ok(Some(group_not_found_error(&header.consumer_group)));
        };
        let body = serde_json::to_vec(&result).map_err(|error| error.to_string())?;
        Ok(Some(RemotingCommand {
            code: response::SUCCESS,
            body: Some(body),
            ..Default::default()
        }))
    }
}

impl ResetOffsetProcessor {
    pub(crate) fn new(
        consumer: Weak<crate::push::PushConsumer>,
        runtime: Weak<ClientRuntime>,
    ) -> Self {
        Self { consumer, runtime }
    }
}

impl RequestProcessor for ResetOffsetProcessor {
    fn process(&self, command: RemotingCommand) -> Result<Option<RemotingCommand>, String> {
        let fields = command
            .ext_fields
            .as_ref()
            .ok_or_else(|| "reset offset request has no ext fields".to_owned())?;
        let header =
            ResetOffsetRequestHeader::decode_fields(fields).map_err(|error| error.to_string())?;
        let body = command
            .body
            .as_deref()
            .map(std::str::from_utf8)
            .transpose()
            .map_err(|error| error.to_string())?
            .map(ResetOffsetBody::decode_java_json)
            .transpose()
            .map_err(|error| error.to_string())?
            .unwrap_or_default();
        let offsets = body
            .offset_table
            .map(|table| table.snapshot())
            .unwrap_or_default();
        let (Some(consumer), Some(runtime)) = (self.consumer.upgrade(), self.runtime.upgrade())
        else {
            return Ok(None);
        };
        runtime
            .execute_task(async move {
                if let Err(error) = consumer
                    .reset_consumer_offsets(&header.topic, offsets)
                    .await
                {
                    tracing::warn!(group = %header.group, topic = %header.topic, error = %error, "consumer offset reset did not complete");
                }
            })
            .map_err(|error| error.to_string())?;
        // Java's resetOffset processor is a one-way callback and returns null.
        Ok(None)
    }
}

impl ConsumerStatusProcessor {
    pub(crate) fn new(offset_store: Weak<dyn OffsetStore>) -> Self {
        Self { offset_store }
    }
}

impl RequestProcessor for ConsumerStatusProcessor {
    fn process(&self, command: RemotingCommand) -> Result<Option<RemotingCommand>, String> {
        let fields = command
            .ext_fields
            .as_ref()
            .ok_or_else(|| "consumer status request has no ext fields".to_owned())?;
        let header = GetConsumerStatusRequestHeader::decode_fields(fields)
            .map_err(|error| error.to_string())?;
        let message_queue_table = self
            .offset_store
            .upgrade()
            .map(|offset_store| {
                offset_store
                    .clone_offset_table(Some(&header.topic))
                    .into_iter()
                    .map(|(queue, offset)| (queue, Some(offset)))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let body = GetConsumerStatusBody {
            message_queue_table: Some(SharedValue::new(message_queue_table)),
            consumer_table: Some(SharedValue::new(std::collections::BTreeMap::new())),
        }
        .encode_java_json()
        .map_err(|error| error.to_string())?;
        Ok(Some(RemotingCommand {
            code: response::SUCCESS,
            body: Some(body),
            ..Default::default()
        }))
    }
}

fn group_not_found_error(group: &str) -> RemotingCommand {
    RemotingCommand {
        code: response::SYSTEM_ERROR,
        remark: Some(format!(
            "The Consumer Group <{group}> not exist in this consumer"
        )),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offset::{OffsetRead, OffsetStoreFuture, ReadOffsetType};
    use rocketmq_classic_common::message::MessageQueue;
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    #[derive(Default)]
    struct MemoryOffsetStore(Mutex<HashMap<MessageQueue, i64>>);

    impl OffsetStore for MemoryOffsetStore {
        fn load(&self) -> OffsetStoreFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }

        fn update_offset(&self, queue: &MessageQueue, offset: i64, _increase_only: bool) {
            self.0.lock().unwrap().insert(queue.clone(), offset);
        }

        fn update_and_freeze_offset(&self, queue: &MessageQueue, offset: i64) {
            self.0.lock().unwrap().insert(queue.clone(), offset);
        }

        fn read_offset<'a>(
            &'a self,
            queue: &'a MessageQueue,
            _mode: ReadOffsetType,
        ) -> OffsetStoreFuture<'a, OffsetRead> {
            let offset = self.0.lock().unwrap().get(queue).copied();
            Box::pin(async move { Ok(offset.map_or(OffsetRead::NotFound, OffsetRead::Found)) })
        }

        fn persist<'a>(&'a self, _queue: &'a MessageQueue) -> OffsetStoreFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }

        fn persist_all<'a>(&'a self, _queues: &'a [MessageQueue]) -> OffsetStoreFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }

        fn remove_offset(&self, queue: &MessageQueue) {
            self.0.lock().unwrap().remove(queue);
        }

        fn clone_offset_table(&self, topic: Option<&str>) -> HashMap<MessageQueue, i64> {
            self.0
                .lock()
                .unwrap()
                .iter()
                .filter(|(queue, _)| topic.is_none_or(|topic| queue.topic() == Some(topic)))
                .map(|(queue, offset)| (queue.clone(), *offset))
                .collect()
        }

        fn update_consume_offset_to_broker<'a>(
            &'a self,
            _queue: &'a MessageQueue,
            _offset: i64,
            _oneway: bool,
        ) -> OffsetStoreFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    #[test]
    fn consumer_status_returns_only_the_requested_namespaced_topic_snapshot() {
        let store = Arc::new(MemoryOffsetStore::default());
        let selected = MessageQueue::new("tenant%orders", "broker-a", 1);
        let other = MessageQueue::new("tenant%other", "broker-a", 0);
        store.update_offset(&selected, 17, false);
        store.update_offset(&other, 99, false);
        let store: Arc<dyn OffsetStore> = store;
        let processor = ConsumerStatusProcessor::new(Arc::downgrade(&store));
        let mut fields = rocketmq_classic_protocol::header::HeaderMap::new();
        GetConsumerStatusRequestHeader {
            topic: "tenant%orders".into(),
            group: "tenant%workers".into(),
            client_addr: None,
        }
        .encode_fields(&mut fields);

        let response = processor
            .process(RemotingCommand {
                code: request::GET_CONSUMER_STATUS_FROM_CLIENT,
                ext_fields: Some(fields),
                ..Default::default()
            })
            .unwrap()
            .unwrap();

        assert_eq!(response.code, response::SUCCESS);
        let body = String::from_utf8(response.body.unwrap()).unwrap();
        let body = GetConsumerStatusBody::decode_java_json(&body).unwrap();
        assert_eq!(
            body.message_queue_table.unwrap().snapshot(),
            vec![(selected, Some(17))]
        );
    }

    #[test]
    fn consumer_status_rejects_missing_or_malformed_request_headers() {
        let store: Arc<dyn OffsetStore> = Arc::new(MemoryOffsetStore::default());
        let offset_store = Arc::downgrade(&store);
        drop(store);
        let processor = ConsumerStatusProcessor::new(offset_store);
        for command in [
            RemotingCommand::default(),
            RemotingCommand {
                ext_fields: Some(Default::default()),
                ..Default::default()
            },
        ] {
            assert!(processor.process(command).is_err());
        }
    }
}
