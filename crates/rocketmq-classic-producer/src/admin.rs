use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use rocketmq_classic_common::message::{MessageExt, MessageQueue, decode_message_id};
use rocketmq_classic_core::{
    api::admin::{AdminQueryRequest, QueryResult},
    route::broker_vip_channel,
};
use rocketmq_classic_protocol::header::{
    BoundaryType, CreateTopicRequestHeader, GetEarliestMsgStoretimeRequestHeader,
    GetMaxOffsetRequestHeader, GetMinOffsetRequestHeader, SearchOffsetRequestHeader,
    ViewMessageRequestHeader,
};
use tokio::time::Instant;

use crate::{Producer, ProducerError, validation};

const ADMIN_TIMEOUT_MILLIS: u64 = 6_000;
const UNIQUE_KEY_LOOKBACK_MILLIS: i64 = 3 * 24 * 60 * 60 * 1_000;

impl Producer {
    pub async fn query_message(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> Result<QueryResult, ProducerError> {
        self.query_messages(topic, key, max_num, begin_timestamp, end_timestamp, false)
            .await
    }

    /// Views a message by its physical ID, falling back to Java's unique-key
    /// query when parsing or the physical lookup fails.
    pub async fn view_message(
        &self,
        topic: &str,
        message_id: &str,
    ) -> Result<Option<MessageExt>, ProducerError> {
        let runtime = self.started_runtime().await?;
        let decoded = decode_message_id(message_id).ok().and_then(|decoded| {
            decoded
                .address()?
                .socket_addr()
                .map(|address| (address, decoded.offset()))
        });
        if let Some((address, offset)) = decoded {
            let header = ViewMessageRequestHeader {
                topic: Some(topic.to_owned()),
                offset,
            };
            let timeout = std::time::Duration::from_millis(
                i64::from(self.config.client_config().mq_client_api_timeout()).max(0) as u64,
            );
            if let Ok(message) = runtime
                .api()
                .view_message(&address.to_string(), &header, Instant::now() + timeout)
                .await
            {
                return Ok(Some(message));
            }
        }

        let begin_timestamp = current_time_millis().saturating_sub(UNIQUE_KEY_LOOKBACK_MILLIS);
        let result = self
            .query_messages(topic, message_id, 32, begin_timestamp, i64::MAX, true)
            .await?;
        Ok(first_query_message(result))
    }

    #[allow(clippy::too_many_arguments)]
    async fn query_messages(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
        unique_key: bool,
    ) -> Result<QueryResult, ProducerError> {
        let runtime = self.started_runtime().await?;
        let mut client_config = self.config.client_config().clone();
        let topic = client_config
            .with_namespace(Some(topic))?
            .ok_or_else(|| ProducerError::NoRoute(topic.to_owned()))?;
        let query = if unique_key {
            AdminQueryRequest::by_unique_key(&topic, key, max_num, begin_timestamp, end_timestamp)
        } else {
            AdminQueryRequest::by_key(&topic, key, max_num, begin_timestamp, end_timestamp)
        };
        let timeout_millis =
            i64::from(self.config.client_config().mq_client_api_timeout()).max(0) as u64;
        let deadline =
            Instant::now() + std::time::Duration::from_millis(timeout_millis).saturating_mul(4);
        let result = runtime.query_message(&query, deadline).await?;
        if let Some(messages) = result.get_message_list() {
            for message in messages.snapshot().into_iter().flatten() {
                message.with_mut(|message| {
                    if let Some(topic) = client_config.without_namespace(Some(message.topic()))? {
                        message.set_topic(topic);
                    }
                    Ok::<(), ProducerError>(())
                })?;
            }
        }
        Ok(result)
    }

    /// Searches a queue by Java's default lower timestamp boundary.
    pub async fn search_offset(
        &self,
        input_queue: &MessageQueue,
        timestamp_millis: i64,
    ) -> Result<i64, ProducerError> {
        let (runtime, queue, broker_name, address, deadline) =
            self.prepare_admin_queue(input_queue).await?;
        let header = SearchOffsetRequestHeader {
            topic: queue.topic().ok_or(ProducerError::InvalidQueue)?.to_owned(),
            queue_id: queue.queue_id(),
            timestamp: timestamp_millis,
            boundary_type: Some(BoundaryType::Lower),
            broker_name: Some(broker_name),
            ..SearchOffsetRequestHeader::default()
        };
        Ok(runtime
            .api()
            .search_offset(&address, &header, deadline)
            .await?)
    }

    pub async fn get_max_offset(&self, input_queue: &MessageQueue) -> Result<i64, ProducerError> {
        let (runtime, queue, broker_name, address, deadline) =
            self.prepare_admin_queue(input_queue).await?;
        let header = GetMaxOffsetRequestHeader {
            topic: queue.topic().ok_or(ProducerError::InvalidQueue)?.to_owned(),
            queue_id: queue.queue_id(),
            committed: true,
            broker_name: Some(broker_name),
            ..GetMaxOffsetRequestHeader::default()
        };
        Ok(runtime
            .api()
            .get_max_offset(&address, &header, deadline)
            .await?)
    }

    pub async fn get_min_offset(&self, input_queue: &MessageQueue) -> Result<i64, ProducerError> {
        let (runtime, queue, broker_name, address, deadline) =
            self.prepare_admin_queue(input_queue).await?;
        let header = GetMinOffsetRequestHeader {
            topic: queue.topic().ok_or(ProducerError::InvalidQueue)?.to_owned(),
            queue_id: queue.queue_id(),
            broker_name: Some(broker_name),
            ..GetMinOffsetRequestHeader::default()
        };
        Ok(runtime
            .api()
            .get_min_offset(&address, &header, deadline)
            .await?)
    }

    pub async fn get_earliest_msg_store_time(
        &self,
        input_queue: &MessageQueue,
    ) -> Result<i64, ProducerError> {
        let (runtime, queue, broker_name, address, deadline) =
            self.prepare_admin_queue(input_queue).await?;
        let header = GetEarliestMsgStoretimeRequestHeader {
            topic: queue.topic().ok_or(ProducerError::InvalidQueue)?.to_owned(),
            queue_id: queue.queue_id(),
            broker_name: Some(broker_name),
            ..GetEarliestMsgStoretimeRequestHeader::default()
        };
        Ok(runtime
            .api()
            .get_earliest_msg_storetime(&address, &header, deadline)
            .await?)
    }

    async fn prepare_admin_queue(
        &self,
        input_queue: &MessageQueue,
    ) -> Result<
        (
            std::sync::Arc<rocketmq_classic_core::runtime::ClientRuntime>,
            MessageQueue,
            String,
            String,
            Instant,
        ),
        ProducerError,
    > {
        let runtime = self.started_runtime().await?;
        let queue = self
            .config
            .client_config()
            .clone()
            .queue_with_namespace(input_queue)?;
        let topic = queue
            .topic()
            .filter(|topic| !topic.is_empty())
            .ok_or(ProducerError::InvalidQueue)?;
        let broker_name = queue
            .broker_name()
            .filter(|broker| !broker.is_empty())
            .ok_or(ProducerError::InvalidQueue)?
            .to_owned();
        let timeout_millis =
            i64::from(self.config.client_config().mq_client_api_timeout()).max(0) as u64;
        let deadline = Instant::now() + std::time::Duration::from_millis(timeout_millis);
        let mut route = runtime.route_snapshot()?;
        let mut address = route.publish_address(&broker_name).map(str::to_owned);
        if address.is_none() {
            runtime.update_topic_route(topic, deadline).await?;
            route = runtime.route_snapshot()?;
            address = route.publish_address(&broker_name).map(str::to_owned);
        }
        let address = address.ok_or_else(|| ProducerError::BrokerNotFound {
            broker: broker_name.clone(),
        })?;
        Ok((runtime, queue, broker_name, address, deadline))
    }

    /// Creates a topic on each master Broker advertising the supplied key.
    /// A successful creation on any Broker makes the operation successful,
    /// matching Java's partial-success rule. Java 5.5.1 accepts but discards
    /// the producer facade's `attributes` argument.
    pub async fn create_topic(
        &self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        _attributes: Option<&BTreeMap<String, String>>,
    ) -> Result<(), ProducerError> {
        self.create_topic_with_sys_flag(key, new_topic, queue_num, 0, None)
            .await
    }

    pub async fn create_topic_with_sys_flag(
        &self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        topic_sys_flag: i32,
        _attributes: Option<&BTreeMap<String, String>>,
    ) -> Result<(), ProducerError> {
        let runtime = self.started_runtime().await?;
        let mut client_config = self.config.client_config().clone();
        let route_topic = key.to_owned();
        let topic = client_config
            .with_namespace(Some(new_topic))?
            .ok_or_else(|| ProducerError::NoRoute(new_topic.to_owned()))?;
        validation::validate_topic(&topic)?;
        validation::validate_system_topic(&topic)?;

        let timeout = std::time::Duration::from_millis(ADMIN_TIMEOUT_MILLIS);
        let route = runtime
            .api()
            .get_topic_route(&route_topic, Instant::now() + timeout)
            .await?
            .ok_or_else(|| ProducerError::NoRoute(route_topic.clone()))?;
        let broker_rows = route
            .broker_datas_snapshot()
            .ok_or_else(|| ProducerError::NoRoute(route_topic.clone()))?;
        let mut brokers = Vec::with_capacity(broker_rows.len());
        for broker in broker_rows {
            brokers.push(broker.ok_or(ProducerError::Runtime(
                rocketmq_classic_core::runtime::RuntimeError::InvalidRoute(
                    "null broker data during admin topic creation",
                ),
            ))?);
        }
        if brokers.is_empty() {
            return Err(ProducerError::NoRoute(route_topic));
        }
        if brokers.iter().any(|broker| broker.broker_name.is_none()) {
            return Err(ProducerError::Runtime(
                rocketmq_classic_core::runtime::RuntimeError::InvalidRoute(
                    "null broker name during admin topic creation",
                ),
            ));
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
            topic,
            default_topic: route_topic,
            read_queue_nums: queue_num,
            write_queue_nums: queue_num,
            perm: 6,
            topic_filter_type: "SINGLE_TAG".into(),
            topic_sys_flag: Some(topic_sys_flag),
            order: Some(false),
            // DefaultMQProducer's public overloads accept the Java Map
            // argument but pass null to MQAdminImpl in 5.5.1.
            attributes: Some(String::new()),
            force: Some(false),
            ..Default::default()
        };
        let vip_enabled = runtime
            .config()
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .vip_channel_enabled();
        let mut created_at_least_once = false;
        let mut last_error = None;
        for broker in brokers {
            let addresses = broker.broker_addrs.as_ref().ok_or(ProducerError::Runtime(
                rocketmq_classic_core::runtime::RuntimeError::InvalidRoute(
                    "null broker address map during admin topic creation",
                ),
            ))?;
            let master = addresses.with(|addresses| addresses.get(&Some(0)).and_then(Clone::clone));
            let Some(master) = master else {
                continue;
            };
            let address = broker_vip_channel(vip_enabled, &master)?;
            for _ in 0..5 {
                match runtime
                    .api()
                    .create_topic(&address, &header, Instant::now() + timeout)
                    .await
                {
                    Ok(()) => {
                        created_at_least_once = true;
                        break;
                    }
                    Err(error) => last_error = Some(error),
                }
            }
        }
        if !created_at_least_once {
            if let Some(error) = last_error {
                return Err(ProducerError::Rpc(error));
            }
        }
        Ok(())
    }
}

fn current_time_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn first_query_message(result: QueryResult) -> Option<MessageExt> {
    let messages = result.get_message_list()?;
    let message = messages.snapshot().into_iter().next()?;
    message.map(|message| message.snapshot())
}
