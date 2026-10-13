use std::{collections::BTreeSet, time::Duration};

use rocketmq_classic_common::{message::MessageQueue, topic::TopicValidator};
use rocketmq_classic_core::{
    api::admin::AdminQueryError,
    error::RpcError,
    route::broker_vip_channel,
    runtime::{ClientRuntime, RuntimeError},
};
use rocketmq_classic_protocol::{
    codes::{request, response},
    header::CreateTopicRequestHeader,
};
use tokio::time::Instant;

const ADMIN_TIMEOUT: Duration = Duration::from_millis(6_000);

pub(crate) fn validate_topic(topic: &str) -> Result<(), String> {
    let validation = TopicValidator::validate_topic(Some(topic));
    if !validation.is_valid() {
        return Err(validation.remark().to_owned());
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
