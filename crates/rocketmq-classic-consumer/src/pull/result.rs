use std::{
    collections::HashMap,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex, RwLock},
};

use rand::Rng;
use rocketmq_classic_common::{
    message::{
        DecodeError, DecodeLimits, DecodeOptions, Message, MessageExt, MessageQueue, decode_many,
        decode_properties,
    },
    sysflag::message::{INNER_BATCH_FLAG, NEED_UNWRAP_FLAG},
};
use rocketmq_classic_core::api::consumer::{PullResultExt, PullStatus};
use rocketmq_classic_protocol::{heartbeat::SubscriptionData, route::TopicRouteData};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProcessPullError {
    #[error("FOUND pull result has no message binary")]
    MissingMessageBinary,
    #[error(transparent)]
    MessageDecode(#[from] DecodeError),
    #[error("inner batch is malformed at byte {offset}: {reason}")]
    MalformedInnerBatch { offset: usize, reason: &'static str },
    #[error("inner batch decoded size exceeds configured limit {limit}")]
    InnerBatchLimitExceeded { limit: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessedPullResult {
    pub result: PullResultExt,
    /// Java keeps this unset for every status except FOUND.
    pub messages: Option<Vec<MessageExt>>,
}

pub struct FilterMessageContext {
    pub unit_mode: bool,
    pub messages: Vec<MessageExt>,
}

pub trait FilterMessageHook: Send + Sync {
    fn hook_name(&self) -> &str;
    fn filter_message(&self, context: &mut FilterMessageContext);
}

/// Pull-result processing state shared by pull consumers.
#[derive(Default)]
pub struct PullAPIWrapper {
    suggested_broker_ids: Mutex<HashMap<MessageQueue, i64>>,
    hooks: RwLock<Vec<Arc<dyn FilterMessageHook>>>,
    unit_mode: bool,
}

impl PullAPIWrapper {
    pub fn new(unit_mode: bool) -> Self {
        Self {
            suggested_broker_ids: Mutex::new(HashMap::new()),
            hooks: RwLock::new(Vec::new()),
            unit_mode,
        }
    }

    pub fn register_filter_message_hook(&self, hook: Arc<dyn FilterMessageHook>) {
        self.hooks.write().unwrap().push(hook);
    }

    pub fn process_pull_result(
        &self,
        queue: &MessageQueue,
        mut result: PullResultExt,
        subscription: &SubscriptionData,
        options: DecodeOptions,
        limits: DecodeLimits,
    ) -> Result<ProcessedPullResult, ProcessPullError> {
        self.suggested_broker_ids
            .lock()
            .unwrap()
            .insert(queue.clone(), result.suggest_which_broker_id);

        let messages = if result.status == PullStatus::Found {
            let raw = result
                .raw_messages
                .take()
                .ok_or(ProcessPullError::MissingMessageBinary)?;
            let mut messages = decode_many(&raw, options, limits)?;
            if messages.iter().any(|message| {
                message.sys_flag() & INNER_BATCH_FLAG != 0
                    && message.sys_flag() & NEED_UNWRAP_FLAG != 0
            }) {
                if let Ok(unwrapped) = unwrap_messages(&messages, options, limits) {
                    messages = unwrapped;
                }
            }

            attach_client_message_ids(&mut messages);

            filter_tags(&mut messages, subscription);
            let mut context = FilterMessageContext {
                unit_mode: self.unit_mode,
                messages,
            };
            for hook in self.hooks.read().unwrap().iter() {
                let _ = catch_unwind(AssertUnwindSafe(|| hook.filter_message(&mut context)));
            }
            enrich_messages(&mut context.messages, queue, &result);
            Some(context.messages)
        } else {
            None
        };

        // Match PullAPIWrapper: the raw broker buffer is released for every status.
        result.raw_messages = None;
        Ok(ProcessedPullResult { result, messages })
    }
}

pub fn build_class_filter_subscription_data(
    topic: impl Into<String>,
    class_name: impl Into<String>,
    filter_class_source: impl Into<String>,
) -> SubscriptionData {
    let mut subscription = SubscriptionData::new(Some(topic.into()), Some("*".to_owned()));
    subscription.set_sub_string(Some(class_name.into()));
    subscription.set_class_filter_mode(true);
    subscription.set_filter_class_source(Some(filter_class_source.into()));
    subscription
}

#[derive(Debug, Error, PartialEq, Eq)]
#[allow(dead_code)] // Used by the pull request path added in the following consumer phase.
pub(crate) enum FilterServerError {
    #[error("Find Filter Server Failed, Broker Addr: {broker_address} topic: {topic}")]
    NotFound {
        broker_address: String,
        topic: String,
    },
}

/// Choose a filter server using the broker address keys carried by route data.
#[allow(dead_code)] // Kept crate-internal for the pull request path added in the following consumer phase.
pub(crate) fn select_filter_server(
    route: &TopicRouteData,
    topic: &str,
    broker_address: &str,
) -> Result<Option<String>, FilterServerError> {
    let not_found = || FilterServerError::NotFound {
        broker_address: broker_address.to_owned(),
        topic: topic.to_owned(),
    };
    let table = route.filter_server_table_snapshot().ok_or_else(not_found)?;
    let list = table
        .get(broker_address)
        .and_then(Option::as_ref)
        .ok_or_else(not_found)?;
    if list.is_empty() {
        return Err(not_found());
    }
    let index = rand::rng().random_range(0..list.len());
    Ok(list[index].clone())
}

/// Apply class-filter routing only when the Java pull class-filter bit is set.
#[allow(dead_code)] // Kept crate-internal for the pull request path added in the following consumer phase.
pub(crate) fn resolve_pull_broker_address(
    route: &TopicRouteData,
    topic: &str,
    broker_address: &str,
    pull_sys_flag: i32,
) -> Result<Option<String>, FilterServerError> {
    if rocketmq_classic_common::sysflag::pull::has_class_filter_flag(pull_sys_flag) {
        select_filter_server(route, topic, broker_address)
    } else {
        Ok(Some(broker_address.to_owned()))
    }
}

fn filter_tags(messages: &mut Vec<MessageExt>, subscription: &SubscriptionData) {
    if subscription.is_class_filter_mode() {
        return;
    }
    let tags = subscription
        .get_tags_set()
        .map(|set| set.snapshot())
        .unwrap_or_default();
    if tags.is_empty() {
        return;
    }
    messages.retain(|message| {
        message
            .message()
            .tags()
            .is_some_and(|tag| tags.contains(&Some(tag.to_owned())))
    });
}

fn attach_client_message_ids(messages: &mut [MessageExt]) {
    for message in messages {
        let offset_message_id = message.message_id().map(str::to_owned);
        let message_id = message
            .message()
            .properties()
            .get(
                rocketmq_classic_common::message::constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            )
            .cloned()
            .or_else(|| offset_message_id.clone());
        message.set_offset_message_id(offset_message_id);
        message.set_message_id(message_id);
    }
}

fn enrich_messages(messages: &mut [MessageExt], queue: &MessageQueue, result: &PullResultExt) {
    for message in messages {
        let is_prepared = message
            .message()
            .properties()
            .get(rocketmq_classic_common::message::constants::PROPERTY_TRANSACTION_PREPARED)
            .is_some_and(|value| value.eq_ignore_ascii_case("true"));
        if is_prepared {
            let transaction_id = message
                .message()
                .properties()
                .get(rocketmq_classic_common::message::constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                .cloned();
            message.message_mut().set_transaction_id(transaction_id);
        }
        let _ = message.message_mut().set_system_property(
            rocketmq_classic_common::message::constants::PROPERTY_MIN_OFFSET,
            result.min_offset.to_string(),
        );
        let _ = message.message_mut().set_system_property(
            rocketmq_classic_common::message::constants::PROPERTY_MAX_OFFSET,
            result.max_offset.to_string(),
        );
        message.set_broker_name(queue.broker_name().map(str::to_owned));
        message.set_queue_id(queue.queue_id());
        if let Some(delta) = result.offset_delta {
            message.set_queue_offset(message.queue_offset().wrapping_add(delta));
        }
    }
}

fn unwrap_messages(
    messages: &[MessageExt],
    options: DecodeOptions,
    limits: DecodeLimits,
) -> Result<Vec<MessageExt>, ProcessPullError> {
    if !options.read_body {
        return Err(ProcessPullError::MalformedInnerBatch {
            offset: 0,
            reason: "message bodies were not read",
        });
    }
    let mut unwrapped = Vec::new();
    for outer in messages {
        if outer.sys_flag() & INNER_BATCH_FLAG != 0 && outer.sys_flag() & NEED_UNWRAP_FLAG != 0 {
            unwrapped.extend(decode_inner_batch(outer, limits)?);
        } else {
            unwrapped.push(outer.clone());
        }
    }
    Ok(unwrapped)
}

fn decode_inner_batch(
    outer: &MessageExt,
    limits: DecodeLimits,
) -> Result<Vec<MessageExt>, ProcessPullError> {
    let bytes = outer.message().body();
    let mut offset = 0usize;
    let mut total_decoded = 0usize;
    let mut messages = Vec::new();
    while offset < bytes.len() {
        let size = read_i32(bytes, offset).ok_or(ProcessPullError::MalformedInnerBatch {
            offset,
            reason: "missing record size",
        })?;
        let size = usize::try_from(size).map_err(|_| ProcessPullError::MalformedInnerBatch {
            offset,
            reason: "negative record size",
        })?;
        if size > limits.max_record_bytes {
            return Err(ProcessPullError::InnerBatchLimitExceeded {
                limit: limits.max_record_bytes,
            });
        }
        if size < 22 || offset.checked_add(size).is_none_or(|end| end > bytes.len()) {
            return Err(ProcessPullError::MalformedInnerBatch {
                offset,
                reason: "record size is truncated or smaller than its header",
            });
        }
        let record = &bytes[offset..offset + size];
        let flag = read_i32(record, 12).ok_or(ProcessPullError::MalformedInnerBatch {
            offset,
            reason: "missing flag",
        })?;
        let body_size = usize::try_from(read_i32(record, 16).ok_or(
            ProcessPullError::MalformedInnerBatch {
                offset,
                reason: "missing body size",
            },
        )?)
        .map_err(|_| ProcessPullError::MalformedInnerBatch {
            offset,
            reason: "negative body size",
        })?;
        let body_start = 20usize;
        let body_end = body_start
            .checked_add(body_size)
            .filter(|end| *end <= record.len())
            .ok_or(ProcessPullError::MalformedInnerBatch {
                offset,
                reason: "body exceeds its record",
            })?;
        let properties_size =
            read_i16(record, body_end).ok_or(ProcessPullError::MalformedInnerBatch {
                offset,
                reason: "missing properties size",
            })?;
        let properties_size = usize::try_from(properties_size).map_err(|_| {
            ProcessPullError::MalformedInnerBatch {
                offset,
                reason: "negative properties size",
            }
        })?;
        let properties_start = body_end + 2;
        let properties_end = properties_start
            .checked_add(properties_size)
            .filter(|end| *end == record.len())
            .ok_or(ProcessPullError::MalformedInnerBatch {
                offset,
                reason: "properties length does not match record size",
            })?;
        total_decoded = total_decoded
            .checked_add(body_size)
            .and_then(|size| size.checked_add(properties_size))
            .ok_or(ProcessPullError::InnerBatchLimitExceeded {
                limit: limits.max_total_decoded_bytes,
            })?;
        if total_decoded > limits.max_total_decoded_bytes {
            return Err(ProcessPullError::InnerBatchLimitExceeded {
                limit: limits.max_total_decoded_bytes,
            });
        }
        let properties = String::from_utf8_lossy(&record[properties_start..properties_end]);
        let mut message = Message::default();
        message.set_topic(outer.message().topic().to_owned());
        message.set_body(record[body_start..body_end].to_vec());
        message.set_flag(flag);
        for (key, value) in decode_properties(&properties) {
            let _ = message.set_system_property(key, value);
        }
        let mut inner = MessageExt::new(message);
        inner.set_queue_id(outer.queue_id());
        inner.set_queue_offset(outer.queue_offset().wrapping_add(messages.len() as i64));
        inner.set_sys_flag(outer.sys_flag());
        inner.set_born_timestamp(outer.born_timestamp());
        inner.set_born_host(outer.born_host().map(str::to_owned));
        inner.set_store_timestamp(outer.store_timestamp());
        inner.set_store_host(outer.store_host().map(str::to_owned));
        inner.set_commit_log_offset(outer.commit_log_offset());
        messages.push(inner);
        offset += size;
    }
    Ok(messages)
}

fn read_i16(bytes: &[u8], offset: usize) -> Option<i16> {
    Some(i16::from_be_bytes(
        bytes.get(offset..offset.checked_add(2)?)?.try_into().ok()?,
    ))
}

fn read_i32(bytes: &[u8], offset: usize) -> Option<i32> {
    Some(i32::from_be_bytes(
        bytes.get(offset..offset.checked_add(4)?)?.try_into().ok()?,
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn empty_pull_result(status: PullStatus) -> PullResultExt {
        PullResultExt {
            status,
            next_begin_offset: 100,
            min_offset: 20,
            max_offset: 160,
            suggest_which_broker_id: 7,
            offset_delta: None,
            raw_messages: None,
        }
    }

    #[test]
    fn pull_processing_retains_suggested_broker_for_non_found_statuses() {
        let queue = MessageQueue::new("orders", "broker-a", 2);
        let wrapper = PullAPIWrapper::default();
        let processed = wrapper
            .process_pull_result(
                &queue,
                empty_pull_result(PullStatus::NoNewMessage),
                &SubscriptionData::default(),
                DecodeOptions::default(),
                DecodeLimits::default(),
            )
            .unwrap();
        assert_eq!(processed.result.next_begin_offset, 100);
        assert_eq!(
            wrapper.suggested_broker_ids.lock().unwrap().get(&queue),
            Some(&7)
        );
    }

    #[test]
    fn found_decode_error_still_retains_the_suggested_broker() {
        let queue = MessageQueue::new("orders", "broker-a", 2);
        let wrapper = PullAPIWrapper::default();
        assert!(matches!(
            wrapper.process_pull_result(
                &queue,
                empty_pull_result(PullStatus::Found),
                &SubscriptionData::default(),
                DecodeOptions::default(),
                DecodeLimits::default(),
            ),
            Err(ProcessPullError::MissingMessageBinary)
        ));
        assert_eq!(
            wrapper.suggested_broker_ids.lock().unwrap().get(&queue),
            Some(&7)
        );
    }

    #[test]
    fn class_filter_routing_uses_the_broker_address_route_entry() {
        let mut route = TopicRouteData::default();
        route.set_filter_server_table_owned(Some(BTreeMap::from([(
            "192.0.2.9:10911".to_owned(),
            Some(vec![
                Some("192.0.2.20:10911".to_owned()),
                Some("192.0.2.21:10911".to_owned()),
            ]),
        )])));
        let selected = select_filter_server(&route, "orders", "192.0.2.9:10911").unwrap();
        assert!(matches!(
            selected.as_deref(),
            Some("192.0.2.20:10911" | "192.0.2.21:10911")
        ));
        assert!(matches!(
            select_filter_server(&route, "orders", "missing"),
            Err(FilterServerError::NotFound { .. })
        ));
        assert_eq!(
            resolve_pull_broker_address(&route, "orders", "192.0.2.9:10911", 0).unwrap(),
            Some("192.0.2.9:10911".to_owned())
        );
        let filter_flag = rocketmq_classic_common::sysflag::pull::FLAG_CLASS_FILTER;
        let routed = resolve_pull_broker_address(&route, "orders", "192.0.2.9:10911", filter_flag)
            .unwrap()
            .unwrap();
        assert!(matches!(
            routed.as_str(),
            "192.0.2.20:10911" | "192.0.2.21:10911"
        ));
        route.set_filter_server_table_owned(Some(BTreeMap::from([(
            "empty".to_owned(),
            Some(Vec::new()),
        )])));
        assert!(matches!(
            select_filter_server(&route, "orders", "empty"),
            Err(FilterServerError::NotFound { .. })
        ));
    }
}
