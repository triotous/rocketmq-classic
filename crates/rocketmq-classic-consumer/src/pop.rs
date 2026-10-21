//! POP message receipt data used by the push consumer's ACK lifecycle.
//!
//! Batch-ACK request assembly is an internal implementation detail.
//!
//! ```compile_fail
//! use rocketmq_classic_consumer::pop::build_batch_ack_body;
//! ```

#[cfg(test)]
use std::collections::BTreeMap;
use std::collections::HashMap;

use rocketmq_classic_common::message::{DecodeError, DecodeOptions, MessageExt, decode_many};
use rocketmq_classic_core::api::extensions::PopReply;
use rocketmq_classic_protocol::header::{
    AckMessageRequestHeader, ChangeInvisibleTimeRequestHeader,
};
use rocketmq_classic_protocol::header::{PopMessageRequestHeader, PopMessageResponseHeader};
#[cfg(test)]
use rocketmq_classic_protocol::{
    SharedValue,
    body::{BatchAck, BatchAckMessageRequestBody, BitSet},
};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

pub use rocketmq_classic_core::api::extensions::PopStatus;

const PROPERTY_POP_CK: &str = rocketmq_classic_common::message::constants::PROPERTY_POP_CK;
const PROPERTY_FIRST_POP_TIME: &str =
    rocketmq_classic_common::message::constants::PROPERTY_FIRST_POP_TIME;

/// Assignment-local state for one Push POP queue.
pub(crate) struct PopProcessQueue {
    queue: rocketmq_classic_common::message::MessageQueue,
    generation: u64,
    dropped: AtomicBool,
    wait_ack_count: AtomicI64,
    last_pop_timestamp: AtomicI64,
}

impl PopProcessQueue {
    pub(crate) fn new(
        queue: rocketmq_classic_common::message::MessageQueue,
        generation: u64,
    ) -> Self {
        Self {
            queue,
            generation,
            dropped: AtomicBool::new(false),
            wait_ack_count: AtomicI64::new(0),
            last_pop_timestamp: AtomicI64::new(epoch_millis()),
        }
    }

    pub(crate) fn queue(&self) -> &rocketmq_classic_common::message::MessageQueue {
        &self.queue
    }

    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn is_dropped(&self) -> bool {
        self.dropped.load(Ordering::Acquire)
    }

    pub(crate) fn drop_assignment(&self) {
        self.dropped.store(true, Ordering::Release);
    }

    pub(crate) fn set_last_pop_timestamp(&self, timestamp: i64) {
        self.last_pop_timestamp.store(timestamp, Ordering::Release);
    }

    pub(crate) fn inc_found_messages(&self, count: usize) {
        self.wait_ack_count
            .fetch_add(i64::try_from(count).unwrap_or(i64::MAX), Ordering::AcqRel);
    }

    pub(crate) fn ack(&self) -> i64 {
        self.wait_ack_count.fetch_sub(1, Ordering::AcqRel)
    }

    pub(crate) fn dec_found_messages(&self, count: usize) {
        self.wait_ack_count
            .fetch_sub(i64::try_from(count).unwrap_or(i64::MAX), Ordering::AcqRel);
    }

    pub(crate) fn wait_ack_count(&self) -> i64 {
        self.wait_ack_count.load(Ordering::Acquire)
    }

    pub(crate) fn last_pop_timestamp(&self) -> i64 {
        self.last_pop_timestamp.load(Ordering::Acquire)
    }

    pub(crate) fn is_pull_expired(&self, max_idle_time_millis: i64) -> bool {
        crate::process_queue::pull_idle_time_expired(
            epoch_millis(),
            self.last_pop_timestamp.load(Ordering::Acquire),
            max_idle_time_millis,
        )
    }
}

fn epoch_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().min(i64::MAX as u128) as i64
        })
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PopProcessError {
    #[error(transparent)]
    Decode(#[from] DecodeError),
    #[error("FOUND POP response is missing its response header or message body")]
    MissingResponseData,
    #[error("POP response metadata is malformed: {0}")]
    MalformedMetadata(String),
}

/// Java-compatible result passed from POP request execution to its consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PopResult {
    messages: Option<Vec<rocketmq_classic_common::message::MessageExt>>,
    status: PopStatus,
    pop_time: i64,
    invisible_time: i64,
    rest_num: i64,
}

impl PopResult {
    pub fn new(
        status: PopStatus,
        messages: Option<Vec<rocketmq_classic_common::message::MessageExt>>,
    ) -> Self {
        Self {
            messages,
            status,
            pop_time: 0,
            invisible_time: 0,
            rest_num: 0,
        }
    }

    pub fn messages(&self) -> Option<&[rocketmq_classic_common::message::MessageExt]> {
        self.messages.as_deref()
    }

    pub fn set_messages(
        &mut self,
        messages: Option<Vec<rocketmq_classic_common::message::MessageExt>>,
    ) {
        self.messages = messages;
    }

    pub const fn status(&self) -> &PopStatus {
        &self.status
    }

    pub fn set_status(&mut self, status: PopStatus) {
        self.status = status;
    }

    pub const fn pop_time(&self) -> i64 {
        self.pop_time
    }

    pub fn set_pop_time(&mut self, pop_time: i64) {
        self.pop_time = pop_time;
    }

    pub const fn invisible_time(&self) -> i64 {
        self.invisible_time
    }

    pub fn set_invisible_time(&mut self, invisible_time: i64) {
        self.invisible_time = invisible_time;
    }

    pub const fn rest_num(&self) -> i64 {
        self.rest_num
    }

    pub fn set_rest_num(&mut self, rest_num: i64) {
        self.rest_num = rest_num;
    }
}

/// Applies the pinned Java POP response decoration rules. Receipt checkpoints
/// are mapped by retry category and queue ID, so one response may safely
/// contain messages from multiple queues.
pub(crate) fn process_pop_reply(
    mut reply: PopReply<PopMessageResponseHeader>,
    request: &PopMessageRequestHeader,
    broker_name: &str,
    options: DecodeOptions,
) -> Result<PopResult, PopProcessError> {
    let status = reply.status.clone();
    let mut result = PopResult::new(status.clone(), None);
    let header = reply.header.ok_or(PopProcessError::MissingResponseData)?;
    result.set_rest_num(header.get_rest_num());
    if status != PopStatus::Found {
        return Ok(result);
    }

    result.set_pop_time(header.get_pop_time());
    result.set_invisible_time(header.get_invisible_time());
    let raw = reply
        .raw_messages
        .take()
        .ok_or(PopProcessError::MissingResponseData)?;
    let mut messages = decode_many(&raw, options)?;
    add_receipts(&mut messages, &header, request, broker_name)?;
    result.set_messages(Some(messages));
    Ok(result)
}

fn add_receipts(
    messages: &mut [MessageExt],
    header: &PopMessageResponseHeader,
    request: &PopMessageRequestHeader,
    broker_name: &str,
) -> Result<(), PopProcessError> {
    let start_offsets = header
        .get_start_offset_info()
        .filter(|value| !value.is_empty())
        .map(parse_start_offset_info)
        .transpose()?;
    let message_offsets = header
        .get_msg_offset_info()
        .filter(|value| !value.is_empty())
        .map(parse_message_offset_info)
        .transpose()?;

    let mut sorted_offsets: HashMap<String, Vec<i64>> = HashMap::new();
    if start_offsets.is_some() {
        for message in messages.iter() {
            let retry = retry_category(message.topic(), message.properties().get(PROPERTY_POP_CK))?;
            let key = format!("{retry}@{}", message.queue_id());
            sorted_offsets
                .entry(key)
                .or_default()
                .push(message.queue_offset());
        }
        for offsets in sorted_offsets.values_mut() {
            offsets.sort_unstable();
        }
    }

    let fallback_offsets = if start_offsets.is_none() {
        let mut checkpoints = HashMap::new();
        for message in messages.iter() {
            let retry = retry_category(message.topic(), message.properties().get(PROPERTY_POP_CK))?;
            checkpoints
                .entry(format!("{retry}@{}", message.queue_id()))
                .or_insert(message.queue_offset());
        }
        Some(checkpoints)
    } else {
        None
    };

    for message in messages.iter_mut() {
        if message.properties().contains_key(PROPERTY_POP_CK) {
            continue;
        }
        let retry = retry_category(message.topic(), None)?;
        let key = format!("{retry}@{}", message.queue_id());
        let raw = if let Some(start_offsets) = &start_offsets {
            let start_offset = start_offsets.get(&key).ok_or_else(|| {
                PopProcessError::MalformedMetadata(format!(
                    "startOffsetInfo has no entry for {key}"
                ))
            })?;
            let available_offsets = sorted_offsets.get(&key).ok_or_else(|| {
                PopProcessError::MalformedMetadata(format!(
                    "message offsets have no entry for {key}"
                ))
            })?;
            let index = available_offsets
                .iter()
                .position(|offset| *offset == message.queue_offset())
                .ok_or_else(|| {
                    PopProcessError::MalformedMetadata(format!(
                        "queue offset {} is absent from {key}",
                        message.queue_offset()
                    ))
                })?;
            let queue_offset = message_offsets
                .as_ref()
                .and_then(|offsets| offsets.get(&key))
                .and_then(|offsets| offsets.get(index))
                .copied()
                .ok_or_else(|| {
                    PopProcessError::MalformedMetadata(format!(
                        "msgOffsetInfo has no offset {index} for {key}"
                    ))
                })?;
            format!(
                "{} {} {} {} {} {} {} {}",
                start_offset,
                header.get_pop_time(),
                header.get_invisible_time(),
                header.get_revive_qid(),
                retry,
                broker_name,
                message.queue_id(),
                queue_offset
            )
        } else {
            let checkpoint = fallback_offsets
                .as_ref()
                .and_then(|offsets| offsets.get(&key))
                .copied()
                .unwrap_or(message.queue_offset());
            format!(
                "{} {} {} {} {} {} {} {}",
                checkpoint,
                header.get_pop_time(),
                header.get_invisible_time(),
                header.get_revive_qid(),
                retry,
                broker_name,
                message.queue_id(),
                message.queue_offset()
            )
        };
        let _ = message.set_system_property(PROPERTY_POP_CK, raw);
    }

    let topic = request.get_topic().unwrap_or_default().to_owned();
    for message in messages.iter_mut() {
        message.set_broker_name(Some(broker_name.to_owned()));
        message.set_topic(topic.clone());
        if !message.properties().contains_key(PROPERTY_FIRST_POP_TIME) {
            let _ = message
                .set_system_property(PROPERTY_FIRST_POP_TIME, header.get_pop_time().to_string());
        }
    }
    Ok(())
}

fn retry_category(topic: &str, pop_ck: Option<&String>) -> Result<&'static str, PopProcessError> {
    if let Some(pop_ck) = pop_ck {
        let receipt = PopReceipt::parse(pop_ck.clone())
            .map_err(|error| PopProcessError::MalformedMetadata(error.to_string()))?;
        return match receipt.retry() {
            "0" => Ok("0"),
            "1" => Ok("1"),
            "2" => Ok("2"),
            other => Err(PopProcessError::MalformedMetadata(format!(
                "unknown retry category {other}"
            ))),
        };
    }
    if topic.starts_with("%RETRY%") {
        Ok(if topic.contains('+') { "2" } else { "1" })
    } else {
        Ok("0")
    }
}

fn parse_start_offset_info(value: &str) -> Result<HashMap<String, i64>, PopProcessError> {
    let mut result = HashMap::new();
    for entry in value.split(';') {
        let fields = entry.split(' ').collect::<Vec<_>>();
        if fields.len() != 3 {
            return Err(PopProcessError::MalformedMetadata(format!(
                "invalid startOffsetInfo entry {entry:?}"
            )));
        }
        let key = format!("{}@{}", fields[0], fields[1]);
        let offset = fields[2].parse::<i64>().map_err(|_| {
            PopProcessError::MalformedMetadata(format!("invalid start offset in {entry:?}"))
        })?;
        if result.insert(key.clone(), offset).is_some() {
            return Err(PopProcessError::MalformedMetadata(format!(
                "duplicate startOffsetInfo entry {key}"
            )));
        }
    }
    Ok(result)
}

fn parse_message_offset_info(value: &str) -> Result<HashMap<String, Vec<i64>>, PopProcessError> {
    let mut result = HashMap::new();
    for entry in value.split(';') {
        let fields = entry.split(' ').collect::<Vec<_>>();
        if fields.len() != 3 {
            return Err(PopProcessError::MalformedMetadata(format!(
                "invalid msgOffsetInfo entry {entry:?}"
            )));
        }
        let key = format!("{}@{}", fields[0], fields[1]);
        let offsets = fields[2]
            .split(',')
            .map(|offset| {
                offset.parse::<i64>().map_err(|_| {
                    PopProcessError::MalformedMetadata(format!(
                        "invalid message offset in {entry:?}"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if result.insert(key.clone(), offsets).is_some() {
            return Err(PopProcessError::MalformedMetadata(format!(
                "duplicate msgOffsetInfo entry {key}"
            )));
        }
    }
    Ok(result)
}

impl std::fmt::Display for PopResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "PopResult [popStatus={},msgFoundList={},restNum={}]",
            self.status,
            self.messages.as_ref().map_or(0, Vec::len),
            self.rest_num
        )
    }
}

/// A validated POP receipt. The original extra-info string is retained because the
/// Broker uses it as an opaque token when acknowledging or extending invisibility.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PopReceipt {
    raw: String,
    checkpoint_queue_offset: i64,
    pop_time: i64,
    invisible_time: i64,
    revive_queue_id: i32,
    retry: String,
    broker_name: String,
    queue_id: i32,
    queue_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PopReceiptError {
    #[error("POP receipt is missing required fields (found {actual}, need at least 8)")]
    MissingFields { actual: usize },
    #[error("POP receipt field {field} is not a valid {kind}")]
    InvalidNumber {
        field: &'static str,
        kind: &'static str,
    },
    #[error("POP receipt queue offset is outside the Java batch ACK bitset range")]
    BatchAckOffsetOutOfRange,
}

impl PopReceipt {
    /// Parses the eight fields consumed by Java `ExtraInfoUtil` and retains the
    /// unmodified source string, including any fields added by newer Brokers.
    pub fn parse(raw: impl Into<String>) -> Result<Self, PopReceiptError> {
        let raw = raw.into();
        let mut fields = raw.split(' ').collect::<Vec<_>>();
        while fields.last().is_some_and(|field| field.is_empty()) {
            fields.pop();
        }
        if fields.len() < 8 {
            return Err(PopReceiptError::MissingFields {
                actual: fields.len(),
            });
        }

        Ok(Self {
            checkpoint_queue_offset: parse_i64(fields[0], "checkpoint queue offset", "long")?,
            pop_time: parse_i64(fields[1], "pop time", "long")?,
            invisible_time: parse_i64(fields[2], "invisibility time", "long")?,
            revive_queue_id: parse_i32(fields[3], "revive queue id", "int")?,
            retry: fields[4].to_owned(),
            broker_name: fields[5].to_owned(),
            queue_id: parse_i32(fields[6], "queue id", "int")?,
            queue_offset: parse_i64(fields[7], "queue offset", "long")?,
            raw,
        })
    }

    pub fn raw(&self) -> &str {
        &self.raw
    }

    pub const fn checkpoint_queue_offset(&self) -> i64 {
        self.checkpoint_queue_offset
    }

    pub const fn pop_time(&self) -> i64 {
        self.pop_time
    }

    pub const fn invisible_time(&self) -> i64 {
        self.invisible_time
    }

    pub const fn revive_queue_id(&self) -> i32 {
        self.revive_queue_id
    }

    pub fn retry(&self) -> &str {
        &self.retry
    }

    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    pub const fn queue_id(&self) -> i32 {
        self.queue_id
    }

    pub const fn queue_offset(&self) -> i64 {
        self.queue_offset
    }

    /// Resolves the Broker topic encoded by Java's retry discriminator.
    pub fn real_topic(&self, topic: &str, consumer_group: &str) -> String {
        match self.retry.as_str() {
            "1" => format!("%RETRY%{consumer_group}_{topic}"),
            "2" => format!("%RETRY%{consumer_group}+{topic}"),
            _ => topic.to_owned(),
        }
    }

    pub fn ack_header(&self, topic: &str, consumer_group: &str) -> AckMessageRequestHeader {
        {
            let mut __phase18_header_0 = AckMessageRequestHeader::default();
            __phase18_header_0.set_consumer_group(consumer_group.to_owned());
            __phase18_header_0.set_topic(self.real_topic(topic, consumer_group));
            __phase18_header_0.set_queue_id(self.queue_id);
            __phase18_header_0.set_extra_info(self.raw.clone());
            __phase18_header_0.set_offset(self.queue_offset);
            __phase18_header_0.set_lite_topic(None);
            __phase18_header_0
        }
    }

    pub fn change_invisible_header(
        &self,
        topic: &str,
        consumer_group: &str,
        invisible_time: i64,
    ) -> ChangeInvisibleTimeRequestHeader {
        {
            let mut __phase18_header_1 = ChangeInvisibleTimeRequestHeader::default();
            __phase18_header_1.set_consumer_group(Some(consumer_group.to_owned()));
            __phase18_header_1.set_topic(Some(self.real_topic(topic, consumer_group)));
            __phase18_header_1.set_queue_id(Some(self.queue_id));
            __phase18_header_1.set_extra_info(Some(self.raw.clone()));
            __phase18_header_1.set_offset(Some(self.queue_offset));
            __phase18_header_1.set_invisible_time(Some(invisible_time));
            __phase18_header_1.set_lite_topic(None);
            __phase18_header_1.set_suspend(false);
            __phase18_header_1
        }
    }
}

/// Builds the source-used batch ACK request body. Java merges receipts that
/// share retry kind, queue, checkpoint offset and pop time into one bitset.
#[cfg(test)]
fn build_batch_ack_body(
    receipts: &[PopReceipt],
    topic: &str,
    consumer_group: &str,
) -> Result<BatchAckMessageRequestBody, PopReceiptError> {
    type BatchKey = (String, i32, i64, i64);
    let mut broker_name = None;
    let mut batches = BTreeMap::<BatchKey, BatchAck>::new();

    for receipt in receipts {
        broker_name.get_or_insert_with(|| receipt.broker_name.clone());
        let key = (
            receipt.retry.clone(),
            receipt.queue_id,
            receipt.checkpoint_queue_offset,
            receipt.pop_time,
        );
        let batch = batches.entry(key).or_insert_with(|| {
            let mut batch = BatchAck::default();
            batch.set_consumer_group(Some(consumer_group.to_owned()));
            batch.set_topic(Some(topic.to_owned()));
            batch.set_retry(Some(receipt.retry.clone()));
            batch.set_start_offset(receipt.checkpoint_queue_offset);
            batch.set_queue_id(receipt.queue_id);
            batch.set_revive_queue_id(receipt.revive_queue_id);
            batch.set_pop_time(receipt.pop_time);
            batch.set_invisible_time(receipt.invisible_time);
            batch.set_bit_set(Some(SharedValue::new(BitSet::new())));
            batch
        });
        let bit_index = receipt
            .queue_offset
            .checked_sub(receipt.checkpoint_queue_offset)
            .and_then(|offset| i32::try_from(offset).ok())
            .filter(|offset| *offset >= 0)
            .ok_or(PopReceiptError::BatchAckOffsetOutOfRange)?;
        let bit_set = batch
            .get_bit_set()
            .ok_or(PopReceiptError::BatchAckOffsetOutOfRange)?;
        bit_set
            .with_mut(|bit_set| bit_set.set(bit_index))
            .map_err(|_| PopReceiptError::BatchAckOffsetOutOfRange)?;
    }

    let mut body = BatchAckMessageRequestBody::default();
    body.set_broker_name(broker_name);
    body.set_acks(Some(SharedValue::new(
        batches
            .into_values()
            .map(|ack| Some(SharedValue::new(ack)))
            .collect(),
    )));
    Ok(body)
}

fn parse_i64(value: &str, field: &'static str, kind: &'static str) -> Result<i64, PopReceiptError> {
    value
        .parse()
        .map_err(|_| PopReceiptError::InvalidNumber { field, kind })
}

fn parse_i32(value: &str, field: &'static str, kind: &'static str) -> Result<i32, PopReceiptError> {
    value
        .parse()
        .map_err(|_| PopReceiptError::InvalidNumber { field, kind })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_classic_common::message::Message;

    fn message(topic: &str, queue_id: i32, queue_offset: i64) -> MessageExt {
        let mut message = crate::test_support::message_ext_from(Message::new(topic, Vec::new()));
        message.set_queue_id(queue_id);
        message.set_queue_offset(queue_offset);
        message
    }

    #[test]
    fn receipt_matches_pinned_java_fields_and_keeps_the_original_token() {
        let raw = "17 123 456 2 1 broker-a 4 19";
        let receipt = PopReceipt::parse(raw).unwrap();
        assert_eq!(receipt.raw(), raw);
        assert_eq!(receipt.checkpoint_queue_offset(), 17);
        assert_eq!(receipt.pop_time(), 123);
        assert_eq!(receipt.invisible_time(), 456);
        assert_eq!(receipt.revive_queue_id(), 2);
        assert_eq!(receipt.retry(), "1");
        assert_eq!(receipt.broker_name(), "broker-a");
        assert_eq!(receipt.queue_id(), 4);
        assert_eq!(receipt.queue_offset(), 19);
    }

    #[test]
    fn ack_and_invisibility_headers_use_real_topic_and_preserve_receipt() {
        let receipt = PopReceipt::parse("17 123 456 2 2 broker-a 4 19 extra-field").unwrap();
        let ack = receipt.ack_header("orders", "workers");
        assert_eq!(ack.get_topic(), "%RETRY%workers+orders");
        assert_eq!(ack.get_offset(), 19);
        assert_eq!(ack.get_extra_info(), receipt.raw());
        let change = receipt.change_invisible_header("orders", "workers", 9_000);
        assert_eq!(change.get_consumer_group(), Some("workers"));
        assert_eq!(change.get_topic(), Some(ack.get_topic()));
        assert_eq!(change.get_queue_id(), Some(4));
        assert_eq!(change.get_offset(), Some(19));
        assert_eq!(change.get_invisible_time(), Some(9_000));
        assert_eq!(change.get_extra_info(), Some(receipt.raw()));
    }

    #[test]
    fn retry_topic_names_match_the_pinned_java_key_builder() {
        let v1 = PopReceipt::parse("17 123 456 2 1 broker-a 4 19").unwrap();
        let v2 = PopReceipt::parse("17 123 456 2 2 broker-a 4 19").unwrap();
        assert_eq!(v1.real_topic("orders", "workers"), "%RETRY%workers_orders");
        assert_eq!(v2.real_topic("orders", "workers"), "%RETRY%workers+orders");
    }

    #[test]
    fn pop_response_metadata_maps_receipts_by_retry_kind_and_queue() {
        let request = {
            let mut __phase18_header_2 = PopMessageRequestHeader::default();
            __phase18_header_2.set_topic(Some("orders".into()));
            __phase18_header_2
        };
        let mut header = PopMessageResponseHeader::default();
        header.set_pop_time(123);
        header.set_invisible_time(456);
        header.set_revive_qid(2);
        header.set_rest_num(8);
        header.set_start_offset_info(Some("0 1 10;0 2 20".into()));
        header.set_msg_offset_info(Some("0 1 100,101;0 2 200".into()));
        let mut messages = vec![
            message("orders", 1, 5),
            message("orders", 1, 6),
            message("orders", 2, 9),
        ];
        add_receipts(&mut messages, &header, &request, "broker-a").unwrap();

        let receipts = messages
            .iter()
            .map(|message| {
                PopReceipt::parse(message.properties().get(PROPERTY_POP_CK).unwrap().clone())
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(receipts[0].checkpoint_queue_offset(), 10);
        assert_eq!(receipts[0].queue_offset(), 100);
        assert_eq!(receipts[1].queue_offset(), 101);
        assert_eq!(receipts[2].checkpoint_queue_offset(), 20);
        assert_eq!(receipts[2].queue_offset(), 200);
        assert_eq!(messages[0].broker_name(), Some("broker-a"));
        assert_eq!(messages[0].topic(), "orders");
        assert_eq!(
            messages[0]
                .properties()
                .get(PROPERTY_FIRST_POP_TIME)
                .map(String::as_str),
            Some("123")
        );
    }

    #[test]
    fn pop_response_without_checkpoint_metadata_uses_first_queue_offset() {
        let request = {
            let mut __phase18_header_3 = PopMessageRequestHeader::default();
            __phase18_header_3.set_topic(Some("orders".into()));
            __phase18_header_3
        };
        let mut header = PopMessageResponseHeader::default();
        header.set_pop_time(123);
        header.set_invisible_time(456);
        header.set_revive_qid(2);
        header.set_rest_num(0);
        header.set_start_offset_info(Some(String::new()));
        header.set_msg_offset_info(Some(String::new()));
        let mut messages = vec![message("orders", 1, 5), message("orders", 1, 6)];
        add_receipts(&mut messages, &header, &request, "broker-a").unwrap();
        let first =
            PopReceipt::parse(messages[0].properties().get(PROPERTY_POP_CK).unwrap()).unwrap();
        let second =
            PopReceipt::parse(messages[1].properties().get(PROPERTY_POP_CK).unwrap()).unwrap();
        assert_eq!(first.checkpoint_queue_offset(), 5);
        assert_eq!(second.checkpoint_queue_offset(), 5);
        assert_eq!(second.queue_offset(), 6);
    }

    #[test]
    fn malformed_mixed_queue_metadata_is_rejected_before_dispatch() {
        let request = {
            let mut __phase18_header_4 = PopMessageRequestHeader::default();
            __phase18_header_4.set_topic(Some("orders".into()));
            __phase18_header_4
        };
        let mut header = PopMessageResponseHeader::default();
        header.set_pop_time(123);
        header.set_invisible_time(456);
        header.set_revive_qid(2);
        header.set_rest_num(0);
        header.set_start_offset_info(Some("0 1 10".into()));
        header.set_msg_offset_info(Some("0 2 100".into()));
        let mut messages = vec![message("orders", 1, 5)];
        assert!(matches!(
            add_receipts(&mut messages, &header, &request, "broker-a"),
            Err(PopProcessError::MalformedMetadata(_))
        ));
    }

    #[test]
    fn batch_ack_groups_receipts_and_sets_java_bit_positions() {
        let receipts = [
            PopReceipt::parse("10 123 456 2 0 broker-a 1 10").unwrap(),
            PopReceipt::parse("10 123 456 2 0 broker-a 1 12").unwrap(),
            PopReceipt::parse("20 200 300 3 2 broker-a 2 21").unwrap(),
        ];
        let body = build_batch_ack_body(&receipts, "orders", "workers").unwrap();
        assert_eq!(body.get_broker_name(), Some("broker-a"));
        let batches = body
            .get_acks()
            .unwrap()
            .snapshot()
            .into_iter()
            .flatten()
            .map(|ack| ack.snapshot())
            .collect::<Vec<_>>();
        assert_eq!(batches.len(), 2);
        let first = batches
            .iter()
            .find(|batch| batch.get_start_offset() == 10)
            .unwrap();
        let bits = first.get_bit_set().unwrap().snapshot();
        assert!(bits.get(0).unwrap());
        assert!(bits.get(2).unwrap());
        let retry = batches
            .iter()
            .find(|batch| batch.get_start_offset() == 20)
            .unwrap();
        assert_eq!(retry.get_retry(), Some("2"));
        assert!(
            retry
                .get_bit_set()
                .unwrap()
                .with(|bits| bits.get(1).unwrap())
        );
    }

    #[test]
    fn batch_ack_rejects_offsets_that_overflow_the_java_bitset_index() {
        for raw in [
            "9223372036854775807 123 456 2 1 broker-a 4 -9223372036854775808",
            "10 123 456 2 1 broker-a 4 9",
            "0 123 456 2 1 broker-a 4 2147483648",
        ] {
            let receipt = PopReceipt::parse(raw).unwrap();
            assert!(matches!(
                build_batch_ack_body(&[receipt], "orders", "workers"),
                Err(PopReceiptError::BatchAckOffsetOutOfRange)
            ));
        }
    }

    #[test]
    fn receipt_rejects_missing_fields_and_numeric_overflow() {
        assert!(matches!(
            PopReceipt::parse("17 123 456 2 1 broker-a 4"),
            Err(PopReceiptError::MissingFields { .. })
        ));
        assert!(matches!(
            PopReceipt::parse("17 123 456 999999999999999999999999 1 broker-a 4 19"),
            Err(PopReceiptError::InvalidNumber {
                field: "revive queue id",
                ..
            })
        ));
        assert!(matches!(
            PopReceipt::parse("17 123 456 2 1 broker-a 4 9223372036854775808"),
            Err(PopReceiptError::InvalidNumber {
                field: "queue offset",
                ..
            })
        ));
    }
}
