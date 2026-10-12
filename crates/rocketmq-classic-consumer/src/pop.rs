//! POP message receipt data used by the push consumer's ACK lifecycle.

use rocketmq_classic_protocol::header::{
    AckMessageRequestHeader, ChangeInvisibleTimeRequestHeader,
};

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
            "1" => format!("%RETRY%{consumer_group}:{topic}"),
            "2" => format!("%RETRY%{consumer_group}+{topic}"),
            _ => topic.to_owned(),
        }
    }

    pub fn ack_header(&self, topic: &str, consumer_group: &str) -> AckMessageRequestHeader {
        AckMessageRequestHeader {
            consumer_group: consumer_group.to_owned(),
            topic: self.real_topic(topic, consumer_group),
            queue_id: self.queue_id,
            extra_info: self.raw.clone(),
            offset: self.queue_offset,
            lite_topic: None,
        }
    }

    pub fn change_invisible_header(
        &self,
        topic: &str,
        consumer_group: &str,
        invisible_time: i64,
    ) -> ChangeInvisibleTimeRequestHeader {
        ChangeInvisibleTimeRequestHeader {
            consumer_group: consumer_group.to_owned(),
            topic: self.real_topic(topic, consumer_group),
            queue_id: self.queue_id,
            extra_info: self.raw.clone(),
            offset: self.queue_offset,
            invisible_time,
            lite_topic: None,
            suspend: false,
        }
    }
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
        assert_eq!(ack.topic, "%RETRY%workers+orders");
        assert_eq!(ack.offset, 19);
        assert_eq!(ack.extra_info, receipt.raw());
        let change = receipt.change_invisible_header("orders", "workers", 9_000);
        assert_eq!(change.topic, ack.topic);
        assert_eq!(change.offset, 19);
        assert_eq!(change.invisible_time, 9_000);
        assert_eq!(change.extra_info, receipt.raw());
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
