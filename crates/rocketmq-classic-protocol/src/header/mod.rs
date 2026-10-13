use std::collections::BTreeMap;

use thiserror::Error;

pub type HeaderMap = BTreeMap<String, String>;

pub use crate::body::ConsumerListByGroupResponseBody as GetConsumerListByGroupResponseBody;

mod basic;
pub use basic::{
    AckMessageRequestHeader, BoundaryType, ChangeInvisibleTimeRequestHeader,
    CheckTransactionStateRequestHeader, CheckTransactionStateResponseHeader,
    ConsumeMessageDirectlyResultRequestHeader, ConsumerSendMsgBackRequestHeader,
    EndTransactionRequestHeader, EndTransactionResponseHeader, GetConsumerListByGroupRequestHeader,
    GetConsumerStatusRequestHeader, GetEarliestMsgStoretimeRequestHeader,
    GetEarliestMsgStoretimeResponseHeader, GetMaxOffsetRequestHeader, GetMaxOffsetResponseHeader,
    GetMinOffsetRequestHeader, GetMinOffsetResponseHeader, GetRouteInfoRequestHeader,
    LiteSubscriptionCtlRequestHeader, LockBatchMqRequestHeader, NotificationRequestHeader,
    NotificationResponseHeader, NotifyUnsubscribeLiteRequestHeader, PollingInfoRequestHeader,
    PollingInfoResponseHeader, PopLiteMessageRequestHeader, PopLiteMessageResponseHeader,
    PopMessageRequestHeader, PopMessageResponseHeader, PullMessageRequestHeader,
    PullMessageResponseHeader, QueryConsumerOffsetRequestHeader, QueryConsumerOffsetResponseHeader,
    RecallMessageRequestHeader, RecallMessageResponseHeader, ReplyMessageRequestHeader,
    ResetOffsetRequestHeader, SearchOffsetRequestHeader, SearchOffsetResponseHeader,
    SendMessageRequestHeaderV1, SendMessageRequestHeaderV2, SendMessageResponseHeader,
    UnlockBatchMqRequestHeader, UnregisterClientRequestHeader, UnregisterClientResponseHeader,
    UpdateConsumerOffsetRequestHeader,
};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProtocolError {
    #[error("missing required header field `{0}`")]
    MissingField(&'static str),
    #[error("invalid header field `{field}`: {reason}")]
    InvalidField { field: &'static str, reason: String },
}

pub trait CommandHeader: Sized {
    fn encode_fields(&self, fields: &mut HeaderMap);
    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError>;
}

pub fn required_string<'a>(
    fields: &'a HeaderMap,
    field: &'static str,
) -> Result<&'a str, ProtocolError> {
    fields
        .get(field)
        .map(String::as_str)
        .ok_or(ProtocolError::MissingField(field))
}

pub fn optional_string(fields: &HeaderMap, field: &str) -> Option<String> {
    fields.get(field).cloned()
}

pub fn required_i32(fields: &HeaderMap, field: &'static str) -> Result<i32, ProtocolError> {
    parse_number(required_string(fields, field)?, field)
}

pub fn optional_i32(fields: &HeaderMap, field: &'static str) -> Result<Option<i32>, ProtocolError> {
    fields
        .get(field)
        .map(|value| parse_number(value, field))
        .transpose()
}

pub fn required_i64(fields: &HeaderMap, field: &'static str) -> Result<i64, ProtocolError> {
    parse_number(required_string(fields, field)?, field)
}

pub fn optional_i64(fields: &HeaderMap, field: &'static str) -> Result<Option<i64>, ProtocolError> {
    fields
        .get(field)
        .map(|value| parse_number(value, field))
        .transpose()
}

pub fn java_boolean(fields: &HeaderMap, field: &str) -> bool {
    fields
        .get(field)
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

pub fn optional_java_boolean(fields: &HeaderMap, field: &str) -> Option<bool> {
    fields
        .get(field)
        .map(|value| value.eq_ignore_ascii_case("true"))
}

fn parse_number<T>(value: &str, field: &'static str) -> Result<T, ProtocolError>
where
    T: std::str::FromStr,
{
    value.parse().map_err(|_| ProtocolError::InvalidField {
        field,
        reason: "expected a valid number".into(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProofHeader {
    pub topic: String,
}

impl CommandHeader for ProofHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("topic".into(), self.topic.clone());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        fields
            .get("topic")
            .cloned()
            .map(|topic| Self { topic })
            .ok_or(ProtocolError::MissingField("topic"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proof_header_round_trips_without_consuming_raw_fields() {
        let header = ProofHeader {
            topic: "orders".into(),
        };
        let mut raw = HeaderMap::from([("opaque".into(), "preserved".into())]);
        header.encode_fields(&mut raw);
        let decoded = ProofHeader::decode_fields(&raw).unwrap();
        assert_eq!(decoded, header);
        assert_eq!(raw.get("opaque").map(String::as_str), Some("preserved"));
    }

    #[test]
    fn field_parsers_match_java_boolean_and_signed_number_behavior() {
        assert_eq!(
            required_i32(
                &HeaderMap::from([("value".into(), i32::MIN.to_string())]),
                "value"
            ),
            Ok(i32::MIN)
        );
        assert_eq!(
            optional_i64(
                &HeaderMap::from([("value".into(), i64::MAX.to_string())]),
                "value"
            ),
            Ok(Some(i64::MAX))
        );
        assert!(matches!(
            required_i32(
                &HeaderMap::from([("value".into(), "2147483648".into())]),
                "value"
            ),
            Err(ProtocolError::InvalidField { field: "value", .. })
        ));
        assert!(matches!(
            required_string(&HeaderMap::new(), "value"),
            Err(ProtocolError::MissingField("value"))
        ));
        assert!(java_boolean(
            &HeaderMap::from([("flag".into(), "TrUe".into())]),
            "flag"
        ));
        for value in ["false", "1", " true ", "yes"] {
            assert!(!java_boolean(
                &HeaderMap::from([("flag".into(), value.into())]),
                "flag"
            ));
        }
        assert_eq!(optional_java_boolean(&HeaderMap::new(), "flag"), None);
    }
}
