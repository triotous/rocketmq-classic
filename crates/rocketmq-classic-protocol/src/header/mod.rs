use std::collections::BTreeMap;

use thiserror::Error;

pub type HeaderMap = BTreeMap<String, String>;

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
}
