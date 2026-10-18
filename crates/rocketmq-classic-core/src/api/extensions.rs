//! One-attempt lifecycle and extension requests. Message decoding belongs to the consumer layer.
use rocketmq_classic_protocol::{
    body::{
        BatchAckMessageRequestBody, LiteSubscriptionCtlRequestBody, QueryAssignmentRequestBody,
        QueryAssignmentResponseBody, SetMessageRequestModeRequestBody,
    },
    codes::{request, response},
    header::{
        AckMessageRequestHeader, ChangeInvisibleTimeRequestHeader,
        ChangeInvisibleTimeResponseHeader, HeartbeatRequestHeader,
        LiteSubscriptionCtlRequestHeader, PopLiteMessageRequestHeader,
        PopLiteMessageResponseHeader, PopMessageRequestHeader, PopMessageResponseHeader,
        ProtocolError, UnregisterClientRequestHeader,
    },
    heartbeat::HeartbeatData,
};
use rocketmq_classic_remoting::RemotingCommand;
use tokio::time::Instant;

use super::{MQClientApi, classify, decode_body};
use crate::error::RpcError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatV2Result {
    pub version: i32,
    pub is_sub_change: bool,
    pub is_support_heartbeat_v2: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PopStatus {
    Found,
    NoNewMsg,
    PollingNotFound,
    PollingFull,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PopStatusLookupError {
    #[error("Name is null")]
    NullName,
    #[error("No enum constant org.apache.rocketmq.client.consumer.PopStatus.{0}")]
    UnknownName(String),
}

impl PopStatus {
    pub const fn values() -> [Self; 4] {
        [
            Self::Found,
            Self::NoNewMsg,
            Self::PollingFull,
            Self::PollingNotFound,
        ]
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, PopStatusLookupError> {
        match name {
            None => Err(PopStatusLookupError::NullName),
            Some("FOUND") => Ok(Self::Found),
            Some("NO_NEW_MSG") => Ok(Self::NoNewMsg),
            Some("POLLING_FULL") => Ok(Self::PollingFull),
            Some("POLLING_NOT_FOUND") => Ok(Self::PollingNotFound),
            Some(other) => Err(PopStatusLookupError::UnknownName(other.to_owned())),
        }
    }

    pub const fn name(&self) -> &'static str {
        match self {
            Self::Found => "FOUND",
            Self::NoNewMsg => "NO_NEW_MSG",
            Self::PollingFull => "POLLING_FULL",
            Self::PollingNotFound => "POLLING_NOT_FOUND",
        }
    }

    pub const fn ordinal(&self) -> i32 {
        match self {
            Self::Found => 0,
            Self::NoNewMsg => 1,
            Self::PollingFull => 2,
            Self::PollingNotFound => 3,
        }
    }

    pub const fn java_compare_to(&self, other: &Self) -> i32 {
        self.ordinal() - other.ordinal()
    }
}

impl std::fmt::Display for PopStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PopReply<H> {
    pub status: PopStatus,
    pub header: Option<H>,
    pub raw_messages: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckStatus {
    Ok,
    NoExist,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AckStatusLookupError {
    #[error("Name is null")]
    NullName,
    #[error("No enum constant org.apache.rocketmq.client.consumer.AckStatus.{0}")]
    UnknownName(String),
}

impl AckStatus {
    pub const fn values() -> [Self; 2] {
        [Self::Ok, Self::NoExist]
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, AckStatusLookupError> {
        match name {
            None => Err(AckStatusLookupError::NullName),
            Some("OK") => Ok(Self::Ok),
            Some("NO_EXIST") => Ok(Self::NoExist),
            Some(other) => Err(AckStatusLookupError::UnknownName(other.to_owned())),
        }
    }

    pub const fn name(&self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::NoExist => "NO_EXIST",
        }
    }

    pub const fn ordinal(&self) -> i32 {
        match self {
            Self::Ok => 0,
            Self::NoExist => 1,
        }
    }

    pub const fn java_compare_to(&self, other: &Self) -> i32 {
        self.ordinal() - other.ordinal()
    }
}

impl std::fmt::Display for AckStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeInvisibleTimeResult {
    pub status: AckStatus,
    pub pop_time: Option<i64>,
    pub invisible_time: Option<i64>,
    pub revive_qid: Option<i32>,
    pub extra_info: Option<String>,
}

fn json_body<T: serde::Serialize>(value: &T, request_code: i32) -> Result<Vec<u8>, RpcError> {
    serde_json::to_vec(value).map_err(|source| RpcError::RequestSerialization {
        request_code,
        source,
    })
}

fn heartbeat_json_body(data: &HeartbeatData) -> Result<Vec<u8>, RpcError> {
    data.java_encode()
        .map_err(|source| RpcError::RequestSerialization {
            request_code: request::HEART_BEAT,
            source,
        })
}

fn pop_status(reply: &RemotingCommand, code: i32, addr: &str) -> Result<PopStatus, RpcError> {
    match reply.code {
        response::SUCCESS => Ok(PopStatus::Found),
        response::PULL_NOT_FOUND => Ok(PopStatus::PollingNotFound),
        response::POLLING_TIMEOUT => Ok(PopStatus::PollingNotFound),
        response::POLLING_FULL => Ok(PopStatus::PollingFull),
        _ => Err(RpcError::Response {
            request_code: code,
            peer: addr.into(),
            code: reply.code,
            remark: reply.remark.clone(),
        }),
    }
}

// Other POP response headers still use Java's reflective primitive defaults.
fn primitive<T: std::str::FromStr + Default>(reply: &RemotingCommand, key: &str) -> T {
    reply
        .ext_fields
        .as_ref()
        .and_then(|fields| fields.get(key))
        .and_then(|value| value.parse().ok())
        .unwrap_or_default()
}

fn optional_field(reply: &RemotingCommand, key: &str) -> Option<String> {
    reply
        .ext_fields
        .as_ref()
        .and_then(|fields| fields.get(key))
        .cloned()
}

impl MQClientApi {
    pub async fn send_heartbeat(
        &self,
        addr: &str,
        data: &HeartbeatData,
        deadline: Instant,
    ) -> Result<i32, RpcError> {
        let body = heartbeat_json_body(data)?;
        let reply = self
            .request(
                addr,
                request::HEART_BEAT,
                &HeartbeatRequestHeader::default(),
                Some(body),
                deadline,
            )
            .await?;
        Ok(classify(reply, request::HEART_BEAT, addr)?.version)
    }

    pub async fn send_heartbeat_v2(
        &self,
        addr: &str,
        data: &HeartbeatData,
        deadline: Instant,
    ) -> Result<HeartbeatV2Result, RpcError> {
        let body = heartbeat_json_body(data)?;
        let reply = classify(
            self.request(
                addr,
                request::HEART_BEAT,
                &HeartbeatRequestHeader::default(),
                Some(body),
                deadline,
            )
            .await?,
            request::HEART_BEAT,
            addr,
        )?;
        let fields = reply.ext_fields.as_ref();
        Ok(HeartbeatV2Result {
            version: reply.version,
            is_sub_change: fields
                .and_then(|f| f.get("IS_SUB_CHANGE"))
                .is_some_and(|v| v.eq_ignore_ascii_case("true")),
            is_support_heartbeat_v2: fields
                .and_then(|f| f.get("IS_SUPPORT_HEART_BEAT_V2"))
                .is_some_and(|v| v.eq_ignore_ascii_case("true")),
        })
    }

    pub async fn unregister_client(
        &self,
        addr: &str,
        header: &UnregisterClientRequestHeader,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        classify(
            self.request(addr, request::UNREGISTER_CLIENT, header, None, deadline)
                .await?,
            request::UNREGISTER_CLIENT,
            addr,
        )?;
        Ok(())
    }

    pub async fn query_assignment(
        &self,
        addr: &str,
        body: &QueryAssignmentRequestBody,
        deadline: Instant,
    ) -> Result<QueryAssignmentResponseBody, RpcError> {
        let reply = classify(
            self.request(
                addr,
                request::QUERY_ASSIGNMENT,
                &LiteSubscriptionCtlRequestHeader,
                Some(json_body(body, request::QUERY_ASSIGNMENT)?),
                deadline,
            )
            .await?,
            request::QUERY_ASSIGNMENT,
            addr,
        )?;
        decode_body(&reply, request::QUERY_ASSIGNMENT, addr)
    }

    pub async fn set_message_request_mode(
        &self,
        addr: &str,
        body: &SetMessageRequestModeRequestBody,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        classify(
            self.request(
                addr,
                request::SET_MESSAGE_REQUEST_MODE,
                &LiteSubscriptionCtlRequestHeader,
                Some(json_body(body, request::SET_MESSAGE_REQUEST_MODE)?),
                deadline,
            )
            .await?,
            request::SET_MESSAGE_REQUEST_MODE,
            addr,
        )?;
        Ok(())
    }

    pub async fn lite_subscription_ctl(
        &self,
        addr: &str,
        body: &LiteSubscriptionCtlRequestBody,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        classify(
            self.request_future(
                addr,
                request::LITE_SUBSCRIPTION_CTL,
                &LiteSubscriptionCtlRequestHeader,
                Some(json_body(body, request::LITE_SUBSCRIPTION_CTL)?),
                deadline,
            )
            .await?,
            request::LITE_SUBSCRIPTION_CTL,
            addr,
        )?;
        Ok(())
    }

    pub async fn pop_message(
        &self,
        addr: &str,
        header: &PopMessageRequestHeader,
        deadline: Instant,
    ) -> Result<PopReply<PopMessageResponseHeader>, RpcError> {
        let reply = self
            .request_future(addr, request::POP_MESSAGE, header, None, deadline)
            .await?;
        let status = pop_status(&reply, request::POP_MESSAGE, addr)?;
        if status == PopStatus::Found && reply.body.is_none() {
            return Err(RpcError::MalformedResponse {
                request_code: request::POP_MESSAGE,
                peer: addr.into(),
                reason: "missing pop message body".into(),
            });
        }
        let parsed = Some(PopMessageResponseHeader {
            pop_time: primitive(&reply, "popTime"),
            invisible_time: primitive(&reply, "invisibleTime"),
            revive_queue_id: primitive(&reply, "reviveQid"),
            rest_num: primitive(&reply, "restNum"),
            start_offset_info: optional_field(&reply, "startOffsetInfo"),
            msg_offset_info: optional_field(&reply, "msgOffsetInfo"),
            order_count_info: optional_field(&reply, "orderCountInfo"),
        });
        Ok(PopReply {
            status,
            header: parsed,
            raw_messages: reply.body,
        })
    }

    pub async fn pop_lite_message(
        &self,
        addr: &str,
        header: &PopLiteMessageRequestHeader,
        deadline: Instant,
    ) -> Result<PopReply<PopLiteMessageResponseHeader>, RpcError> {
        let reply = self
            .request_future(addr, request::POP_LITE_MESSAGE, header, None, deadline)
            .await?;
        let status = pop_status(&reply, request::POP_LITE_MESSAGE, addr)?;
        if status == PopStatus::Found && reply.body.is_none() {
            return Err(RpcError::MalformedResponse {
                request_code: request::POP_LITE_MESSAGE,
                peer: addr.into(),
                reason: "missing pop lite message body".into(),
            });
        }
        let parsed = Some(PopLiteMessageResponseHeader {
            pop_time: primitive(&reply, "popTime"),
            invisible_time: primitive(&reply, "invisibleTime"),
            revive_queue_id: primitive(&reply, "reviveQid"),
            start_offset_info: optional_field(&reply, "startOffsetInfo"),
            msg_offset_info: optional_field(&reply, "msgOffsetInfo"),
            order_count_info: optional_field(&reply, "orderCountInfo"),
        });
        Ok(PopReply {
            status,
            header: parsed,
            raw_messages: reply.body,
        })
    }

    pub async fn ack_message(
        &self,
        addr: &str,
        header: &AckMessageRequestHeader,
        deadline: Instant,
    ) -> Result<AckStatus, RpcError> {
        let reply = self
            .request_future(addr, request::ACK_MESSAGE, header, None, deadline)
            .await?;
        Ok(if reply.code == response::SUCCESS {
            AckStatus::Ok
        } else {
            AckStatus::NoExist
        })
    }

    pub async fn batch_ack_message(
        &self,
        addr: &str,
        body: &BatchAckMessageRequestBody,
        deadline: Instant,
    ) -> Result<AckStatus, RpcError> {
        let reply = self
            .request_future(
                addr,
                request::BATCH_ACK_MESSAGE,
                &LiteSubscriptionCtlRequestHeader,
                Some(json_body(body, request::BATCH_ACK_MESSAGE)?),
                deadline,
            )
            .await?;
        Ok(if reply.code == response::SUCCESS {
            AckStatus::Ok
        } else {
            AckStatus::NoExist
        })
    }

    pub async fn change_invisible_time(
        &self,
        addr: &str,
        broker_name: &str,
        header: &ChangeInvisibleTimeRequestHeader,
        deadline: Instant,
    ) -> Result<ChangeInvisibleTimeResult, RpcError> {
        let reply = self
            .request_future(
                addr,
                request::CHANGE_MESSAGE_INVISIBLETIME,
                header,
                None,
                deadline,
            )
            .await?;
        let response_header = reply
            .decode_command_custom_header_directly::<ChangeInvisibleTimeResponseHeader>(true)?;
        let pop_time = response_header.pop_time;
        let invisible_time = response_header.invisible_time;
        let revive_qid = response_header.revive_qid;
        if reply.code != response::SUCCESS {
            return Ok(ChangeInvisibleTimeResult {
                status: AckStatus::NoExist,
                pop_time: None,
                invisible_time: None,
                revive_qid: None,
                extra_info: None,
            });
        }
        let offset = header.offset.ok_or(ProtocolError::MissingField("offset"))?;
        let topic = header.topic.as_deref();
        let queue_id = header
            .queue_id
            .ok_or(ProtocolError::MissingField("queueId"))?;
        let topic = topic.ok_or(ProtocolError::MissingField("topic"))?;
        Ok(ChangeInvisibleTimeResult {
            status: AckStatus::Ok,
            pop_time: Some(pop_time),
            invisible_time: Some(invisible_time),
            revive_qid: Some(revive_qid),
            extra_info: Some(change_invisible_extra_info(
                offset,
                pop_time,
                invisible_time,
                revive_qid,
                topic,
                broker_name,
                queue_id,
            )),
        })
    }
}

fn change_invisible_extra_info(
    offset: i64,
    pop_time: i64,
    invisible_time: i64,
    revive_qid: i32,
    topic: &str,
    broker_name: &str,
    queue_id: i32,
) -> String {
    let retry = if topic.starts_with("%RETRY%") && topic.contains('+') {
        2
    } else if topic.starts_with("%RETRY%") {
        1
    } else {
        0
    };
    format!(
        "{offset} {pop_time} {invisible_time} {revive_qid} {retry} {broker_name} {queue_id} {offset}"
    )
}

#[cfg(test)]
mod serialization_tests {
    use super::*;

    #[test]
    fn serializer_failure_returns_typed_request_serialization_error() {
        struct FailingValue;

        impl serde::Serialize for FailingValue {
            fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                Err(serde::ser::Error::custom("fixture serialization failure"))
            }
        }

        let error = json_body(&FailingValue, request::HEART_BEAT).unwrap_err();
        match error {
            RpcError::RequestSerialization {
                request_code,
                source,
            } => {
                assert_eq!(request_code, request::HEART_BEAT);
                assert!(!source.to_string().is_empty());
            }
            other => panic!("unexpected error: {other}"),
        }
    }
}

#[cfg(test)]
mod change_invisible_tests {
    use super::change_invisible_extra_info;

    #[test]
    fn extra_info_tracks_java_retry_topic_classification() {
        for (topic, retry) in [
            ("oracle-topic", "0"),
            ("%RETRY%consumer", "1"),
            ("%RETRY%consumer+P1", "2"),
        ] {
            assert_eq!(
                change_invisible_extra_info(17, 123, 456, 2, topic, "broker", 2),
                format!("17 123 456 2 {retry} broker 2 17")
            );
        }
    }
}
