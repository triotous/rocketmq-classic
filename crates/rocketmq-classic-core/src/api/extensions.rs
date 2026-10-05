//! One-attempt lifecycle and extension requests. Message decoding belongs to the consumer layer.
use rocketmq_classic_protocol::{
    body::{
        BatchAckMessageRequestBody, LiteSubscriptionCtlRequestBody, QueryAssignmentRequestBody,
        QueryAssignmentResponseBody, SetMessageRequestModeRequestBody,
    },
    codes::{request, response},
    header::{
        AckMessageRequestHeader, ChangeInvisibleTimeRequestHeader,
        LiteSubscriptionCtlRequestHeader, PopLiteMessageRequestHeader,
        PopLiteMessageResponseHeader, PopMessageRequestHeader, PopMessageResponseHeader,
        UnregisterClientRequestHeader,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeInvisibleTimeResult {
    pub status: AckStatus,
    pub pop_time: Option<i64>,
    pub invisible_time: Option<i64>,
    pub revive_qid: Option<i32>,
}

fn json_body<T: serde::Serialize>(value: &T) -> Vec<u8> {
    // Phase 4 body models are JSON wire representations and serialize infallibly.
    serde_json::to_vec(value).expect("protocol body serialization")
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

// Reflectively decoded Java primitive response fields retain zero after a missing/invalid value.
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
        let reply = self
            .request(
                addr,
                request::HEART_BEAT,
                &LiteSubscriptionCtlRequestHeader,
                Some(json_body(data)),
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
        let reply = classify(
            self.request(
                addr,
                request::HEART_BEAT,
                &LiteSubscriptionCtlRequestHeader,
                Some(json_body(data)),
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
                Some(json_body(body)),
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
                Some(json_body(body)),
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
                Some(json_body(body)),
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
                Some(json_body(body)),
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
        // Java's reflective decoder catches field conversion failures and retains primitive defaults.
        let pop_time = primitive(&reply, "popTime");
        let invisible_time = primitive(&reply, "invisibleTime");
        let revive_qid = primitive(&reply, "reviveQid");
        if reply.code != response::SUCCESS {
            return Ok(ChangeInvisibleTimeResult {
                status: AckStatus::NoExist,
                pop_time: None,
                invisible_time: None,
                revive_qid: None,
            });
        }
        Ok(ChangeInvisibleTimeResult {
            status: AckStatus::Ok,
            pop_time: Some(pop_time),
            invisible_time: Some(invisible_time),
            revive_qid: Some(revive_qid),
        })
    }
}
