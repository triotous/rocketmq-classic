use rocketmq_classic_common::{message::MessageQueue, sysflag::pull::has_lite_pull_flag};
use rocketmq_classic_protocol::{
    body::{
        ConsumerListByGroupResponseBody, LockBatchRequestBody, LockBatchResponseBody,
        UnlockBatchRequestBody,
    },
    codes::{request, response},
    header::{
        ConsumerSendMsgBackRequestHeader, GetEarliestMsgStoretimeRequestHeader,
        GetEarliestMsgStoretimeResponseHeader, GetMaxOffsetRequestHeader,
        GetMaxOffsetResponseHeader, GetMinOffsetRequestHeader, GetMinOffsetResponseHeader,
        HeaderMap, LockBatchMqRequestHeader, PullMessageRequestHeader, PullMessageResponseHeader,
        QueryConsumerOffsetRequestHeader, QueryConsumerOffsetResponseHeader,
        SearchOffsetRequestHeader, SearchOffsetResponseHeader, UpdateConsumerOffsetRequestHeader,
    },
};
use tokio::time::Instant;

use super::{MQClientApi, classify, decode_body, decode_header};
use crate::error::RpcError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PullStatus {
    Found,
    NoNewMessage,
    NoMatchedMessage,
    OffsetIllegal,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PullStatusLookupError {
    #[error("Name is null")]
    NullName,
    #[error("No enum constant org.apache.rocketmq.client.consumer.PullStatus.{0}")]
    UnknownName(String),
}

impl PullStatus {
    pub const fn values() -> [Self; 4] {
        [
            Self::Found,
            Self::NoNewMessage,
            Self::NoMatchedMessage,
            Self::OffsetIllegal,
        ]
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, PullStatusLookupError> {
        match name {
            None => Err(PullStatusLookupError::NullName),
            Some("FOUND") => Ok(Self::Found),
            Some("NO_NEW_MSG") => Ok(Self::NoNewMessage),
            Some("NO_MATCHED_MSG") => Ok(Self::NoMatchedMessage),
            Some("OFFSET_ILLEGAL") => Ok(Self::OffsetIllegal),
            Some(other) => Err(PullStatusLookupError::UnknownName(other.to_owned())),
        }
    }

    pub const fn name(&self) -> &'static str {
        match self {
            Self::Found => "FOUND",
            Self::NoNewMessage => "NO_NEW_MSG",
            Self::NoMatchedMessage => "NO_MATCHED_MSG",
            Self::OffsetIllegal => "OFFSET_ILLEGAL",
        }
    }

    pub const fn ordinal(&self) -> i32 {
        match self {
            Self::Found => 0,
            Self::NoNewMessage => 1,
            Self::NoMatchedMessage => 2,
            Self::OffsetIllegal => 3,
        }
    }

    pub const fn java_compare_to(&self, other: &Self) -> i32 {
        self.ordinal() - other.ordinal()
    }
}

impl std::fmt::Display for PullStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullResultExt {
    pub status: PullStatus,
    pub next_begin_offset: i64,
    pub min_offset: i64,
    pub max_offset: i64,
    pub suggest_which_broker_id: i64,
    pub offset_delta: Option<i64>,
    pub raw_messages: Option<Vec<u8>>,
}

fn pull_request_code(header: &PullMessageRequestHeader) -> i32 {
    if has_lite_pull_flag(header.sys_flag) {
        request::LITE_PULL_MESSAGE
    } else {
        request::PULL_MESSAGE
    }
}

impl MQClientApi {
    pub async fn check_client_in_broker(
        &self,
        addr: &str,
        body: &rocketmq_classic_protocol::body::CheckClientRequestBody,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let code = request::CHECK_CLIENT_CONFIG;
        let reply = self
            .request(
                addr,
                code,
                &rocketmq_classic_protocol::header::LiteSubscriptionCtlRequestHeader,
                Some(
                    serde_json::to_vec(body).expect("check-client body serialization cannot fail"),
                ),
                deadline,
            )
            .await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    pub async fn pull_message(
        &self,
        addr: &str,
        header: &PullMessageRequestHeader,
        deadline: Instant,
    ) -> Result<PullResultExt, RpcError> {
        let code = pull_request_code(header);
        let reply = self.request(addr, code, header, None, deadline).await?;
        self.process_pull_response(addr, code, reply)
    }

    /// Java ASYNC pull represented as an awaited Rust future with transport cleanup grace.
    pub async fn pull_message_future(
        &self,
        addr: &str,
        header: &PullMessageRequestHeader,
        deadline: Instant,
    ) -> Result<PullResultExt, RpcError> {
        let code = pull_request_code(header);
        let reply = self
            .request_future(addr, code, header, None, deadline)
            .await?;
        self.process_pull_response(addr, code, reply)
    }

    /// Search a queue by a signed Java millisecond timestamp.
    ///
    /// A successful broker response may contain `-1` for an empty queue; only
    /// a remoting or broker error is returned as `Err`.
    pub async fn search_offset(
        &self,
        addr: &str,
        header: &SearchOffsetRequestHeader,
        deadline: Instant,
    ) -> Result<i64, RpcError> {
        let code = request::SEARCH_OFFSET_BY_TIMESTAMP;
        let reply = self.request(addr, code, header, None, deadline).await?;
        let reply = classify(reply, code, addr)?;
        Ok(decode_header::<SearchOffsetResponseHeader>(&reply, code, addr)?.offset)
    }

    pub async fn get_max_offset(
        &self,
        addr: &str,
        header: &GetMaxOffsetRequestHeader,
        deadline: Instant,
    ) -> Result<i64, RpcError> {
        let code = request::GET_MAX_OFFSET;
        let reply = self.request(addr, code, header, None, deadline).await?;
        let reply = classify(reply, code, addr)?;
        Ok(decode_header::<GetMaxOffsetResponseHeader>(&reply, code, addr)?.offset)
    }

    pub async fn get_min_offset(
        &self,
        addr: &str,
        header: &GetMinOffsetRequestHeader,
        deadline: Instant,
    ) -> Result<i64, RpcError> {
        let code = request::GET_MIN_OFFSET;
        let reply = self.request(addr, code, header, None, deadline).await?;
        let reply = classify(reply, code, addr)?;
        Ok(decode_header::<GetMinOffsetResponseHeader>(&reply, code, addr)?.offset)
    }

    pub async fn get_earliest_msg_storetime(
        &self,
        addr: &str,
        header: &GetEarliestMsgStoretimeRequestHeader,
        deadline: Instant,
    ) -> Result<i64, RpcError> {
        let code = request::GET_EARLIEST_MSG_STORETIME;
        let reply = self.request(addr, code, header, None, deadline).await?;
        let reply = classify(reply, code, addr)?;
        Ok(decode_header::<GetEarliestMsgStoretimeResponseHeader>(&reply, code, addr)?.timestamp)
    }

    fn process_pull_response(
        &self,
        addr: &str,
        code: i32,
        reply: rocketmq_classic_remoting::RemotingCommand,
    ) -> Result<PullResultExt, RpcError> {
        let status = match reply.code {
            response::SUCCESS => PullStatus::Found,
            response::PULL_NOT_FOUND => PullStatus::NoNewMessage,
            response::PULL_RETRY_IMMEDIATELY => PullStatus::NoMatchedMessage,
            response::PULL_OFFSET_MOVED => PullStatus::OffsetIllegal,
            _ => return classify(reply, code, addr).map(|_| unreachable!()),
        };
        let fields: PullMessageResponseHeader = decode_header(&reply, code, addr)?;
        Ok(PullResultExt {
            status,
            next_begin_offset: fields.next_begin_offset,
            min_offset: fields.min_offset,
            max_offset: fields.max_offset,
            suggest_which_broker_id: fields.suggest_which_broker_id,
            offset_delta: fields.offset_delta,
            raw_messages: reply.body,
        })
    }

    /// Java raises OffsetNotFoundException for QUERY_NOT_FOUND; preserve its code and remark.
    pub async fn query_consumer_offset(
        &self,
        addr: &str,
        header: &QueryConsumerOffsetRequestHeader,
        deadline: Instant,
    ) -> Result<Option<i64>, RpcError> {
        let code = request::QUERY_CONSUMER_OFFSET;
        let reply = self.request(addr, code, header, None, deadline).await?;
        if reply.code == response::QUERY_NOT_FOUND {
            return Ok(None);
        }
        let reply = classify(reply, code, addr)?;
        let fields: QueryConsumerOffsetResponseHeader = decode_header(&reply, code, addr)?;
        Ok(Some(fields.offset))
    }

    pub async fn update_consumer_offset(
        &self,
        addr: &str,
        header: &UpdateConsumerOffsetRequestHeader,
        oneway: bool,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let code = request::UPDATE_CONSUMER_OFFSET;
        if oneway {
            self.oneway(addr, code, header, None, deadline).await
        } else {
            let reply = self.request(addr, code, header, None, deadline).await?;
            classify(reply, code, addr)?;
            Ok(())
        }
    }

    pub async fn get_consumer_list_by_group(
        &self,
        addr: &str,
        consumer_group: &str,
        deadline: Instant,
    ) -> Result<Option<Vec<Option<String>>>, RpcError> {
        self.get_consumer_list_by_group_nullable(addr, Some(consumer_group), deadline)
            .await
    }

    pub async fn get_consumer_list_by_group_nullable(
        &self,
        addr: &str,
        consumer_group: Option<&str>,
        deadline: Instant,
    ) -> Result<Option<Vec<Option<String>>>, RpcError> {
        let code = request::GET_CONSUMER_LIST_BY_GROUP;
        let mut fields = HeaderMap::new();
        if let Some(consumer_group) = consumer_group {
            fields.insert("consumerGroup".into(), consumer_group.to_owned());
        }
        let reply = self
            .request_fields_with_mode(addr, code, fields, None, deadline, true)
            .await?;
        let reply = classify(reply, code, addr)?;
        if reply.body.is_none() {
            return Err(RpcError::Response {
                request_code: code,
                peer: addr.to_owned(),
                code: reply.code,
                remark: reply.remark,
            });
        }
        let body: ConsumerListByGroupResponseBody = decode_body(&reply, code, addr)?;
        Ok(body
            .consumer_id_list
            .map(|consumer_ids| consumer_ids.snapshot()))
    }

    pub async fn consumer_send_message_back(
        &self,
        addr: &str,
        header: &ConsumerSendMsgBackRequestHeader,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let code = request::CONSUMER_SEND_MSG_BACK;
        let reply = self.request(addr, code, header, None, deadline).await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    pub async fn lock_batch_mq(
        &self,
        addr: &str,
        body: &LockBatchRequestBody,
        deadline: Instant,
    ) -> Result<Option<std::collections::HashSet<Option<MessageQueue>>>, RpcError> {
        let code = request::LOCK_BATCH_MQ;
        let payload = serde_json::to_vec(body).expect("lock body serialization cannot fail");
        let reply = self
            .request(
                addr,
                code,
                &LockBatchMqRequestHeader::default(),
                Some(payload),
                deadline,
            )
            .await?;
        let reply = classify(reply, code, addr)?;
        let body: LockBatchResponseBody = decode_body(&reply, code, addr)?;
        Ok(body.lock_okmq_set.map(|queues| {
            queues.with(|queues| {
                queues
                    .iter()
                    .map(|queue| queue.as_ref().map(|queue| queue.snapshot()))
                    .collect()
            })
        }))
    }

    pub async fn unlock_batch_mq(
        &self,
        addr: &str,
        body: &UnlockBatchRequestBody,
        oneway: bool,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let code = request::UNLOCK_BATCH_MQ;
        let payload = serde_json::to_vec(body).expect("unlock body serialization cannot fail");
        if oneway {
            self.oneway(
                addr,
                code,
                &LockBatchMqRequestHeader::default(),
                Some(payload),
                deadline,
            )
            .await
        } else {
            let reply = self
                .request(
                    addr,
                    code,
                    &LockBatchMqRequestHeader::default(),
                    Some(payload),
                    deadline,
                )
                .await?;
            classify(reply, code, addr)?;
            Ok(())
        }
    }
}
