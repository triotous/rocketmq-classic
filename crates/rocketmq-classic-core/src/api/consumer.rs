use rocketmq_classic_common::{message::MessageQueue, sysflag::pull::has_lite_pull_flag};
use rocketmq_classic_protocol::{
    body::{
        ConsumerListByGroupResponseBody, LockBatchRequestBody, LockBatchResponseBody,
        UnlockBatchRequestBody,
    },
    codes::{request, response},
    header::{
        ConsumerSendMsgBackRequestHeader, GetConsumerListByGroupRequestHeader,
        LockBatchMqRequestHeader, PullMessageRequestHeader, PullMessageResponseHeader,
        QueryConsumerOffsetRequestHeader, QueryConsumerOffsetResponseHeader,
        UpdateConsumerOffsetRequestHeader,
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
    ) -> Result<Option<Vec<String>>, RpcError> {
        let code = request::GET_CONSUMER_LIST_BY_GROUP;
        let reply = self
            .request(
                addr,
                code,
                &GetConsumerListByGroupRequestHeader {
                    consumer_group: consumer_group.into(),
                },
                None,
                deadline,
            )
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
        Ok(body.consumer_id_list)
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
    ) -> Result<Vec<MessageQueue>, RpcError> {
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
        Ok(body.lock_okmq_set)
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
