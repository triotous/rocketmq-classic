use rocketmq_classic_common::{message::MessageQueue, sysflag::pull::has_lite_pull_flag};
use rocketmq_classic_protocol::{
    body::{
        ConsumerListByGroupResponseBody, LockBatchRequestBody, LockBatchResponseBody,
        QueryConsumeTimeSpanBody, QueueTimeSpan, UnlockBatchRequestBody,
    },
    codes::{request, response},
    header::{
        ConsumerSendMsgBackRequestHeader, GetEarliestMsgStoretimeRequestHeader,
        GetEarliestMsgStoretimeResponseHeader, GetMaxOffsetRequestHeader,
        GetMaxOffsetResponseHeader, GetMinOffsetRequestHeader, GetMinOffsetResponseHeader,
        HeaderMap, LockBatchMqRequestHeader, PullMessageRequestHeader, PullMessageResponseHeader,
        QueryConsumeTimeSpanRequestHeader, QueryConsumerOffsetRequestHeader,
        QueryConsumerOffsetResponseHeader, SearchOffsetRequestHeader, SearchOffsetResponseHeader,
        UpdateConsumerOffsetRequestHeader,
    },
};
use tokio::time::Instant;

use super::{MQClientApi, classify, decode_body, decode_header, read_shared_client_config};
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
    /// Queries one Broker for consume timestamp ranges. The caller owns route
    /// iteration and result concatenation, matching Java's consumer wrapper.
    pub async fn query_consume_time_span(
        &self,
        addr: &str,
        header: &QueryConsumeTimeSpanRequestHeader,
        deadline: Instant,
    ) -> Result<Option<Vec<Option<QueueTimeSpan>>>, RpcError> {
        let code = request::QUERY_CONSUME_TIME_SPAN;
        let vip_enabled = self
            .config
            .as_ref()
            .is_some_and(|config| read_shared_client_config(config).vip_channel_enabled);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            }
        })?;
        let reply = self
            .request(&broker_addr, code, header, None, deadline)
            .await?;
        let reply = classify(reply, code, &broker_addr)?;
        let body: QueryConsumeTimeSpanBody = decode_body(&reply, code, &broker_addr)?;
        Ok(body.consume_time_span_set)
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_classic_protocol::codes::response;
    use rocketmq_classic_protocol::header::QueryConsumeTimeSpanRequestHeader;
    use rocketmq_classic_remoting::{
        RemotingCommand,
        client::{RemotingClient, TransportMode},
        codec::frame::{self, DEFAULT_MAX_FRAME_SIZE},
    };
    use std::time::Duration;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    async fn read_command(stream: &mut TcpStream) -> RemotingCommand {
        let mut length = [0; 4];
        stream.read_exact(&mut length).await.unwrap();
        let mut bytes = bytes::BytesMut::from(&length[..]);
        bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
        stream.read_exact(&mut bytes[4..]).await.unwrap();
        frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
            .unwrap()
            .unwrap()
    }

    async fn reply(
        stream: &mut TcpStream,
        request: &RemotingCommand,
        code: i32,
        remark: Option<String>,
        body: Option<Vec<u8>>,
    ) {
        let mut response = RemotingCommand {
            code,
            opaque: request.opaque,
            remark,
            body,
            ..Default::default()
        };
        response.mark_response();
        stream
            .write_all(&frame::encode(&response, DEFAULT_MAX_FRAME_SIZE).unwrap())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn query_consume_time_span_matches_java_invoked_fixture() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/lookup.json")).unwrap();
        let case = &fixture["queryConsumeTimeSpanApiCase"];
        let request_case = case["requests"][0].clone();
        let success_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let success_address = success_listener.local_addr().unwrap().to_string();
        let success_body = case["responseBody"].as_str().unwrap().as_bytes().to_vec();
        let success_peer = tokio::spawn(async move {
            let (mut stream, _) = success_listener.accept().await.unwrap();
            let incoming = read_command(&mut stream).await;
            assert_eq!(incoming.code, request::QUERY_CONSUME_TIME_SPAN);
            assert!(incoming.body.is_none());
            let fields = incoming.ext_fields.as_ref().unwrap();
            assert_eq!(fields.get("topic").map(String::as_str), Some("orders"));
            assert_eq!(fields.get("group").map(String::as_str), Some("consumer-a"));
            assert_eq!(
                request_case["requestCode"].as_i64(),
                Some(i64::from(incoming.code))
            );
            assert_eq!(request_case["topic"], fields["topic"]);
            assert_eq!(request_case["group"], fields["group"]);
            reply(
                &mut stream,
                &incoming,
                response::SUCCESS,
                None,
                Some(success_body),
            )
            .await;
        });

        let error_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let error_address = error_listener.local_addr().unwrap().to_string();
        let error_peer = tokio::spawn(async move {
            let (mut stream, _) = error_listener.accept().await.unwrap();
            let incoming = read_command(&mut stream).await;
            assert_eq!(incoming.code, request::QUERY_CONSUME_TIME_SPAN);
            reply(
                &mut stream,
                &incoming,
                response::TOPIC_NOT_EXIST,
                Some("topic not exist".into()),
                None,
            )
            .await;
        });

        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let api = MQClientApi::new(remoting.clone());
        let header = QueryConsumeTimeSpanRequestHeader {
            topic: "orders".into(),
            group: "consumer-a".into(),
        };
        let spans = api
            .query_consume_time_span(
                &success_address,
                &header,
                Instant::now() + Duration::from_secs(3),
            )
            .await
            .unwrap()
            .unwrap();
        success_peer.await.unwrap();
        assert_eq!(spans.len(), 2);
        for (span, expected) in spans.iter().zip(case["successRows"].as_array().unwrap()) {
            let span = span.as_ref().unwrap();
            let queue = span.message_queue.as_ref().unwrap();
            assert_eq!(queue.topic(), expected["topic"].as_str());
            assert_eq!(queue.broker_name(), expected["brokerName"].as_str());
            assert_eq!(
                queue.queue_id(),
                expected["queueId"].as_i64().unwrap() as i32
            );
            assert_eq!(
                span.min_time_stamp,
                expected["minTimeStamp"].as_i64().unwrap()
            );
            assert_eq!(
                span.max_time_stamp,
                expected["maxTimeStamp"].as_i64().unwrap()
            );
            assert_eq!(
                span.consume_time_stamp,
                expected["consumeTimeStamp"].as_i64().unwrap()
            );
            assert_eq!(span.delay_time, expected["delayTime"].as_i64().unwrap());
        }

        let error = api
            .query_consume_time_span(
                &error_address,
                &header,
                Instant::now() + Duration::from_secs(3),
            )
            .await
            .unwrap_err();
        error_peer.await.unwrap();
        assert!(matches!(
            error,
            RpcError::Response {
                request_code: request::QUERY_CONSUME_TIME_SPAN,
                peer,
                code: response::TOPIC_NOT_EXIST,
                remark: Some(remark),
            } if peer == error_address && remark == case["topicNotExistMessage"].as_str().unwrap()
        ));
        remoting.shutdown().await;
    }
}
