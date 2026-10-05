use bytes::BytesMut;
use rocketmq_classic_core::{config::ClientConfig, runtime::ClientRuntimeManager};
use rocketmq_classic_protocol::{
    codes::{LanguageCode, request},
    header::UpdateConsumerOffsetRequestHeader,
    heartbeat::HeartbeatData,
};
use rocketmq_classic_remoting::{
    RemotingCommand,
    client::{RpcHook, RpcHookError},
    codec::frame::{self, DEFAULT_MAX_FRAME_SIZE},
};
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{Duration, Instant},
};

struct Signer;
impl RpcHook for Signer {
    fn before_request_mut_result(
        &self,
        _: &str,
        command: &mut RemotingCommand,
    ) -> Result<(), RpcHookError> {
        let fields = command.ext_fields.as_mut().unwrap();
        assert_eq!(fields["ns"], "v2");
        assert_eq!(fields["nsd"], "true");
        assert_eq!(fields["ReqT"], "0");
        fields.insert("signature".into(), "ns=v2;ReqT=0".into());
        Ok(())
    }
}

#[tokio::test]
async fn runtime_hooks_reach_nameserver_heartbeat_v1_v2_and_oneway_with_source_specific_language() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let mut config = ClientConfig::new("192.0.2.6");
    config.namesrv_addr = Some(address.clone());
    config.namespace_v2 = Some("v2".into());
    config.enable_stream_request_type = true;
    config.language = LanguageCode::Rust;
    let runtime = ClientRuntimeManager::new()
        .get_or_create_with_hook(&config, Some(Arc::new(Signer)))
        .await
        .unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        for code in [
            request::GET_ROUTEINFO_BY_TOPIC,
            request::HEART_BEAT,
            request::HEART_BEAT,
            request::UPDATE_CONSUMER_OFFSET,
        ] {
            let mut length = [0; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut bytes = BytesMut::from(&length[..]);
            bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut bytes[4..]).await.unwrap();
            let command = frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
                .unwrap()
                .unwrap();
            assert_eq!(command.code, code);
            assert_eq!(
                command.language,
                if code == request::HEART_BEAT {
                    LanguageCode::Rust
                } else {
                    LanguageCode::Java
                }
            );
            let fields = command.ext_fields.as_ref().unwrap();
            assert_eq!(fields["signature"], "ns=v2;ReqT=0");
            if command.is_oneway() {
                continue;
            }
            let mut reply = RemotingCommand {
                opaque: command.opaque,
                version: 551,
                body: (code == request::GET_ROUTEINFO_BY_TOPIC).then(|| {
                    br#"{"queueDatas":[],"brokerDatas":[],"filterServerTable":{}}"#.to_vec()
                }),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
                .await
                .unwrap();
        }
    });
    runtime.remoting().start().unwrap();
    runtime
        .update_topic_route("topic", Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    runtime
        .api()
        .send_heartbeat(
            &address,
            &HeartbeatData::default(),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    runtime
        .api()
        .send_heartbeat_v2(
            &address,
            &HeartbeatData::default(),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    runtime
        .api()
        .update_consumer_offset(
            &address,
            &UpdateConsumerOffsetRequestHeader::default(),
            true,
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    peer.await.unwrap();
    runtime.remoting().shutdown().await;
}
