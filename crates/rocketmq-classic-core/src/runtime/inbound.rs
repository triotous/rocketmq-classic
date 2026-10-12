use super::{ClientRuntime, ConsumerOperation, RuntimeState};
use rocketmq_classic_common::message::{
    constants::{PROPERTY_CORRELATION_ID, PROPERTY_PRODUCER_GROUP},
    decode_properties,
};
use rocketmq_classic_protocol::codes::{request, response};
use rocketmq_classic_remoting::{RemotingCommand, client::RequestProcessor};
use std::sync::{Arc, OnceLock, Weak};

pub(super) const CODES: [i32; 8] = [
    request::CHECK_TRANSACTION_STATE,
    request::NOTIFY_CONSUMER_IDS_CHANGED,
    request::RESET_CONSUMER_CLIENT_OFFSET,
    request::GET_CONSUMER_STATUS_FROM_CLIENT,
    request::GET_CONSUMER_RUNNING_INFO,
    request::CONSUME_MESSAGE_DIRECTLY,
    request::PUSH_REPLY_MESSAGE_TO_CLIENT,
    request::NOTIFY_UNSUBSCRIBE_LITE,
];

#[derive(Default)]
pub(super) struct RuntimeProcessor {
    runtime: OnceLock<Weak<ClientRuntime>>,
}

impl RuntimeProcessor {
    pub fn bind(&self, runtime: &Arc<ClientRuntime>) {
        let _ = self.runtime.set(Arc::downgrade(runtime));
    }

    fn dispatch(
        &self,
        remote: &str,
        command: RemotingCommand,
    ) -> Result<Option<RemotingCommand>, String> {
        let Some(runtime) = self.runtime.get().and_then(Weak::upgrade) else {
            return Ok(Some(error(
                response::SYSTEM_ERROR,
                "client runtime is unavailable",
            )));
        };
        if command.code == request::NOTIFY_CONSUMER_IDS_CHANGED {
            // Java decodes this header for logging only, then wakes the
            // runtime-wide rebalance service and returns null. The group does
            // not constrain which consumers are rebalanced. Java does this
            // before the service starts as well; the wakeup remains sticky.
            runtime.rebalance_immediately();
            return Ok(None);
        }
        if runtime.state() != RuntimeState::Running {
            return Ok(Some(error(
                response::SYSTEM_ERROR,
                "client runtime is not running",
            )));
        }
        let groups = runtime
            .groups
            .snapshot()
            .map_err(|error| error.to_string())?;
        let fields = command.ext_fields.as_ref();
        let field = |key: &str| {
            fields
                .and_then(|fields| fields.get(key))
                .map(String::as_str)
        };
        let selected = match command.code {
            request::CHECK_TRANSACTION_STATE => {
                let producers = groups
                    .producers
                    .into_iter()
                    .filter_map(|(group, producer)| {
                        producer
                            .transaction_processor()
                            .map(|processor| (group, processor))
                    })
                    .collect::<Vec<_>>();
                let properties =
                    match stored_properties(command.body.as_deref().unwrap_or_default()) {
                        Ok(properties) => properties,
                        Err(_) => return Ok(None),
                    };
                producers
                    .into_iter()
                    .find(|(group, _)| properties.get(PROPERTY_PRODUCER_GROUP) == Some(group))
                    .map(|(_, processor)| processor)
            }
            request::PUSH_REPLY_MESSAGE_TO_CLIENT => {
                let properties = decode_properties(field("properties").unwrap_or_default());
                let Some(correlation) = properties.get(PROPERTY_CORRELATION_ID) else {
                    return Ok(Some(unsupported(command.code, "Phase 15")));
                };
                groups
                    .producers
                    .into_iter()
                    .find_map(|(_, producer)| producer.reply_processor(correlation))
            }
            request::NOTIFY_UNSUBSCRIBE_LITE => {
                // MQClientAPIImpl registers this code, but Java's
                // ClientRemotingProcessor switch has no branch and returns null.
                return Ok(None);
            }
            code => {
                let (operation, key, phase) = match code {
                    request::RESET_CONSUMER_CLIENT_OFFSET => {
                        (ConsumerOperation::ResetOffset, "group", "Phase 17")
                    }
                    request::GET_CONSUMER_STATUS_FROM_CLIENT => {
                        (ConsumerOperation::ConsumerStatus, "group", "Phase 17")
                    }
                    request::GET_CONSUMER_RUNNING_INFO => {
                        (ConsumerOperation::RunningInfo, "consumerGroup", "Phase 17")
                    }
                    request::CONSUME_MESSAGE_DIRECTLY => (
                        ConsumerOperation::ConsumeDirectly,
                        "consumerGroup",
                        "Phase 17",
                    ),
                    _ => return Ok(Some(unsupported(code, "no Java client owner"))),
                };
                let Some(group) = field(key) else {
                    return Ok(Some(unsupported(code, phase)));
                };
                let Some(consumer) = runtime
                    .select_consumer(group)
                    .map_err(|error| error.to_string())?
                else {
                    return Ok(Some(unsupported(code, phase)));
                };
                consumer.inbound_processor(operation)
            }
        };
        match selected {
            Some(processor) => processor.process_with_context(remote, command),
            None if command.code == request::CHECK_TRANSACTION_STATE => Ok(None),
            None => Ok(Some(unsupported(
                command.code,
                match command.code {
                    request::CHECK_TRANSACTION_STATE => "Phase 14",
                    request::PUSH_REPLY_MESSAGE_TO_CLIENT => "Phase 15",
                    _ => "Phase 17",
                },
            ))),
        }
    }
}

impl RequestProcessor for RuntimeProcessor {
    fn process(&self, command: RemotingCommand) -> Result<Option<RemotingCommand>, String> {
        self.dispatch("", command)
    }
    fn process_with_context(
        &self,
        remote: &str,
        command: RemotingCommand,
    ) -> Result<Option<RemotingCommand>, String> {
        self.dispatch(remote, command)
    }
}

fn error(code: i32, remark: &str) -> RemotingCommand {
    RemotingCommand {
        code,
        remark: Some(remark.into()),
        ..Default::default()
    }
}
fn unsupported(code: i32, phase: &str) -> RemotingCommand {
    error(
        response::REQUEST_CODE_NOT_SUPPORTED,
        &format!("client request {code} has no active handler ({phase})"),
    )
}

/// Routing-only metadata extraction from MessageDecoder's V1/V2 layout. Full
/// message decoding/decompression belongs to Phase 8/14; no payload is copied.
fn stored_properties(
    body: &[u8],
) -> Result<rocketmq_classic_common::message::MessageProperties, String> {
    fn bytes(body: &[u8], offset: usize, length: usize) -> Result<&[u8], String> {
        offset
            .checked_add(length)
            .and_then(|end| body.get(offset..end))
            .ok_or_else(|| "malformed stored message metadata".into())
    }
    fn int(body: &[u8], offset: usize) -> Result<i32, String> {
        Ok(i32::from_be_bytes(
            bytes(body, offset, 4)?.try_into().unwrap(),
        ))
    }
    let magic = int(body, 4)?;
    let flag = int(body, 36)?;
    let body_position =
        84 + if flag & (1 << 4) != 0 { 12 } else { 0 } + if flag & (1 << 5) != 0 { 12 } else { 0 };
    let length =
        usize::try_from(int(body, body_position)?).map_err(|_| "negative stored body length")?;
    let topic_position = body_position
        .checked_add(4)
        .and_then(|offset| offset.checked_add(length))
        .ok_or("stored body length overflow")?;
    let (topic_length, width) = match magic {
        -626843481 => (
            usize::try_from(bytes(body, topic_position, 1)?[0] as i8)
                .map_err(|_| "negative V1 topic length")?,
            1,
        ),
        -626843477 => (
            usize::try_from(i16::from_be_bytes(
                bytes(body, topic_position, 2)?.try_into().unwrap(),
            ))
            .map_err(|_| "negative V2 topic length")?,
            2,
        ),
        _ => return Err("invalid stored message magic code".into()),
    };
    let properties_position = topic_position
        .checked_add(width)
        .and_then(|offset| offset.checked_add(topic_length))
        .ok_or("stored topic length overflow")?;
    let length = i16::from_be_bytes(bytes(body, properties_position, 2)?.try_into().unwrap()).max(0)
        as usize;
    Ok(decode_properties(&String::from_utf8_lossy(bytes(
        body,
        properties_position + 2,
        length,
    )?)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::ClientConfig,
        runtime::{ClientRuntimeManager, ConsumerRuntime, GroupLease, ProducerRuntime},
    };
    use base64::Engine;
    use rocketmq_classic_protocol::heartbeat::ConsumerData;
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    fn fixtures() -> serde_json::Value {
        serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/inbound-routing.json"
        ))
        .unwrap()
    }
    fn body(case: &serde_json::Value) -> Vec<u8> {
        base64::engine::general_purpose::STANDARD
            .decode(case["body"].as_str().unwrap())
            .unwrap()
    }

    #[test]
    fn routing_properties_match_java_v1_v2_and_all_ipv4_ipv6_layouts() {
        for case in fixtures()["cases"].as_array().unwrap() {
            let body = body(case);
            assert_eq!(
                serde_json::to_value(stored_properties(&body).unwrap()).unwrap(),
                case["properties"],
                "{}",
                case["case"]
            );
            for length in 0..body.len() {
                assert!(
                    stored_properties(&body[..length]).is_err(),
                    "{} prefix {length}",
                    case["case"]
                );
            }
        }
    }

    #[test]
    fn hostile_metadata_lengths_and_magic_are_rejected_without_payload_allocation() {
        let mut body = body(&fixtures()["cases"][0]);
        body[84..88].copy_from_slice(&i32::MAX.to_be_bytes());
        assert!(stored_properties(&body).is_err());
        body[84..88].copy_from_slice(&(-1i32).to_be_bytes());
        assert!(stored_properties(&body).is_err());
        body[4..8].copy_from_slice(&0i32.to_be_bytes());
        assert!(stored_properties(&body).is_err());
    }

    struct Handler {
        owner: String,
        runtime: Weak<ClientRuntime>,
        calls: Mutex<Vec<(String, i32)>>,
    }
    impl RequestProcessor for Handler {
        fn process(&self, _: RemotingCommand) -> Result<Option<RemotingCommand>, String> {
            panic!("channel context must be passed");
        }
        fn process_with_context(
            &self,
            remote: &str,
            command: RemotingCommand,
        ) -> Result<Option<RemotingCommand>, String> {
            let runtime = self.runtime.upgrade().unwrap();
            runtime.route_snapshot().unwrap(); // Callback may reenter cache and group registry.
            if self.owner.starts_with("producer") {
                assert!(runtime.select_producer(&self.owner).unwrap().is_some());
            } else {
                assert!(runtime.select_consumer(&self.owner).unwrap().is_some());
            }
            self.calls
                .lock()
                .unwrap()
                .push((remote.to_owned(), command.code));
            Ok(Some(RemotingCommand {
                code: 0,
                remark: Some(self.owner.clone()),
                ..Default::default()
            }))
        }
    }
    struct Producer {
        handler: Arc<Handler>,
        correlation: &'static str,
    }
    impl ProducerRuntime for Producer {
        fn transaction_processor(&self) -> Option<Arc<dyn RequestProcessor>> {
            Some(self.handler.clone())
        }
        fn reply_processor(&self, correlation: &str) -> Option<Arc<dyn RequestProcessor>> {
            (correlation == self.correlation)
                .then(|| self.handler.clone() as Arc<dyn RequestProcessor>)
        }
    }
    struct NoInboundProducer;
    impl ProducerRuntime for NoInboundProducer {}
    struct Consumer {
        handler: Arc<Handler>,
    }
    impl ConsumerRuntime for Consumer {
        fn heartbeat_data(&self) -> ConsumerData {
            ConsumerData {
                group_name: Some(self.handler.owner.clone()),
                ..Default::default()
            }
        }
        fn inbound_processor(&self, _: ConsumerOperation) -> Option<Arc<dyn RequestProcessor>> {
            Some(self.handler.clone())
        }
    }
    struct NoInboundConsumer;
    impl ConsumerRuntime for NoInboundConsumer {
        fn heartbeat_data(&self) -> ConsumerData {
            ConsumerData {
                group_name: Some("consumer-no-inbound".into()),
                ..Default::default()
            }
        }
    }
    struct RebalancingConsumer(AtomicUsize);
    impl ConsumerRuntime for RebalancingConsumer {
        fn heartbeat_data(&self) -> ConsumerData {
            ConsumerData::default()
        }
        fn try_rebalance(&self) -> Option<Pin<Box<dyn Future<Output = bool> + Send + '_>>> {
            Some(Box::pin(async {
                self.0.fetch_add(1, Ordering::SeqCst);
                true
            }))
        }
    }
    fn handler(runtime: &Arc<ClientRuntime>, owner: &str) -> Arc<Handler> {
        Arc::new(Handler {
            owner: owner.into(),
            runtime: Arc::downgrade(runtime),
            calls: Mutex::default(),
        })
    }
    async fn runtime(id: &str) -> Arc<ClientRuntime> {
        let mut config = ClientConfig::new(id);
        config.namesrv_addr = Some(String::new());
        let runtime = ClientRuntimeManager::new()
            .get_or_create(&config)
            .await
            .unwrap();
        runtime.start().await.unwrap();
        runtime
    }
    fn notify_request() -> RemotingCommand {
        RemotingCommand {
            code: request::NOTIFY_CONSUMER_IDS_CHANGED,
            ..Default::default()
        }
    }
    fn inactive_request() -> RemotingCommand {
        RemotingCommand {
            code: request::RESET_CONSUMER_CLIENT_OFFSET,
            ..Default::default()
        }
    }
    fn dispatch(
        runtime: &Arc<ClientRuntime>,
        code: i32,
        fields: &[(&str, &str)],
        body: Option<Vec<u8>>,
    ) -> RemotingCommand {
        runtime
            .inbound
            .process_with_context(
                "broker:10911",
                RemotingCommand {
                    code,
                    ext_fields: Some(
                        fields
                            .iter()
                            .map(|(key, value)| (key.to_string(), value.to_string()))
                            .collect(),
                    ),
                    body,
                    ..Default::default()
                },
            )
            .unwrap()
            .unwrap()
    }
    fn dispatch_optional(
        runtime: &Arc<ClientRuntime>,
        fields: &[(&str, &str)],
    ) -> Option<RemotingCommand> {
        runtime
            .inbound
            .process_with_context(
                "broker:10911",
                RemotingCommand {
                    code: request::NOTIFY_CONSUMER_IDS_CHANGED,
                    ext_fields: Some(
                        fields
                            .iter()
                            .map(|(key, value)| (key.to_string(), value.to_string()))
                            .collect(),
                    ),
                    ..Default::default()
                },
            )
            .unwrap()
    }
    async fn settle() {
        for _ in 0..30 {
            tokio::task::yield_now().await;
        }
    }
    async fn unregister(runtime: &Arc<ClientRuntime>, lease: &GroupLease, producer: bool) {
        if producer {
            runtime
                .unregister_producer(lease, tokio::time::Instant::now())
                .await
                .unwrap();
        } else {
            runtime
                .unregister_consumer(lease, tokio::time::Instant::now())
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn unbound_and_non_running_feature_slots_return_system_error_but_code_40_is_silent() {
        let unbound = RuntimeProcessor::default()
            .process(inactive_request())
            .unwrap()
            .unwrap();
        assert_eq!(unbound.code, response::SYSTEM_ERROR);
        assert_eq!(
            unbound.remark.as_deref(),
            Some("client runtime is unavailable")
        );

        let mut config = ClientConfig::new("inbound-not-running");
        config.namesrv_addr = Some(String::new());
        let runtime = ClientRuntimeManager::new()
            .get_or_create(&config)
            .await
            .unwrap();
        assert!(runtime.inbound.process(notify_request()).unwrap().is_none());
        let created = runtime
            .inbound
            .process(inactive_request())
            .unwrap()
            .unwrap();
        assert_eq!(created.code, response::SYSTEM_ERROR);
        assert_eq!(
            created.remark.as_deref(),
            Some("client runtime is not running")
        );

        runtime.start().await.unwrap();
        assert!(runtime.shutdown().await.unwrap());
        assert!(runtime.inbound.process(notify_request()).unwrap().is_none());
        let stopped = runtime
            .inbound
            .process(inactive_request())
            .unwrap()
            .unwrap();
        assert_eq!(stopped.code, response::SYSTEM_ERROR);
        assert_eq!(
            stopped.remark.as_deref(),
            Some("client runtime is not running")
        );

        let mut failed_config = ClientConfig::new("inbound-failed");
        failed_config.namesrv_addr = Some(String::new());
        failed_config.poll_name_server_interval = 0;
        let failed = ClientRuntimeManager::new()
            .get_or_create(&failed_config)
            .await
            .unwrap();
        assert!(failed.start().await.is_err());
        assert_eq!(failed.state(), RuntimeState::Failed);
        assert!(failed.inbound.process(notify_request()).unwrap().is_none());
        let failure = failed.inbound.process(inactive_request()).unwrap().unwrap();
        assert_eq!(failure.code, response::SYSTEM_ERROR);
        assert_eq!(
            failure.remark.as_deref(),
            Some("client runtime is not running")
        );
    }

    #[tokio::test]
    async fn transaction_routes_actual_body_group_not_header_and_reply_uses_correlation_not_responder()
     {
        let runtime = runtime("transaction-slots").await;
        let first = Arc::new(Producer {
            handler: handler(&runtime, "producer-a"),
            correlation: "first",
        });
        let second = Arc::new(Producer {
            handler: handler(&runtime, "producer-b"),
            correlation: "second",
        });
        let first_lease = runtime
            .register_producer("producer-a", first.clone())
            .await
            .unwrap();
        let second_lease = runtime
            .register_producer("producer-b", second.clone())
            .await
            .unwrap();
        for case in fixtures()["cases"].as_array().unwrap() {
            let result = dispatch(
                &runtime,
                request::CHECK_TRANSACTION_STATE,
                &[("producerGroup", "forged-header-group")],
                Some(body(case)),
            );
            assert_eq!(result.remark.as_deref(), case["group"].as_str());
        }
        let reply = dispatch(
            &runtime,
            request::PUSH_REPLY_MESSAGE_TO_CLIENT,
            &[
                ("producerGroup", "producer-b"),
                ("properties", "CORRELATION_ID\u{1}first\u{2}"),
            ],
            Some(vec![0, 255]),
        );
        assert_eq!(reply.remark.as_deref(), Some("producer-a"));
        assert!(
            first
                .handler
                .calls
                .lock()
                .unwrap()
                .iter()
                .all(|(remote, _)| remote == "broker:10911")
        );
        unregister(&runtime, &first_lease, true).await;
        assert_eq!(
            dispatch(
                &runtime,
                request::PUSH_REPLY_MESSAGE_TO_CLIENT,
                &[("properties", "CORRELATION_ID\u{1}first\u{2}")],
                None
            )
            .code,
            response::REQUEST_CODE_NOT_SUPPORTED
        );
        unregister(&runtime, &second_lease, true).await;
        assert!(runtime.shutdown().await.unwrap());
    }

    #[tokio::test]
    async fn consumer_slots_route_group_with_registry_reentry_and_do_not_cross_runtime_identity() {
        let runtime = runtime("consumer-slots").await;
        let other = self::runtime("other-client").await;
        let first = Arc::new(Consumer {
            handler: handler(&runtime, "consumer-a"),
        });
        let second = Arc::new(Consumer {
            handler: handler(&other, "consumer-a"),
        });
        let first_lease = runtime
            .register_consumer("consumer-a", first.clone())
            .await
            .unwrap();
        let second_lease = other
            .register_consumer("consumer-a", second.clone())
            .await
            .unwrap();
        for (code, key) in [
            (request::RESET_CONSUMER_CLIENT_OFFSET, "group"),
            (request::GET_CONSUMER_STATUS_FROM_CLIENT, "group"),
            (request::GET_CONSUMER_RUNNING_INFO, "consumerGroup"),
            (request::CONSUME_MESSAGE_DIRECTLY, "consumerGroup"),
        ] {
            assert_eq!(
                dispatch(&runtime, code, &[(key, "consumer-a")], None)
                    .remark
                    .as_deref(),
                Some("consumer-a")
            );
            assert_eq!(
                dispatch(&runtime, code, &[(key, "missing")], None).code,
                response::REQUEST_CODE_NOT_SUPPORTED
            );
        }
        assert_eq!(first.handler.calls.lock().unwrap().len(), 4);
        assert!(second.handler.calls.lock().unwrap().is_empty());
        unregister(&runtime, &first_lease, false).await;
        assert_eq!(
            dispatch(
                &runtime,
                request::CONSUME_MESSAGE_DIRECTLY,
                &[("consumerGroup", "consumer-a")],
                None
            )
            .code,
            response::REQUEST_CODE_NOT_SUPPORTED
        );
        unregister(&other, &second_lease, false).await;
        assert!(runtime.shutdown().await.unwrap());
        assert!(other.shutdown().await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn consumer_ids_changed_wakes_runtime_wide_rebalance_and_returns_no_response() {
        let java_fixture = fixtures();
        let java_cases = java_fixture["consumerIdsChangedCases"]
            .as_array()
            .expect("invoked Java code 40 cases");
        assert_eq!(java_cases.len(), 2);
        for case in java_cases {
            assert_eq!(case["wakeupBefore"], false);
            assert_eq!(case["wakeupAfter"], true);
            assert_eq!(case["responsePresent"], false);
        }

        let mut config = ClientConfig::new("consumer-ids-changed-runtime-wide");
        config.namesrv_addr = Some(String::new());
        let runtime = ClientRuntimeManager::new()
            .get_or_create(&config)
            .await
            .unwrap();
        let first = Arc::new(RebalancingConsumer(AtomicUsize::new(0)));
        let second = Arc::new(RebalancingConsumer(AtomicUsize::new(0)));
        let first_lease = runtime
            .register_consumer("consumer-a", first.clone())
            .await
            .unwrap();
        let second_lease = runtime
            .register_consumer("consumer-b", second.clone())
            .await
            .unwrap();

        // Java leaves a sticky wakeup before the owned worker exists. Advance
        // the source minimum interval, then start that worker.
        let first_response = dispatch_optional(&runtime, &[("consumerGroup", "consumer-a")]);
        assert_eq!(first_response.is_some(), java_cases[0]["responsePresent"]);
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        settle().await;
        runtime.start().await.unwrap();
        settle().await;
        assert_eq!(first.0.load(Ordering::SeqCst), 1);
        assert_eq!(second.0.load(Ordering::SeqCst), 1);

        // The Java header's consumerGroup is optional and does not route the
        // wakeup to a single registered group.
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        settle().await;
        let second_response = dispatch_optional(&runtime, &[]);
        assert_eq!(second_response.is_some(), java_cases[1]["responsePresent"]);
        settle().await;
        assert_eq!(first.0.load(Ordering::SeqCst), 2);
        assert_eq!(second.0.load(Ordering::SeqCst), 2);

        unregister(&runtime, &first_lease, false).await;
        unregister(&runtime, &second_lease, false).await;
        assert!(runtime.shutdown().await.unwrap());
    }

    #[tokio::test]
    async fn absent_consumer_group_and_handler_capabilities_return_phase_errors() {
        let runtime = runtime("consumer-inactive-slots").await;
        let consumer = Arc::new(NoInboundConsumer);
        let lease = runtime
            .register_consumer("consumer-no-inbound", consumer)
            .await
            .unwrap();
        for (code, key, phase) in [
            (request::RESET_CONSUMER_CLIENT_OFFSET, "group", "Phase 17"),
            (
                request::GET_CONSUMER_STATUS_FROM_CLIENT,
                "group",
                "Phase 17",
            ),
            (
                request::GET_CONSUMER_RUNNING_INFO,
                "consumerGroup",
                "Phase 17",
            ),
            (
                request::CONSUME_MESSAGE_DIRECTLY,
                "consumerGroup",
                "Phase 17",
            ),
        ] {
            let expected = format!("client request {code} has no active handler ({phase})");
            let no_capability = dispatch(&runtime, code, &[(key, "consumer-no-inbound")], None);
            assert_eq!(no_capability.code, response::REQUEST_CODE_NOT_SUPPORTED);
            assert_eq!(no_capability.remark.as_deref(), Some(expected.as_str()));

            let no_group = dispatch(&runtime, code, &[], None);
            assert_eq!(no_group.code, response::REQUEST_CODE_NOT_SUPPORTED);
            assert_eq!(no_group.remark.as_deref(), Some(expected.as_str()));
        }
        unregister(&runtime, &lease, false).await;
        assert!(runtime.shutdown().await.unwrap());
    }

    #[tokio::test]
    async fn registered_producer_without_transaction_capability_returns_java_null_response() {
        let runtime = runtime("producer-inactive-slots").await;
        let producer = Arc::new(NoInboundProducer);
        let lease = runtime
            .register_producer("producer-no-inbound", producer)
            .await
            .unwrap();

        let transaction = runtime
            .inbound
            .process_with_context(
                "broker:10911",
                RemotingCommand {
                    code: request::CHECK_TRANSACTION_STATE,
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(transaction.is_none());

        for properties in [None, Some("CORRELATION_ID\u{1}missing\u{2}")] {
            let reply = if let Some(value) = properties {
                dispatch(
                    &runtime,
                    request::PUSH_REPLY_MESSAGE_TO_CLIENT,
                    &[("properties", value)],
                    None,
                )
            } else {
                dispatch(&runtime, request::PUSH_REPLY_MESSAGE_TO_CLIENT, &[], None)
            };
            assert_eq!(reply.code, response::REQUEST_CODE_NOT_SUPPORTED);
            assert_eq!(
                reply.remark.as_deref(),
                Some("client request 326 has no active handler (Phase 15)")
            );
        }

        unregister(&runtime, &lease, true).await;
        assert!(runtime.shutdown().await.unwrap());
    }

    #[tokio::test]
    async fn registered_but_undispatched_lite_code_matches_java_null_response() {
        let case = fixtures()["undispatchedCases"][0].clone();
        let code = case["requestCode"].as_i64().unwrap() as i32;
        let runtime = runtime("undispatched-lite-request").await;
        let response = runtime
            .inbound
            .process(RemotingCommand {
                code,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            response.is_some(),
            case["responsePresent"].as_bool().unwrap(),
            "Java ClientRemotingProcessor.processRequest result for request code {code}"
        );
        if let (Some(response), Some(expected_code)) = (
            response,
            case.get("responseCode").and_then(serde_json::Value::as_i64),
        ) {
            assert_eq!(response.code, expected_code as i32);
        }
        assert!(runtime.shutdown().await.unwrap());
    }
}
