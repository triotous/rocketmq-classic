use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_protocol::{SharedValue, heartbeat::SubscriptionData};
use rocketmq_classic_protocol::{
    body::{
        BatchAckMessageRequestBody, BitSet, ConsumeMessageDirectlyResult, ConsumeMessageResult,
        ConsumerConsumeStatus, ConsumerListByGroupResponseBody, ConsumerPopProcessQueueInfo,
        ConsumerProcessQueueInfo, ConsumerRunningInfoBody, GetConsumerStatusBody,
        LiteSubscriptionAction, LiteSubscriptionCtlRequestBody, LockBatchRequestBody,
        LockBatchResponseBody, MessageRequestMode, QueryAssignmentRequestBody, ResetOffsetBody,
        SetMessageRequestModeRequestBody, UnlockBatchRequestBody,
    },
    header::{
        AckMessageRequestHeader, ChangeInvisibleTimeRequestHeader,
        CheckTransactionStateRequestHeader, CheckTransactionStateResponseHeader, CommandHeader,
        ConsumeMessageDirectlyResultRequestHeader, ConsumerSendMsgBackRequestHeader,
        CreateTopicRequestHeader, EndTransactionRequestHeader, EndTransactionResponseHeader,
        GetConsumerListByGroupRequestHeader, GetConsumerRunningInfoRequestHeader,
        GetConsumerStatusRequestHeader, GetEarliestMsgStoretimeRequestHeader,
        GetEarliestMsgStoretimeResponseHeader, GetMaxOffsetRequestHeader,
        GetMaxOffsetResponseHeader, GetMinOffsetRequestHeader, GetMinOffsetResponseHeader,
        GetRouteInfoRequestHeader, HeaderMap, LiteSubscriptionCtlRequestHeader,
        LockBatchMqRequestHeader, NotificationRequestHeader, NotificationResponseHeader,
        NotifyUnsubscribeLiteRequestHeader, PollingInfoRequestHeader, PollingInfoResponseHeader,
        PopLiteMessageRequestHeader, PopLiteMessageResponseHeader, PopMessageRequestHeader,
        PopMessageResponseHeader, PullMessageRequestHeader, PullMessageResponseHeader,
        QueryConsumeTimeSpanRequestHeader, QueryConsumerOffsetRequestHeader,
        QueryConsumerOffsetResponseHeader, QueryMessageRequestHeader, QueryMessageResponseHeader,
        RecallMessageRequestHeader, RecallMessageResponseHeader, ReplyMessageRequestHeader,
        ResetOffsetRequestHeader, SearchOffsetRequestHeader, SearchOffsetResponseHeader,
        SendMessageRequestHeaderV1, SendMessageRequestHeaderV2, SendMessageResponseHeader,
        UnlockBatchMqRequestHeader, UnregisterClientRequestHeader, UnregisterClientResponseHeader,
        UpdateConsumerOffsetRequestHeader, ViewMessageRequestHeader,
    },
};

fn json_header_map(name: &str) -> HeaderMap {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    let frame = std::fs::read(format!("{root}/fixtures/remoting/{name}.bin")).unwrap();
    let marked = u32::from_be_bytes(frame[4..8].try_into().unwrap());
    let header_len = (marked & 0x00ff_ffff) as usize;
    let header: serde_json::Value = serde_json::from_slice(&frame[8..8 + header_len]).unwrap();
    serde_json::from_value(header["extFields"].clone()).unwrap()
}

fn java_ext_fields(stem: &str) -> HeaderMap {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    let metadata: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(format!("{root}/fixtures/remoting/{stem}.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(metadata["java_client_version"], "5.5.1");
    serde_json::from_value(metadata["normalized_fields"]["ext_fields"].clone()).unwrap()
}

fn java_body(stem: &str) -> Vec<u8> {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    let path = format!("{root}/fixtures/remoting/{stem}.bin");
    let frame = std::fs::read(path).unwrap();
    let marked = u32::from_be_bytes(frame[4..8].try_into().unwrap());
    let header_len = (marked & 0x00ff_ffff) as usize;
    let header_end = 8 + header_len;
    frame[header_end..].to_vec()
}

#[test]
fn java_request_and_response_header_fixtures_decode() {
    let route =
        GetRouteInfoRequestHeader::decode_fields(&java_ext_fields("header-route-request")).unwrap();
    assert_eq!(route.topic, "orders");

    let create_topic =
        CreateTopicRequestHeader::decode_fields(&java_ext_fields("header-create-topic-request"))
            .unwrap();
    assert_eq!(create_topic.topic, "orders");
    assert_eq!(create_topic.default_topic, "TBW102");
    assert_eq!(create_topic.read_queue_nums, 4);
    assert_eq!(create_topic.write_queue_nums, 4);
    assert_eq!(create_topic.perm, 6);
    assert_eq!(create_topic.topic_filter_type, "SINGLE_TAG");
    assert_eq!(create_topic.topic_sys_flag, Some(3));
    assert_eq!(
        create_topic.attributes.as_deref(),
        Some("message.type=normal")
    );
    assert_eq!(create_topic.order, Some(false));
    assert_eq!(create_topic.force, Some(false));

    let query_message =
        QueryMessageRequestHeader::decode_fields(&java_ext_fields("header-query-message-request"))
            .unwrap();
    assert_eq!(query_message.topic, "orders");
    assert_eq!(query_message.key, "order-42");
    assert_eq!(query_message.max_num, 32);
    assert_eq!(query_message.begin_timestamp, 1_712_345_678_000);
    assert_eq!(query_message.end_timestamp, 1_712_345_679_000);
    assert_eq!(query_message.index_type.as_deref(), Some("UNIQUE_KEY"));
    assert_eq!(query_message.last_key.as_deref(), Some("last-index-key"));

    let query_message_response = QueryMessageResponseHeader::decode_fields(&java_ext_fields(
        "header-query-message-response",
    ))
    .unwrap();
    assert_eq!(
        query_message_response.index_last_update_timestamp,
        1_712_345_679_000
    );
    assert_eq!(query_message_response.index_last_update_phyoffset, 9876);

    let view_message =
        ViewMessageRequestHeader::decode_fields(&java_ext_fields("header-view-message-request"))
            .unwrap();
    assert_eq!(view_message.topic.as_deref(), Some("orders"));
    assert_eq!(view_message.offset, 9876);

    let lite_pop =
        PopLiteMessageResponseHeader::decode_fields(&java_ext_fields("header-pop-lite-response"))
            .unwrap();
    assert_eq!(lite_pop.pop_time, 10);
    assert_eq!(lite_pop.invisible_time, 20);
    assert_eq!(lite_pop.revive_queue_id, 1);
    assert_eq!(lite_pop.start_offset_info.as_deref(), Some("orders@0:100"));
    assert_eq!(lite_pop.msg_offset_info.as_deref(), Some("orders@0:101"));
    assert_eq!(lite_pop.order_count_info.as_deref(), Some("0"));

    let query = QueryConsumerOffsetRequestHeader::decode_fields(&java_ext_fields(
        "header-query-offset-request",
    ))
    .unwrap();
    assert_eq!(query.consumer_group, "workers");
    assert_eq!(query.queue_id, 1);

    let query_time_span = QueryConsumeTimeSpanRequestHeader::decode_fields(&java_ext_fields(
        "header-query-consume-time-span-request",
    ))
    .unwrap();
    assert_eq!(query_time_span.topic, "orders");
    assert_eq!(query_time_span.group, "workers");

    let update = UpdateConsumerOffsetRequestHeader::decode_fields(&java_ext_fields(
        "header-update-offset-request",
    ))
    .unwrap();
    assert_eq!(update.commit_offset, 9);

    let pull =
        PullMessageRequestHeader::decode_fields(&java_ext_fields("header-pull-request")).unwrap();
    assert_eq!(pull.queue_offset, 9);
    assert_eq!(pull.sub_version, 1_712_345_678_901);

    let pull_response =
        PullMessageResponseHeader::decode_fields(&java_ext_fields("header-pull-response")).unwrap();
    assert_eq!(pull_response.next_begin_offset, 10);
    let offset_response = QueryConsumerOffsetResponseHeader::decode_fields(&java_ext_fields(
        "header-query-offset-response",
    ))
    .unwrap();
    assert_eq!(offset_response.offset, 9);
    let send_response =
        SendMessageResponseHeader::decode_fields(&java_ext_fields("header-send-response")).unwrap();
    assert_eq!(send_response.msg_id, "msg-1");
    let recall = RecallMessageRequestHeader::decode_fields(&java_ext_fields(
        "header-recall-message-request",
    ))
    .unwrap();
    assert_eq!(recall.producer_group.as_deref(), Some("producers"));
    assert_eq!(recall.topic, "orders");
    assert_eq!(recall.recall_handle, "message-id%orders%1712345678901");
    assert_eq!(recall.namespace.as_deref(), Some("tenant"));
    assert_eq!(recall.namespaced, Some(true));
    assert_eq!(recall.broker_name.as_deref(), Some("broker-a"));
    assert_eq!(recall.oneway, Some(false));
    assert_eq!(recall.logical, Some(true));
    let recall_response = RecallMessageResponseHeader::decode_fields(&java_ext_fields(
        "header-recall-message-response",
    ))
    .unwrap();
    assert_eq!(recall_response.msg_id, "broker-message-id");
    let unregister =
        UnregisterClientRequestHeader::decode_fields(&java_ext_fields("header-unregister-request"))
            .unwrap();
    assert_eq!(unregister.client_id, "client-1");
    assert_eq!(unregister.producer_group.as_deref(), Some("producers"));
    let send_back = ConsumerSendMsgBackRequestHeader::decode_fields(&java_ext_fields(
        "header-send-back-request",
    ))
    .unwrap();
    assert_eq!(send_back.group, "workers");
    assert_eq!(send_back.offset, 9);

    let pop =
        PopMessageRequestHeader::decode_fields(&java_ext_fields("header-pop-request")).unwrap();
    assert_eq!(pop.max_msg_nums, 16);
    assert_eq!(pop.invisible_time, 30_000);
    let ack =
        AckMessageRequestHeader::decode_fields(&java_ext_fields("header-ack-request")).unwrap();
    assert_eq!(ack.offset, 9);
    let change = ChangeInvisibleTimeRequestHeader::decode_fields(&java_ext_fields(
        "header-change-invisible-request",
    ))
    .unwrap();
    assert_eq!(change.invisible_time, 60_000);
    let notification =
        NotificationRequestHeader::decode_fields(&java_ext_fields("header-notification-request"))
            .unwrap();
    assert_eq!(notification.born_time, 1_712_345_678_901);
    let polling =
        PollingInfoRequestHeader::decode_fields(&java_ext_fields("header-polling-request"))
            .unwrap();
    assert_eq!(polling.queue_id, 1);
    let polling_response =
        PollingInfoResponseHeader::decode_fields(&java_ext_fields("header-polling-response"))
            .unwrap();
    assert_eq!(polling_response.polling_num, 2);
    let end = EndTransactionRequestHeader::decode_fields(&java_ext_fields(
        "header-end-transaction-request",
    ))
    .unwrap();
    assert_eq!(end.transaction_id.as_deref(), Some("tx-1"));
    let check = CheckTransactionStateRequestHeader::decode_fields(&java_ext_fields(
        "header-check-transaction-request",
    ))
    .unwrap();
    assert_eq!(check.offset_msg_id.as_deref(), Some("offset-1"));
    let check_response = CheckTransactionStateResponseHeader::decode_fields(&java_ext_fields(
        "header-check-transaction-response",
    ))
    .unwrap();
    assert_eq!(check_response.commit_or_rollback, 8);
    let reply =
        ReplyMessageRequestHeader::decode_fields(&java_ext_fields("header-reply-request")).unwrap();
    assert_eq!(reply.born_host, "127.0.0.1:1001");
    let notification_response =
        NotificationResponseHeader::decode_fields(&java_ext_fields("header-notification-response"))
            .unwrap();
    assert!(notification_response.has_msg);
    let reset =
        ResetOffsetRequestHeader::decode_fields(&java_ext_fields("header-reset-offset-request"))
            .unwrap();
    assert_eq!(reset.offset, Some(9));
    let status = GetConsumerStatusRequestHeader::decode_fields(&java_ext_fields(
        "header-consumer-status-request",
    ))
    .unwrap();
    assert_eq!(status.client_addr, None);
    let direct = ConsumeMessageDirectlyResultRequestHeader::decode_fields(&java_ext_fields(
        "header-consume-directly-request",
    ))
    .unwrap();
    assert_eq!(direct.msg_id, None);
    let lite =
        PopLiteMessageRequestHeader::decode_fields(&java_ext_fields("header-pop-lite-request"))
            .unwrap();
    assert_eq!(lite.client_id, "client");
    assert_eq!(
        UnregisterClientResponseHeader::decode_fields(&HeaderMap::new()),
        Ok(UnregisterClientResponseHeader)
    );
    assert_eq!(
        UnregisterClientResponseHeader::decode_fields(&java_ext_fields(
            "header-unregister-response"
        ))
        .unwrap(),
        UnregisterClientResponseHeader
    );
    let lock =
        LockBatchMqRequestHeader::decode_fields(&java_ext_fields("header-lock-batch-request"))
            .unwrap();
    assert_eq!(lock.namespace.as_deref(), Some("tenant"));
    assert_eq!(lock.broker_name.as_deref(), Some("broker-a"));
    let unlock =
        UnlockBatchMqRequestHeader::decode_fields(&java_ext_fields("header-unlock-batch-request"))
            .unwrap();
    assert_eq!(unlock.namespace.as_deref(), Some("tenant"));
    assert_eq!(
        EndTransactionResponseHeader::decode_fields(&java_ext_fields(
            "header-end-transaction-response"
        ))
        .unwrap(),
        EndTransactionResponseHeader
    );
    let lite_ctl = LiteSubscriptionCtlRequestHeader::decode_fields(&java_ext_fields(
        "header-lite-subscription-request",
    ))
    .unwrap();
    assert_eq!(lite_ctl, LiteSubscriptionCtlRequestHeader);
    let unsubscribe = NotifyUnsubscribeLiteRequestHeader::decode_fields(&java_ext_fields(
        "header-notify-unsubscribe-lite-request",
    ))
    .unwrap();
    assert_eq!(unsubscribe.lite_topic, "lite-orders");

    let send_batch =
        SendMessageRequestHeaderV2::decode_fields(&java_ext_fields("header-send-batch-v2"))
            .unwrap();
    assert_eq!(send_batch.m, Some(false));
    let assignment: QueryAssignmentRequestBody =
        serde_json::from_slice(&java_body("body-query-assignment")).unwrap();
    assert_eq!(
        assignment.message_model,
        Some(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering)
    );

    let pop_response = PopMessageResponseHeader {
        pop_time: 1,
        invisible_time: 2,
        revive_queue_id: 3,
        rest_num: 4,
        ..PopMessageResponseHeader::default()
    };
    let mut fields = HeaderMap::new();
    pop_response.encode_fields(&mut fields);
    assert_eq!(
        PopMessageResponseHeader::decode_fields(&fields),
        Ok(pop_response)
    );

    let consumers = GetConsumerListByGroupRequestHeader::decode_fields(&java_ext_fields(
        "header-consumer-list-request",
    ))
    .unwrap();
    assert_eq!(consumers.consumer_group, "workers");

    let mut pop_fields = java_ext_fields("header-pop-lite-response");
    assert_eq!(
        PopLiteMessageResponseHeader::decode_fields(&pop_fields)
            .unwrap()
            .pop_time,
        10
    );
    for missing in ["popTime", "invisibleTime", "reviveQid"] {
        pop_fields = java_ext_fields("header-pop-lite-response");
        pop_fields.remove(missing);
        assert!(matches!(
            PopLiteMessageResponseHeader::decode_fields(&pop_fields),
            Err(rocketmq_classic_protocol::header::ProtocolError::MissingField(field)) if field == missing
        ));
    }
}

#[test]
fn transaction_and_reply_headers_round_trip_typed_fields() {
    let end = EndTransactionRequestHeader {
        topic: Some("orders".into()),
        producer_group: "producers".into(),
        transaction_state_table_offset: 3,
        commit_log_offset: 9,
        commit_or_rollback: 8,
        from_transaction_check: Some(false),
        msg_id: "msg-1".into(),
        transaction_id: Some("tx-1".into()),
        namespace: None,
        namespaced: None,
        broker_name: None,
        oneway: None,
    };
    let mut fields = HeaderMap::new();
    end.encode_fields(&mut fields);
    assert_eq!(EndTransactionRequestHeader::decode_fields(&fields), Ok(end));

    let check = CheckTransactionStateRequestHeader {
        topic: Some("orders".into()),
        transaction_state_table_offset: 3,
        commit_log_offset: 9,
        msg_id: Some("msg-1".into()),
        transaction_id: Some("tx-1".into()),
        offset_msg_id: Some("offset-id".into()),
        namespace: None,
        namespaced: None,
        broker_name: None,
        oneway: None,
    };
    fields.clear();
    check.encode_fields(&mut fields);
    assert_eq!(
        CheckTransactionStateRequestHeader::decode_fields(&fields),
        Ok(check)
    );

    let response = CheckTransactionStateResponseHeader {
        producer_group: "producers".into(),
        transaction_state_table_offset: 3,
        commit_log_offset: 9,
        commit_or_rollback: 8,
    };
    fields.clear();
    response.encode_fields(&mut fields);
    assert_eq!(
        CheckTransactionStateResponseHeader::decode_fields(&fields),
        Ok(response)
    );

    let reply = ReplyMessageRequestHeader {
        producer_group: "producers".into(),
        topic: "orders".into(),
        default_topic: "TBW102".into(),
        default_topic_queue_nums: 4,
        queue_id: 2,
        sys_flag: 1,
        born_timestamp: 5,
        flag: 7,
        properties: None,
        reconsume_times: None,
        unit_mode: false,
        born_host: "127.0.0.1:1".into(),
        store_host: "127.0.0.1:2".into(),
        store_timestamp: 6,
    };
    fields.clear();
    reply.encode_fields(&mut fields);
    assert_eq!(ReplyMessageRequestHeader::decode_fields(&fields), Ok(reply));
}

#[test]
fn transaction_headers_match_pinned_java_fields_including_rpc_parent() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/transaction.json")).unwrap();
    let transaction_flow = &fixture["transactionFlow"];
    let fields = |value: &serde_json::Value| -> HeaderMap {
        value
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
            .collect()
    };

    for value in transaction_flow["initialEndTransaction"]
        .as_object()
        .unwrap()
        .values()
    {
        let expected = fields(value);
        let header = EndTransactionRequestHeader::decode_fields(&expected).unwrap();
        let mut actual = HeaderMap::new();
        header.encode_fields(&mut actual);
        assert_eq!(actual, expected);
        assert_eq!(header.broker_name.as_deref(), Some("broker-a"));
    }

    let expected_check = fields(&transaction_flow["brokerCheckRequest"]);
    let check = CheckTransactionStateRequestHeader::decode_fields(&expected_check).unwrap();
    let mut actual_check = HeaderMap::new();
    check.encode_fields(&mut actual_check);
    assert_eq!(actual_check, expected_check);
    assert_eq!(check.namespace.as_deref(), Some("fixture-namespace"));
    assert_eq!(check.namespaced, Some(true));
    assert_eq!(check.broker_name.as_deref(), Some("broker-a"));
    assert_eq!(check.oneway, Some(false));

    let mut default_end = HeaderMap::new();
    EndTransactionRequestHeader::default().encode_fields(&mut default_end);
    assert_eq!(
        default_end.get("fromTransactionCheck").map(String::as_str),
        Some("false")
    );
}

#[test]
fn transaction_end_response_and_reply_property_names_are_java_constants() {
    use rocketmq_classic_common::message::constants::*;
    let mut fields = HeaderMap::from([("unexpected".into(), "value".into())]);
    rocketmq_classic_protocol::header::EndTransactionResponseHeader.encode_fields(&mut fields);
    assert_eq!(fields.get("unexpected").map(String::as_str), Some("value"));
    assert_eq!(PROPERTY_CORRELATION_ID, "CORRELATION_ID");
    assert_eq!(PROPERTY_MESSAGE_REPLY_TO_CLIENT, "REPLY_TO_CLIENT");
    assert_eq!(PROPERTY_MESSAGE_TTL, "TTL");
    assert_eq!(PROPERTY_REPLY_MESSAGE_ARRIVE_TIME, "ARRIVE_TIME");
    assert_eq!(PROPERTY_PUSH_REPLY_TIME, "PUSH_REPLY_TIME");
}

#[test]
fn java_json_body_fixtures_decode_to_typed_assignment_lite_and_batch_models() {
    let assignment: QueryAssignmentRequestBody =
        serde_json::from_slice(&java_body("body-query-assignment")).unwrap();
    assert_eq!(assignment.topic.as_deref(), Some("orders"));
    assert_eq!(
        assignment.strategy_name.as_deref(),
        Some("AllocateMessageQueueAveragely")
    );

    let mode: SetMessageRequestModeRequestBody =
        serde_json::from_slice(&java_body("body-set-request-mode")).unwrap();
    assert_eq!(mode.mode, Some(MessageRequestMode::Pop));
    assert_eq!(mode.pop_share_queue_num, 2);

    let lite: LiteSubscriptionCtlRequestBody =
        serde_json::from_slice(&java_body("body-lite-subscription")).unwrap();
    let dto = lite.subscription_set.as_ref().unwrap().with(|items| {
        items
            .iter()
            .next()
            .and_then(Option::as_ref)
            .expect("Java fixture subscription entries are non-null")
            .clone()
    });
    assert_eq!(
        dto.with(|dto| dto.action),
        Some(LiteSubscriptionAction::CompleteAdd)
    );
    assert_eq!(
        dto.with(|dto| dto
            .offset_option
            .as_ref()
            .unwrap()
            .with(|option| option.value)),
        1
    );

    let batch: BatchAckMessageRequestBody =
        serde_json::from_slice(&java_body("body-batch-ack")).unwrap();
    assert_eq!(batch.broker_name.as_deref(), Some("broker-a"));
    assert_eq!(
        batch.acks.as_ref().unwrap().snapshot()[0]
            .as_ref()
            .unwrap()
            .with(|ack| ack.bit_set.as_ref().unwrap().snapshot()),
        BitSet::from_byte_array(&[0b0000_0101])
    );

    for name in ["body-reset-offset", "body-consumer-status"] {
        let raw = java_body(name);
        if name == "body-reset-offset" {
            let typed =
                ResetOffsetBody::decode_java_json(std::str::from_utf8(&raw).unwrap()).unwrap();
            let value = serde_json::to_value(&typed).unwrap();
            assert_eq!(
                value["offsetTable"]
                    .as_object()
                    .unwrap()
                    .values()
                    .next()
                    .unwrap(),
                9
            );
            let offset_table = typed.offset_table.as_ref().unwrap().snapshot();
            let (queue, offset) = &offset_table[0];
            assert_eq!(queue.topic(), Some("orders"));
            assert_eq!(queue.broker_name(), Some("broker-a"));
            assert_eq!(*offset, Some(9));
            assert_eq!(typed.encode_java_json().unwrap(), raw);
        } else {
            let typed = GetConsumerStatusBody::decode_java_json(std::str::from_utf8(&raw).unwrap())
                .unwrap();
            let value = serde_json::to_value(&typed).unwrap();
            assert_eq!(
                value["messageQueueTable"]
                    .as_object()
                    .unwrap()
                    .values()
                    .next()
                    .unwrap(),
                9
            );
            let message_queue_table = typed.message_queue_table.as_ref().unwrap().snapshot();
            let (queue, offset) = &message_queue_table[0];
            assert_eq!(queue.topic(), Some("orders"));
            assert_eq!(queue.broker_name(), Some("broker-a"));
            assert_eq!(*offset, Some(9));
            assert_eq!(typed.encode_java_json().unwrap(), raw);
        }
    }

    let direct: ConsumeMessageDirectlyResult =
        serde_json::from_slice(&java_body("body-consume-directly")).unwrap();
    assert_eq!(direct.consume_result, Some(ConsumeMessageResult::CrSuccess));
    assert_eq!(
        serde_json::to_value(&direct).unwrap(),
        serde_json::from_slice::<serde_json::Value>(&java_body("body-consume-directly")).unwrap()
    );
}

#[test]
fn consumer_running_info_body_matches_pinned_java_5_5_1_json() {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    let fixture: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(format!("{root}/fixtures/consumer/running-info.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        fixture["client"],
        "org.apache.rocketmq:rocketmq-client:5.5.1"
    );
    assert_eq!(
        fixture["sourceCommit"],
        "efa1be59d6fc747ad220f036c86030f05f0419f9"
    );
    assert_eq!(fixture["serverContacted"], false);

    let queue = MessageQueue::new("orders", "broker-a", 1);
    let pop_queue = MessageQueue::new("orders", "broker-a", 2);
    let mut subscription = SubscriptionData::new(Some("orders".into()), Some("*".into()));
    subscription.sub_version = 1_700_000_000_001;
    subscription.tags_set = Some(SharedValue::new(std::collections::HashSet::from([
        Some("blue".to_owned()),
        Some("green".to_owned()),
    ])));
    let body = ConsumerRunningInfoBody {
        properties: std::collections::BTreeMap::from([
            ("PROP_NAMESERVER_ADDR".into(), "127.0.0.1:9876;".into()),
            ("PROP_CONSUME_TYPE".into(), "CONSUME_PASSIVELY".into()),
            ("PROP_CLIENT_VERSION".into(), "V5_5_1".into()),
            (
                "PROP_CONSUMER_START_TIMESTAMP".into(),
                "1700000000000".into(),
            ),
        ]),
        subscription_set: vec![subscription],
        mq_table: vec![(
            queue,
            ConsumerProcessQueueInfo {
                commit_offset: 17,
                cached_msg_min_offset: 11,
                cached_msg_max_offset: 20,
                cached_msg_count: 3,
                cached_msg_size_in_mi_b: 2,
                transaction_msg_min_offset: 2,
                transaction_msg_max_offset: 3,
                transaction_msg_count: 1,
                locked: true,
                try_unlock_times: 4,
                last_lock_timestamp: 1_700_000_000_002,
                droped: false,
                last_pull_timestamp: 1_700_000_000_003,
                last_consume_timestamp: 1_700_000_000_004,
            },
        )],
        mq_pop_table: vec![(
            pop_queue,
            ConsumerPopProcessQueueInfo {
                wait_ack_count: 2,
                droped: false,
                last_pop_timestamp: 1_700_000_000_005,
            },
        )],
        status_table: std::collections::BTreeMap::from([(
            "orders".into(),
            ConsumerConsumeStatus {
                pull_rt: 0.5,
                pull_tps: 2.5,
                consume_rt: 3.5,
                consume_ok_tps: 4.5,
                consume_failed_tps: 0.25,
                consume_failed_msgs: 6,
            },
        )]),
        user_consumer_info: std::collections::BTreeMap::from([("fixture".into(), "value".into())]),
        jstack: Some("jstack-fixture".into()),
    };
    let encoded = body.encode_java_json().unwrap();
    let mut expected =
        ConsumerRunningInfoBody::decode_java_json(fixture["body"].as_str().unwrap()).unwrap();
    let mut actual =
        ConsumerRunningInfoBody::decode_java_json(std::str::from_utf8(&encoded).unwrap()).unwrap();
    sort_java_hash_set_fields(&mut expected);
    sort_java_hash_set_fields(&mut actual);
    assert_eq!(
        actual, expected,
        "only Java HashSet iteration order is normalized"
    );
}

fn sort_java_hash_set_fields(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(fields) => {
            for (name, value) in fields {
                if matches!(name.as_str(), "tagsSet" | "codeSet") {
                    if let serde_json::Value::Array(values) = value {
                        values.sort_by_key(serde_json::Value::to_string);
                    }
                } else {
                    sort_java_hash_set_fields(value);
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                sort_java_hash_set_fields(value);
            }
        }
        _ => {}
    }
}

#[test]
fn java_consumer_list_and_lock_bodies_preserve_optional_and_collection_semantics() {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    let read_body = |stem: &str| {
        let frame = std::fs::read(format!("{root}/fixtures/remoting/{stem}.bin")).unwrap();
        let marked = u32::from_be_bytes(frame[4..8].try_into().unwrap());
        let end = 8 + (marked & 0x00ff_ffff) as usize;
        frame[end..].to_vec()
    };
    let absent: ConsumerListByGroupResponseBody =
        serde_json::from_slice(&read_body("body-consumer-list-absent")).unwrap();
    let empty: ConsumerListByGroupResponseBody =
        serde_json::from_slice(&read_body("body-consumer-list-empty")).unwrap();
    let populated: ConsumerListByGroupResponseBody =
        serde_json::from_slice(&read_body("body-consumer-list-populated")).unwrap();
    assert!(absent.get_consumer_id_list().is_none());
    assert_eq!(
        empty.get_consumer_id_list().unwrap().snapshot(),
        Vec::<Option<String>>::new()
    );
    assert_eq!(
        populated.get_consumer_id_list().unwrap().snapshot(),
        vec![Some("client-a".into()), Some("client-b".into())]
    );

    let lock: LockBatchRequestBody =
        serde_json::from_slice(&read_body("body-lock-request")).unwrap();
    let unlock: UnlockBatchRequestBody =
        serde_json::from_slice(&read_body("body-unlock-request")).unwrap();
    let lock_mq_set = lock.mq_set.as_ref().unwrap().with(|queues| {
        queues
            .iter()
            .map(|queue| queue.as_ref().map(|queue| queue.snapshot()))
            .collect::<std::collections::HashSet<_>>()
    });
    assert_eq!(
        lock_mq_set
            .iter()
            .next()
            .unwrap()
            .as_ref()
            .unwrap()
            .broker_name(),
        Some("broker-a")
    );
    assert!(!lock.only_this_broker);
    assert!(unlock.only_this_broker);
    let response: LockBatchResponseBody =
        serde_json::from_slice(&read_body("body-lock-response")).unwrap();
    let response_set = response.lock_okmq_set.as_ref().unwrap().with(|queues| {
        queues
            .iter()
            .map(|queue| queue.as_ref().map(|queue| queue.snapshot()))
            .collect::<std::collections::HashSet<_>>()
    });
    assert_eq!(
        response_set
            .iter()
            .next()
            .unwrap()
            .as_ref()
            .unwrap()
            .queue_id(),
        1
    );
    let unlock_response: LockBatchResponseBody =
        serde_json::from_slice(&read_body("body-unlock-response")).unwrap();
    let unlock_response_set = unlock_response
        .lock_okmq_set
        .as_ref()
        .unwrap()
        .with(|queues| {
            queues
                .iter()
                .map(|queue| queue.as_ref().map(|queue| queue.snapshot()))
                .collect::<std::collections::HashSet<_>>()
        });
    assert_eq!(
        unlock_response_set
            .iter()
            .next()
            .unwrap()
            .as_ref()
            .unwrap()
            .topic(),
        Some("orders")
    );
}

#[test]
fn java_v1_and_v2_send_fixtures_normalize_identically() {
    let v1 = SendMessageRequestHeaderV1::decode_fields(&java_ext_fields("header-send-v1")).unwrap();
    let v2 = SendMessageRequestHeaderV2::decode_fields(&java_ext_fields("header-send-v2")).unwrap();
    assert_eq!(v2.normalized_v1(), v1);
    assert_eq!(v2.k, Some(false));
    assert_eq!(v2.m, Some(false));
}

#[test]
fn java_frames_match_typed_header_semantics() {
    let route =
        GetRouteInfoRequestHeader::decode_fields(&json_header_map("header-route-request")).unwrap();
    assert_eq!(route.topic, "orders");
    let v1 = SendMessageRequestHeaderV1::decode_fields(&json_header_map("header-send-v1")).unwrap();
    let v2 = SendMessageRequestHeaderV2::decode_fields(&json_header_map("header-send-v2")).unwrap();
    assert_eq!(v1, v2.normalized_v1());
    let response =
        SendMessageResponseHeader::decode_fields(&json_header_map("header-send-response")).unwrap();
    assert_eq!(response.queue_offset, 99);
}

#[test]
fn typed_header_update_preserves_unknown_fields_and_lite_requireds_fail() {
    let mut raw = java_ext_fields("header-notify-unsubscribe-lite-request");
    raw.insert("futureField".into(), "opaque-value".into());
    let mut typed = NotifyUnsubscribeLiteRequestHeader::decode_fields(&raw).unwrap();
    typed.lite_topic = "lite-orders-v2".into();
    typed.encode_fields(&mut raw);
    assert_eq!(
        raw.get("futureField").map(String::as_str),
        Some("opaque-value")
    );
    assert_eq!(
        raw.get("liteTopic").map(String::as_str),
        Some("lite-orders-v2")
    );

    for missing in ["liteTopic", "consumerGroup", "clientId"] {
        let mut fields = java_ext_fields("header-notify-unsubscribe-lite-request");
        fields.remove(missing);
        let error = NotifyUnsubscribeLiteRequestHeader::decode_fields(&fields)
            .unwrap_err()
            .to_string();
        assert!(error.contains(missing), "{error}");
    }
}

#[test]
fn required_header_fields_reject_absence_at_protocol_boundary() {
    let empty = HeaderMap::new();
    let err = GetRouteInfoRequestHeader::decode_fields(&empty)
        .unwrap_err()
        .to_string();
    assert!(err.contains("topic"));
    let err = QueryConsumerOffsetRequestHeader::decode_fields(&empty)
        .unwrap_err()
        .to_string();
    assert!(err.contains("consumerGroup"));
    let valid_prefix = HeaderMap::from([
        ("consumerGroup".into(), "g".into()),
        ("topic".into(), "t".into()),
        ("queueId".into(), "0".into()),
    ]);
    let err = UpdateConsumerOffsetRequestHeader::decode_fields(&valid_prefix)
        .unwrap_err()
        .to_string();
    assert!(err.contains("commitOffset"));
    let err = PullMessageRequestHeader::decode_fields(&empty)
        .unwrap_err()
        .to_string();
    assert!(err.contains("consumerGroup"));
    let err = SendMessageRequestHeaderV2::decode_fields(&empty)
        .unwrap_err()
        .to_string();
    assert!(err.contains("a"));
    let err = SendMessageResponseHeader::decode_fields(&empty)
        .unwrap_err()
        .to_string();
    assert!(err.contains("msgId"));
    let err = QueryConsumerOffsetResponseHeader::decode_fields(&empty)
        .unwrap_err()
        .to_string();
    assert!(err.contains("offset"));
    let err = QueryMessageRequestHeader::decode_fields(&empty)
        .unwrap_err()
        .to_string();
    assert!(err.contains("topic"));
    let err = QueryMessageResponseHeader::decode_fields(&empty)
        .unwrap_err()
        .to_string();
    assert!(err.contains("indexLastUpdateTimestamp"));
    let err = ViewMessageRequestHeader::decode_fields(&empty)
        .unwrap_err()
        .to_string();
    assert!(err.contains("offset"));
    let err = CreateTopicRequestHeader::decode_fields(&empty)
        .unwrap_err()
        .to_string();
    assert!(err.contains("topicFilterType"));

    let mut invalid_topic_filter = java_ext_fields("header-create-topic-request");
    invalid_topic_filter.insert("topicFilterType".into(), "NOT_A_FILTER".into());
    let err = CreateTopicRequestHeader::decode_fields(&invalid_topic_filter)
        .unwrap_err()
        .to_string();
    assert!(err.contains("topicFilterType"));

    let mut bad = HeaderMap::from([
        ("consumerGroup".into(), "g".into()),
        ("topic".into(), "t".into()),
        ("queueId".into(), "2147483648".into()),
    ]);
    let err = QueryConsumerOffsetRequestHeader::decode_fields(&bad)
        .unwrap_err()
        .to_string();
    assert!(err.contains("queueId"));
    bad.insert("queueId".into(), "0".into());
    bad.insert("setZeroIfNotFound".into(), "yes".into());
    let decoded = QueryConsumerOffsetRequestHeader::decode_fields(&bad).unwrap();
    assert_eq!(decoded.set_zero_if_not_found, Some(false));

    let mut transaction = HeaderMap::from([
        ("producerGroup".into(), "g".into()),
        ("tranStateTableOffset".into(), "1".into()),
        ("commitLogOffset".into(), "2".into()),
        ("commitOrRollback".into(), "overflow".into()),
        ("msgId".into(), "m".into()),
    ]);
    let err = EndTransactionRequestHeader::decode_fields(&transaction)
        .unwrap_err()
        .to_string();
    assert!(err.contains("commitOrRollback"));
    transaction.insert("commitOrRollback".into(), "8".into());
    assert_eq!(
        EndTransactionRequestHeader::decode_fields(&transaction)
            .unwrap()
            .from_transaction_check,
        None
    );

    let mut v1_fields = HeaderMap::from([
        ("producerGroup".into(), "p".into()),
        ("topic".into(), "t".into()),
        ("defaultTopic".into(), "d".into()),
        ("defaultTopicQueueNums".into(), "1".into()),
        ("queueId".into(), "0".into()),
        ("sysFlag".into(), "0".into()),
        ("bornTimestamp".into(), "0".into()),
        ("flag".into(), "0".into()),
    ]);
    assert_eq!(
        SendMessageRequestHeaderV1::decode_fields(&v1_fields)
            .unwrap()
            .unit_mode,
        None
    );
    v1_fields.insert("unitMode".into(), "false".into());
    assert_eq!(
        SendMessageRequestHeaderV1::decode_fields(&v1_fields)
            .unwrap()
            .unit_mode,
        Some(false)
    );
}

#[test]
fn header_validation_matrix() {
    use rocketmq_classic_protocol::header::ProtocolError;
    use std::collections::BTreeSet;

    struct Case {
        model: &'static str,
        fixture: &'static str,
        decode: fn(&HeaderMap) -> Result<HeaderMap, ProtocolError>,
        update: fn(&mut HeaderMap) -> Result<(), ProtocolError>,
        required: &'static str,
        i32_fields: &'static str,
        i64_fields: &'static str,
        optional: &'static str,
        booleans: &'static str,
    }
    macro_rules! row {
        ($model:ty, $fixture:literal, $required:literal, $i32:literal, $i64:literal, $optional:literal, $bools:literal) => {
            Case {
                model: stringify!($model),
                fixture: $fixture,
                decode: |map| {
                    let decoded = <$model>::decode_fields(map)?;
                    let mut encoded = HeaderMap::new();
                    decoded.encode_fields(&mut encoded);
                    Ok(encoded)
                },
                update: |map| {
                    let decoded = <$model>::decode_fields(map)?;
                    decoded.encode_fields(map);
                    Ok(())
                },
                required: $required,
                i32_fields: $i32,
                i64_fields: $i64,
                optional: $optional,
                booleans: $bools,
            }
        };
    }
    let cases = [
        row!(
            GetRouteInfoRequestHeader,
            "route-request",
            "topic",
            "",
            "",
            "acceptStandardJsonOnly",
            "acceptStandardJsonOnly"
        ),
        row!(
            CreateTopicRequestHeader,
            "create-topic-request",
            "topic defaultTopic readQueueNums writeQueueNums perm topicFilterType",
            "readQueueNums writeQueueNums perm topicSysFlag",
            "",
            "topicSysFlag attributes ns nsd bname oway lo",
            "order force nsd oway lo"
        ),
        row!(
            QueryMessageRequestHeader,
            "query-message-request",
            "topic key maxNum beginTimestamp endTimestamp",
            "maxNum",
            "beginTimestamp endTimestamp",
            "indexType lastKey ns nsd bname oway lo",
            "nsd oway lo"
        ),
        row!(
            QueryMessageResponseHeader,
            "query-message-response",
            "indexLastUpdateTimestamp indexLastUpdatePhyoffset",
            "",
            "indexLastUpdateTimestamp indexLastUpdatePhyoffset",
            "",
            ""
        ),
        row!(
            ViewMessageRequestHeader,
            "view-message-request",
            "offset",
            "",
            "offset",
            "topic",
            ""
        ),
        row!(
            SendMessageRequestHeaderV1,
            "send-v1",
            "producerGroup topic defaultTopic defaultTopicQueueNums queueId sysFlag bornTimestamp flag",
            "defaultTopicQueueNums queueId sysFlag flag reconsumeTimes maxReconsumeTimes",
            "bornTimestamp",
            "properties reconsumeTimes unitMode maxReconsumeTimes batch",
            "unitMode batch"
        ),
        row!(
            SendMessageRequestHeaderV2,
            "send-v2",
            "a b c d e f g h",
            "d e f h j l",
            "g",
            "i j k l m",
            "k m"
        ),
        row!(
            PullMessageRequestHeader,
            "pull-request",
            "consumerGroup topic queueId queueOffset maxMsgNums sysFlag commitOffset suspendTimeoutMillis subVersion",
            "queueId maxMsgNums sysFlag maxMsgBytes requestSource",
            "queueOffset commitOffset suspendTimeoutMillis subVersion",
            "liteTopic subscription expressionType maxMsgBytes requestSource proxyFrowardClientId bname",
            ""
        ),
        row!(
            PullMessageResponseHeader,
            "pull-response",
            "suggestWhichBrokerId nextBeginOffset minOffset maxOffset",
            "topicSysFlag groupSysFlag forbiddenType",
            "suggestWhichBrokerId nextBeginOffset minOffset maxOffset offsetDelta",
            "offsetDelta topicSysFlag groupSysFlag forbiddenType",
            ""
        ),
        row!(
            QueryConsumerOffsetRequestHeader,
            "query-offset-request",
            "consumerGroup topic queueId",
            "queueId",
            "",
            "setZeroIfNotFound bname",
            "setZeroIfNotFound"
        ),
        row!(
            QueryConsumeTimeSpanRequestHeader,
            "query-consume-time-span-request",
            "topic group",
            "",
            "",
            "",
            ""
        ),
        row!(
            QueryConsumerOffsetResponseHeader,
            "query-offset-response",
            "offset",
            "",
            "offset",
            "",
            ""
        ),
        row!(
            SearchOffsetRequestHeader,
            "search-offset-request",
            "topic queueId timestamp",
            "queueId",
            "timestamp",
            "liteTopic boundaryType bname",
            ""
        ),
        row!(
            GetMaxOffsetRequestHeader,
            "max-offset-request",
            "topic queueId",
            "queueId",
            "",
            "bname committed",
            "committed"
        ),
        row!(
            GetMinOffsetRequestHeader,
            "min-offset-request",
            "topic queueId",
            "queueId",
            "",
            "bname",
            ""
        ),
        row!(
            GetEarliestMsgStoretimeRequestHeader,
            "earliest-msg-storetime-request",
            "topic queueId",
            "queueId",
            "",
            "bname",
            ""
        ),
        row!(
            SearchOffsetResponseHeader,
            "search-offset-response",
            "offset",
            "",
            "offset",
            "",
            ""
        ),
        row!(
            GetMaxOffsetResponseHeader,
            "max-offset-response",
            "offset",
            "",
            "offset",
            "",
            ""
        ),
        row!(
            GetMinOffsetResponseHeader,
            "min-offset-response",
            "offset",
            "",
            "offset",
            "",
            ""
        ),
        row!(
            GetEarliestMsgStoretimeResponseHeader,
            "earliest-msg-storetime-response",
            "timestamp",
            "",
            "timestamp",
            "",
            ""
        ),
        row!(
            UpdateConsumerOffsetRequestHeader,
            "update-offset-request",
            "consumerGroup topic queueId commitOffset",
            "queueId",
            "commitOffset",
            "bname",
            ""
        ),
        row!(
            SendMessageResponseHeader,
            "send-response",
            "msgId queueId queueOffset",
            "queueId",
            "queueOffset",
            "transactionId batchUniqId recallHandle",
            ""
        ),
        row!(
            UnregisterClientRequestHeader,
            "unregister-request",
            "clientID",
            "",
            "",
            "producerGroup consumerGroup",
            ""
        ),
        row!(
            UnregisterClientResponseHeader,
            "unregister-response",
            "",
            "",
            "",
            "",
            ""
        ),
        row!(
            ConsumerSendMsgBackRequestHeader,
            "send-back-request",
            "group originTopic offset delayLevel",
            "delayLevel maxReconsumeTimes",
            "offset",
            "bname originMsgId maxReconsumeTimes",
            "unitMode"
        ),
        row!(
            EndTransactionRequestHeader,
            "end-transaction-request",
            "producerGroup tranStateTableOffset commitLogOffset commitOrRollback msgId",
            "commitOrRollback",
            "tranStateTableOffset commitLogOffset",
            "topic fromTransactionCheck transactionId ns nsd bname oway",
            "fromTransactionCheck nsd oway"
        ),
        row!(
            EndTransactionResponseHeader,
            "end-transaction-response",
            "",
            "",
            "",
            "",
            ""
        ),
        row!(
            CheckTransactionStateRequestHeader,
            "check-transaction-request",
            "tranStateTableOffset commitLogOffset",
            "",
            "tranStateTableOffset commitLogOffset",
            "topic msgId transactionId offsetMsgId ns nsd bname oway",
            "nsd oway"
        ),
        row!(
            CheckTransactionStateResponseHeader,
            "check-transaction-response",
            "producerGroup tranStateTableOffset commitLogOffset commitOrRollback",
            "commitOrRollback",
            "tranStateTableOffset commitLogOffset",
            "",
            ""
        ),
        row!(
            ReplyMessageRequestHeader,
            "reply-request",
            "producerGroup topic defaultTopic defaultTopicQueueNums queueId sysFlag bornTimestamp flag bornHost storeHost storeTimestamp",
            "defaultTopicQueueNums queueId sysFlag flag reconsumeTimes",
            "bornTimestamp storeTimestamp",
            "properties reconsumeTimes",
            "unitMode"
        ),
        row!(
            RecallMessageRequestHeader,
            "recall-message-request",
            "topic recallHandle",
            "",
            "",
            "producerGroup ns nsd bname oway lo",
            "nsd oway lo"
        ),
        row!(
            RecallMessageResponseHeader,
            "recall-message-response",
            "msgId",
            "",
            "",
            "",
            ""
        ),
        row!(
            NotificationRequestHeader,
            "notification-request",
            "consumerGroup topic queueId pollTime bornTime",
            "queueId",
            "pollTime bornTime",
            "order attemptId clientId expType exp",
            "order isLiteConsumer"
        ),
        row!(
            NotificationResponseHeader,
            "notification-response",
            "",
            "",
            "",
            "",
            "hasMsg pollingFull"
        ),
        row!(
            ResetOffsetRequestHeader,
            "reset-offset-request",
            "group topic timestamp",
            "queueId",
            "offset timestamp",
            "queueId offset",
            "isForce"
        ),
        row!(
            GetConsumerStatusRequestHeader,
            "consumer-status-request",
            "topic group",
            "",
            "",
            "clientAddr",
            ""
        ),
        row!(
            GetConsumerRunningInfoRequestHeader,
            "consumer-running-info-request",
            "consumerGroup clientId",
            "",
            "",
            "ns nsd bname oway",
            "jstackEnable nsd oway"
        ),
        row!(
            GetConsumerListByGroupRequestHeader,
            "consumer-list-request",
            "consumerGroup",
            "",
            "",
            "",
            ""
        ),
        row!(
            LockBatchMqRequestHeader,
            "lock-batch-request",
            "",
            "",
            "",
            "ns nsd bname oway",
            "nsd oway"
        ),
        row!(
            UnlockBatchMqRequestHeader,
            "unlock-batch-request",
            "",
            "",
            "",
            "ns nsd bname oway",
            "nsd oway"
        ),
        row!(
            PopMessageRequestHeader,
            "pop-request",
            "consumerGroup topic queueId maxMsgNums invisibleTime pollTime bornTime initMode",
            "queueId maxMsgNums initMode",
            "invisibleTime pollTime bornTime",
            "expType exp order attemptId",
            "order"
        ),
        row!(
            PopMessageResponseHeader,
            "pop-response",
            "popTime invisibleTime reviveQid restNum",
            "reviveQid",
            "popTime invisibleTime restNum",
            "startOffsetInfo msgOffsetInfo orderCountInfo",
            ""
        ),
        row!(
            PopLiteMessageRequestHeader,
            "pop-lite-request",
            "clientId consumerGroup topic maxMsgNum invisibleTime pollTime bornTime",
            "maxMsgNum",
            "invisibleTime pollTime bornTime",
            "attemptId",
            ""
        ),
        row!(
            PopLiteMessageResponseHeader,
            "pop-lite-response",
            "popTime invisibleTime reviveQid",
            "reviveQid",
            "popTime invisibleTime",
            "startOffsetInfo msgOffsetInfo orderCountInfo",
            ""
        ),
        row!(
            AckMessageRequestHeader,
            "ack-request",
            "consumerGroup topic queueId extraInfo offset",
            "queueId",
            "offset",
            "liteTopic",
            ""
        ),
        row!(
            ChangeInvisibleTimeRequestHeader,
            "change-invisible-request",
            "consumerGroup topic queueId extraInfo offset invisibleTime",
            "queueId",
            "offset invisibleTime",
            "liteTopic",
            "suspend"
        ),
        row!(
            PollingInfoRequestHeader,
            "polling-request",
            "consumerGroup topic queueId",
            "queueId",
            "",
            "",
            ""
        ),
        row!(
            PollingInfoResponseHeader,
            "polling-response",
            "pollingNum",
            "pollingNum",
            "",
            "",
            ""
        ),
        row!(
            LiteSubscriptionCtlRequestHeader,
            "lite-subscription-request",
            "",
            "",
            "",
            "",
            ""
        ),
        row!(
            NotifyUnsubscribeLiteRequestHeader,
            "notify-unsubscribe-lite-request",
            "liteTopic consumerGroup clientId",
            "",
            "",
            "",
            ""
        ),
        row!(
            ConsumeMessageDirectlyResultRequestHeader,
            "consume-directly-request",
            "consumerGroup",
            "topicSysFlag groupSysFlag",
            "",
            "clientId msgId brokerName topic topicSysFlag groupSysFlag",
            ""
        ),
    ];
    // Keep the matrix tied to the public implementations, not just to fixture names.
    let source = include_str!("../src/header/basic.rs");
    let implementations: BTreeSet<_> = source
        .lines()
        .filter_map(|line| {
            line.trim()
                .strip_prefix("impl CommandHeader for ")
                .and_then(|name| name.strip_suffix(" {"))
        })
        .collect();
    let matrix: BTreeSet<_> = cases.iter().map(|case| case.model).collect();
    assert_eq!(matrix.len(), cases.len(), "duplicate validation row");
    let mut expected = implementations;
    assert!(source.contains("pub type UnlockBatchMqRequestHeader = LockBatchMqRequestHeader;"));
    expected.insert("UnlockBatchMqRequestHeader");
    assert_eq!(
        matrix, expected,
        "update the matrix for every public header"
    );

    for case in cases {
        let baseline = java_ext_fields(&format!("header-{}", case.fixture));
        (case.decode)(&baseline).unwrap_or_else(|e| panic!("{}: {e}", case.fixture));
        let required: BTreeSet<_> = case.required.split_whitespace().collect();
        let i32_fields: BTreeSet<_> = case.i32_fields.split_whitespace().collect();
        let i64_fields: BTreeSet<_> = case.i64_fields.split_whitespace().collect();
        let optional: BTreeSet<_> = case.optional.split_whitespace().collect();
        let booleans: BTreeSet<_> = case.booleans.split_whitespace().collect();
        assert!(
            i32_fields.is_disjoint(&i64_fields),
            "{} numeric width overlap",
            case.model
        );
        assert!(
            required.is_disjoint(&optional),
            "{} required/optional overlap",
            case.model
        );
        let mut modeled = required.clone();
        modeled.extend(optional.iter().copied());
        modeled.extend(booleans.iter().copied());
        modeled.extend(i32_fields.iter().copied());
        modeled.extend(i64_fields.iter().copied());
        let implementation = if case.model == "UnlockBatchMqRequestHeader" {
            "LockBatchMqRequestHeader"
        } else {
            case.model
        };
        let encode_source = source
            .split_once(&format!("impl CommandHeader for {implementation} {{"))
            .unwrap()
            .1
            .split_once("fn decode_fields")
            .unwrap()
            .0;
        let encoded_keys: BTreeSet<_> = ["fields.insert(", "put_optional("]
            .into_iter()
            .flat_map(|call| encode_source.match_indices(call))
            .map(|(index, call)| {
                let rest = &encode_source[index + call.len()..];
                rest.split_once('"').unwrap().1.split_once('"').unwrap().0
            })
            .collect();
        assert_eq!(
            modeled, encoded_keys,
            "{} matrix must classify every modeled wire field",
            case.model
        );
        let mut raw = baseline.clone();
        for &name in &optional {
            let value = if name == "boundaryType" {
                "LOWER"
            } else if i32_fields.contains(name) || i64_fields.contains(name) {
                "0"
            } else if booleans.contains(name) {
                "false"
            } else {
                ""
            };
            raw.insert(name.into(), value.into());
        }
        raw.insert("futureField".into(), "opaque-value".into());
        let encoded = (case.decode)(&raw).unwrap();
        assert_eq!(
            encoded.keys().map(String::as_str).collect::<BTreeSet<_>>(),
            modeled,
            "{} incomplete modeled field coverage",
            case.model
        );
        (case.update)(&mut raw).unwrap();
        assert_eq!(
            raw.get("futureField").map(String::as_str),
            Some("opaque-value")
        );
        for name in &modeled {
            assert_eq!(
                raw.get(*name),
                encoded.get(*name),
                "{} updated {name}",
                case.model
            );
        }
        for name in case.required.split_whitespace() {
            let mut fields = baseline.clone();
            fields.remove(name);
            assert_eq!(
                (case.decode)(&fields),
                Err(ProtocolError::MissingField(name)),
                "{} {name}",
                case.fixture
            );
            fields.insert(name.into(), "".into());
            assert_eq!(
                (case.decode)(&fields).is_ok(),
                !(i32_fields.contains(name)
                    || i64_fields.contains(name)
                    || (case.model == "CreateTopicRequestHeader" && name == "topicFilterType")),
                "{} empty required {name}",
                case.fixture
            );
        }
        for (width, names) in [(32, case.i32_fields), (64, case.i64_fields)] {
            for name in names.split_whitespace() {
                for value in [
                    "",
                    "not-a-number",
                    if width == 32 {
                        "2147483648"
                    } else {
                        "9223372036854775808"
                    },
                    if width == 32 {
                        "-2147483649"
                    } else {
                        "-9223372036854775809"
                    },
                ] {
                    let mut fields = baseline.clone();
                    fields.insert(name.into(), value.into());
                    assert!(
                        matches!((case.decode)(&fields), Err(ProtocolError::InvalidField { field, .. }) if field == name),
                        "{} {name}={value}",
                        case.fixture
                    );
                }
                for value in if width == 32 {
                    ["2147483647", "-2147483648"]
                } else {
                    ["9223372036854775807", "-9223372036854775808"]
                } {
                    let mut fields = baseline.clone();
                    fields.insert(name.into(), value.into());
                    let decoded = (case.decode)(&fields)
                        .unwrap_or_else(|e| panic!("{} {name}={value}: {e}", case.fixture));
                    assert_eq!(
                        decoded.get(name).map(String::as_str),
                        Some(value),
                        "{} {name}={value}",
                        case.fixture
                    );
                }
                let mut fields = baseline.clone();
                fields.insert(name.into(), "0".into());
                assert_eq!(
                    (case.decode)(&fields)
                        .unwrap()
                        .get(name)
                        .map(String::as_str),
                    Some("0"),
                    "{} {name}=0",
                    case.fixture
                );
            }
        }
        for name in case.optional.split_whitespace() {
            let mut fields = baseline.clone();
            fields.remove(name);
            assert!(
                (case.decode)(&fields).is_ok(),
                "{} absent {name}",
                case.fixture
            );
            let absent = (case.decode)(&fields).unwrap();
            let values: &[&str] = if name == "boundaryType" {
                &["LOWER", "UPPER"]
            } else if name == "committed" {
                &["false"]
            } else if i32_fields.contains(name) || i64_fields.contains(name) {
                &["0"]
            } else if booleans.contains(name) {
                &["false", "true", ""]
            } else {
                &[""]
            };
            for &value in values {
                let mut present = fields.clone();
                present.insert(name.into(), value.into());
                let decoded = (case.decode)(&present)
                    .unwrap_or_else(|e| panic!("{} {name}={value}: {e}", case.fixture));
                assert_ne!(decoded, absent, "{} absent vs {name}={value}", case.fixture);
                assert_eq!(
                    decoded.get(name).map(String::as_str),
                    Some(if booleans.contains(name) && value.is_empty() {
                        "false"
                    } else {
                        value
                    }),
                    "{} {name}={value}",
                    case.fixture
                );
            }
        }
        for name in case.booleans.split_whitespace() {
            let mut absent = baseline.clone();
            absent.remove(name);
            let mut explicit_false = absent.clone();
            explicit_false.insert(name.into(), "false".into());
            let absent_decoded = (case.decode)(&absent).unwrap();
            let false_decoded = (case.decode)(&explicit_false).unwrap();
            if optional.contains(name) {
                assert_ne!(
                    absent_decoded, false_decoded,
                    "{} {name}: None vs Some(false)",
                    case.fixture
                );
            } else {
                assert_eq!(
                    absent_decoded, false_decoded,
                    "{} {name}: absent defaults to false",
                    case.fixture
                );
            }
            for (value, expected) in [
                ("TRUE", "true"),
                ("TrUe", "true"),
                ("false", "false"),
                ("yes", "false"),
                ("1", "false"),
                (" true ", "false"),
                ("", "false"),
            ] {
                let mut fields = baseline.clone();
                fields.insert(name.into(), value.into());
                let mut canonical = baseline.clone();
                canonical.insert(name.into(), expected.into());
                assert_eq!(
                    (case.decode)(&fields),
                    (case.decode)(&canonical),
                    "{} {name}={value}",
                    case.fixture
                );
            }
        }
    }
}
