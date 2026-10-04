use rocketmq_classic_protocol::{
    body::{
        BatchAckMessageRequestBody, ConsumeMessageDirectlyResult, ConsumeMessageResult,
        GetConsumerStatusBody, LiteSubscriptionAction, LiteSubscriptionCtlRequestBody,
        MessageRequestMode, QueryAssignmentRequestBody, ResetOffsetBody,
        SetMessageRequestModeRequestBody,
    },
    header::{
        AckMessageRequestHeader, ChangeInvisibleTimeRequestHeader,
        CheckTransactionStateRequestHeader, CheckTransactionStateResponseHeader, CommandHeader,
        ConsumeMessageDirectlyResultRequestHeader, ConsumerSendMsgBackRequestHeader,
        EndTransactionRequestHeader, EndTransactionResponseHeader,
        GetConsumerListByGroupRequestHeader, GetConsumerStatusRequestHeader,
        GetRouteInfoRequestHeader, HeaderMap, LiteSubscriptionCtlRequestHeader,
        LockBatchMqRequestHeader, NotificationRequestHeader, NotificationResponseHeader,
        PollingInfoRequestHeader, PollingInfoResponseHeader, PopLiteMessageRequestHeader,
        PopMessageRequestHeader, PopMessageResponseHeader, PullMessageRequestHeader,
        PullMessageResponseHeader, QueryConsumerOffsetRequestHeader,
        QueryConsumerOffsetResponseHeader, ReplyMessageRequestHeader, ResetOffsetRequestHeader,
        SendMessageRequestHeaderV1, SendMessageRequestHeaderV2, SendMessageResponseHeader,
        UnlockBatchMqRequestHeader, UnregisterClientRequestHeader, UnregisterClientResponseHeader,
        UpdateConsumerOffsetRequestHeader,
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

    let query = QueryConsumerOffsetRequestHeader::decode_fields(&java_ext_fields(
        "header-query-offset-request",
    ))
    .unwrap();
    assert_eq!(query.consumer_group, "workers");
    assert_eq!(query.queue_id, 1);

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
    assert_eq!(mode.mode, MessageRequestMode::Pop);
    assert_eq!(mode.pop_share_queue_num, 2);

    let lite: LiteSubscriptionCtlRequestBody =
        serde_json::from_slice(&java_body("body-lite-subscription")).unwrap();
    let dto = &lite.subscription_set.as_ref().unwrap()[0];
    assert_eq!(dto.action, Some(LiteSubscriptionAction::CompleteAdd));
    assert_eq!(dto.offset_option.as_ref().unwrap().value, 1);

    let batch: BatchAckMessageRequestBody =
        serde_json::from_slice(&java_body("body-batch-ack")).unwrap();
    assert_eq!(batch.broker_name.as_deref(), Some("broker-a"));
    assert_eq!(
        batch.acks.as_ref().unwrap()[0].bit_set,
        Some(vec![0b0000_0101])
    );

    for name in ["body-reset-offset", "body-consumer-status"] {
        let raw = java_body(name);
        if name == "body-reset-offset" {
            let typed = decode_java_queue_offset_body(&raw, "offsetTable");
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
            let (queue, offset) = &typed.offset_table.as_ref().unwrap()[0];
            assert_eq!(queue.topic(), "orders");
            assert_eq!(queue.broker_name(), "broker-a");
            assert_eq!(*offset, 9);
        } else {
            let table = decode_java_queue_offset_body(&raw, "messageQueueTable");
            let typed = GetConsumerStatusBody {
                message_queue_table: table.offset_table.unwrap_or_default(),
                consumer_table: std::collections::BTreeMap::new(),
            };
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
            let (queue, offset) = &typed.message_queue_table[0];
            assert_eq!(queue.topic(), "orders");
            assert_eq!(queue.broker_name(), "broker-a");
            assert_eq!(*offset, 9);
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

fn decode_java_queue_offset_body(raw: &[u8], field: &str) -> ResetOffsetBody {
    let text = std::str::from_utf8(raw).unwrap();
    let marker = format!("\"{field}\":{{{{");
    let start = text.find(&marker).expect("Java map field exists") + marker.len() - 2;
    let mut depth = 0_i32;
    let mut end = start;
    for (index, ch) in text[start..].char_indices() {
        if ch == '{' {
            depth += 1;
        }
        if ch == '}' {
            depth -= 1;
            if depth == 0 {
                end = start + index + ch.len_utf8();
                break;
            }
        }
    }
    let entry = &text[start + 1..end - 1];
    let separator = entry.rfind("}:").expect("map key/value separator") + 1;
    let key = &entry[..separator];
    let offset = entry[separator + 1..].parse::<i64>().unwrap();
    let queue: rocketmq_classic_common::message::MessageQueue = serde_json::from_str(key).unwrap();
    ResetOffsetBody {
        offset_table: Some(vec![(queue, offset)]),
    }
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
