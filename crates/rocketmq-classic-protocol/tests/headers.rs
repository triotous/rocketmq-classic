use rocketmq_classic_protocol::header::{
    CommandHeader, GetConsumerListByGroupRequestHeader, GetRouteInfoRequestHeader, HeaderMap,
    PullMessageRequestHeader, PullMessageResponseHeader, QueryConsumerOffsetRequestHeader,
    QueryConsumerOffsetResponseHeader, SendMessageRequestHeaderV1, SendMessageRequestHeaderV2,
    SendMessageResponseHeader, UpdateConsumerOffsetRequestHeader,
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

    let consumers = GetConsumerListByGroupRequestHeader::decode_fields(&java_ext_fields(
        "header-consumer-list-request",
    ))
    .unwrap();
    assert_eq!(consumers.consumer_group, "workers");
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
