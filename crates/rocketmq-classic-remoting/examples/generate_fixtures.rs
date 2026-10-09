use std::{env, fs, path::PathBuf};

use rocketmq_classic_protocol::{
    body::{
        BatchAck, BatchAckMessageRequestBody, ConsumeMessageDirectlyResult, ConsumeMessageResult,
        ConsumerListByGroupResponseBody, GetConsumerStatusBody, LiteSubscriptionAction,
        LiteSubscriptionCtlRequestBody, LiteSubscriptionDto, LockBatchRequestBody,
        LockBatchResponseBody, MessageRequestMode, OffsetOption, OffsetOptionType,
        QueryAssignmentRequestBody, ResetOffsetBody, SetMessageRequestModeRequestBody,
        UnlockBatchRequestBody,
    },
    codes::{LanguageCode, SerializeType},
    header::{
        AckMessageRequestHeader, ChangeInvisibleTimeRequestHeader,
        CheckTransactionStateRequestHeader, CheckTransactionStateResponseHeader, CommandHeader,
        ConsumeMessageDirectlyResultRequestHeader, ConsumerSendMsgBackRequestHeader,
        EndTransactionRequestHeader, EndTransactionResponseHeader,
        GetConsumerListByGroupRequestHeader, GetConsumerStatusRequestHeader,
        GetRouteInfoRequestHeader, LiteSubscriptionCtlRequestHeader, LockBatchMqRequestHeader,
        NotificationRequestHeader, NotificationResponseHeader, NotifyUnsubscribeLiteRequestHeader,
        PollingInfoRequestHeader, PollingInfoResponseHeader, PopLiteMessageRequestHeader,
        PopLiteMessageResponseHeader, PopMessageRequestHeader, PopMessageResponseHeader,
        PullMessageRequestHeader, PullMessageResponseHeader, QueryConsumerOffsetRequestHeader,
        QueryConsumerOffsetResponseHeader, ReplyMessageRequestHeader, ResetOffsetRequestHeader,
        SendMessageRequestHeaderV1, SendMessageRequestHeaderV2, SendMessageResponseHeader,
        UnlockBatchMqRequestHeader, UnregisterClientRequestHeader, UnregisterClientResponseHeader,
        UpdateConsumerOffsetRequestHeader,
    },
};
use rocketmq_classic_remoting::{codec::frame, command::RemotingCommand};

fn main() {
    let mut args = env::args_os().skip(1);
    let mode = args.next().unwrap_or_default();
    let output = args.next().map(PathBuf::from).expect(
        "usage: cargo run -p rocketmq-classic-remoting --example generate_fixtures -- <directory>",
    );
    fs::create_dir_all(&output).expect("create output directory");
    if mode == "p4-headers" {
        generate_p4_headers(&output);
        return;
    }
    if mode == "p4-bodies" {
        generate_p4_bodies(&output);
        return;
    }
    for (name, code, serialization) in [
        ("json-route", 105, SerializeType::Json),
        ("rocketmq-transaction", 37, SerializeType::RocketMq),
    ] {
        let command = RemotingCommand {
            code,
            language: Some(LanguageCode::Rust),
            version: 0,
            opaque: 7,
            remark: Some(name.into()),
            ext_fields: Some(std::collections::BTreeMap::from([(
                "oracle".into(),
                "rust-phase-2".into(),
            )])),
            body: Some(vec![0, 1, 2, 255]),
            serialization,
            ..RemotingCommand::default()
        };
        fs::write(
            output.join(format!("{name}.bin")),
            frame::encode(&command, frame::DEFAULT_MAX_FRAME_SIZE).expect("encode frame"),
        )
        .expect("write frame");
    }
}

fn generate_p4_headers(output: &std::path::Path) {
    // Decode the checked-in Java 5.5.1 oracle through each public Rust model,
    // then emit a Rust frame for the same request or response role.
    write_oracle_header::<QueryConsumerOffsetResponseHeader>(
        output,
        "typed-query-offset-response.bin",
        "header-query-offset-response",
        0,
    );
    write_oracle_header::<UnregisterClientRequestHeader>(
        output,
        "typed-unregister-request.bin",
        "header-unregister-request",
        35,
    );
    write_oracle_header::<LockBatchMqRequestHeader>(
        output,
        "typed-lock-batch-request.bin",
        "header-lock-batch-request",
        41,
    );
    write_oracle_header::<UnlockBatchMqRequestHeader>(
        output,
        "typed-unlock-batch-request.bin",
        "header-unlock-batch-request",
        42,
    );
    write_oracle_header::<GetConsumerListByGroupRequestHeader>(
        output,
        "typed-consumer-list-request.bin",
        "header-consumer-list-request",
        38,
    );
    write_oracle_header::<ConsumerSendMsgBackRequestHeader>(
        output,
        "typed-send-back-request.bin",
        "header-send-back-request",
        36,
    );
    write_oracle_header::<UnregisterClientResponseHeader>(
        output,
        "typed-unregister-response.bin",
        "header-unregister-response",
        0,
    );
    write_oracle_header::<EndTransactionResponseHeader>(
        output,
        "typed-end-transaction-response.bin",
        "header-end-transaction-response",
        0,
    );
    write_oracle_header::<GetConsumerStatusRequestHeader>(
        output,
        "typed-consumer-status-request.bin",
        "header-consumer-status-request",
        221,
    );
    write_oracle_header::<PollingInfoRequestHeader>(
        output,
        "typed-polling-request.bin",
        "header-polling-request",
        200055,
    );
    write_oracle_header::<PollingInfoResponseHeader>(
        output,
        "typed-polling-response.bin",
        "header-polling-response",
        0,
    );
    write_oracle_header::<PopLiteMessageRequestHeader>(
        output,
        "typed-pop-lite-request.bin",
        "header-pop-lite-request",
        200070,
    );
    write_oracle_header::<LiteSubscriptionCtlRequestHeader>(
        output,
        "typed-lite-subscription-request.bin",
        "header-lite-subscription-request",
        200071,
    );
    write_oracle_header::<NotifyUnsubscribeLiteRequestHeader>(
        output,
        "typed-notify-unsubscribe-lite-request.bin",
        "header-notify-unsubscribe-lite-request",
        200073,
    );
    write_oracle_header::<ConsumeMessageDirectlyResultRequestHeader>(
        output,
        "typed-consume-directly-request.bin",
        "header-consume-directly-request",
        309,
    );

    write_typed(
        output,
        "typed-route.bin",
        105,
        &GetRouteInfoRequestHeader {
            topic: "orders".into(),
            accept_standard_json_only: None,
        },
    );
    write_typed(
        output,
        "typed-send-v1.bin",
        10,
        &SendMessageRequestHeaderV1 {
            producer_group: "producers".into(),
            topic: "orders".into(),
            default_topic: "TBW102".into(),
            default_topic_queue_nums: 4,
            queue_id: 2,
            sys_flag: 1,
            born_timestamp: 1_712_345_678_901,
            flag: 7,
            properties: Some("TAGS\\u0001blue".into()),
            reconsume_times: Some(0),
            unit_mode: Some(false),
            max_reconsume_times: Some(16),
            batch: Some(false),
        },
    );
    write_typed(
        output,
        "typed-send-v2.bin",
        310,
        &SendMessageRequestHeaderV2 {
            a: "producers".into(),
            b: "orders".into(),
            c: "TBW102".into(),
            d: 4,
            e: 2,
            f: 1,
            g: 1_712_345_678_901,
            h: 7,
            i: Some("TAGS\\u0001blue".into()),
            j: Some(0),
            k: Some(false),
            l: Some(16),
            m: Some(false),
        },
    );
    write_typed(
        output,
        "typed-send-batch-v2.bin",
        320,
        &SendMessageRequestHeaderV2 {
            a: "producers".into(),
            b: "orders".into(),
            c: "TBW102".into(),
            d: 4,
            e: 2,
            f: 1,
            g: 1_712_345_678_901,
            h: 7,
            i: Some("TAGS\\u0001blue".into()),
            j: Some(0),
            k: Some(false),
            l: Some(16),
            m: Some(false),
        },
    );
    write_typed(
        output,
        "typed-pull.bin",
        11,
        &PullMessageRequestHeader {
            consumer_group: "workers".into(),
            topic: "orders".into(),
            queue_id: 1,
            queue_offset: 9,
            max_msg_nums: 32,
            sys_flag: 0,
            commit_offset: 8,
            suspend_timeout_millis: 15_000,
            sub_version: 1_712_345_678_901,
            ..PullMessageRequestHeader::default()
        },
    );
    write_typed(
        output,
        "typed-query-offset.bin",
        14,
        &QueryConsumerOffsetRequestHeader {
            consumer_group: "workers".into(),
            topic: "orders".into(),
            queue_id: 1,
            set_zero_if_not_found: None,
        },
    );
    write_typed(
        output,
        "typed-update-offset.bin",
        15,
        &UpdateConsumerOffsetRequestHeader {
            consumer_group: "workers".into(),
            topic: "orders".into(),
            queue_id: 1,
            commit_offset: 9,
        },
    );
    write_typed(
        output,
        "typed-end-transaction.bin",
        37,
        &EndTransactionRequestHeader {
            topic: Some("orders".into()),
            producer_group: "producers".into(),
            transaction_state_table_offset: 3,
            commit_log_offset: 9,
            commit_or_rollback: 8,
            from_transaction_check: Some(false),
            msg_id: "msg-1".into(),
            transaction_id: Some("tx-1".into()),
        },
    );
    write_typed(
        output,
        "typed-check-transaction.bin",
        39,
        &CheckTransactionStateRequestHeader {
            topic: Some("orders".into()),
            transaction_state_table_offset: 3,
            commit_log_offset: 9,
            msg_id: Some("msg-1".into()),
            transaction_id: Some("tx-1".into()),
            offset_msg_id: Some("offset-1".into()),
        },
    );
    write_typed(
        output,
        "typed-check-transaction-response.bin",
        0,
        &CheckTransactionStateResponseHeader {
            producer_group: "producers".into(),
            transaction_state_table_offset: 3,
            commit_log_offset: 9,
            commit_or_rollback: 8,
        },
    );
    write_typed(
        output,
        "typed-reply.bin",
        324,
        &ReplyMessageRequestHeader {
            producer_group: "producers".into(),
            topic: "orders".into(),
            default_topic: "TBW102".into(),
            default_topic_queue_nums: 4,
            queue_id: 2,
            sys_flag: 1,
            born_timestamp: 5,
            flag: 7,
            properties: Some("CORRELATION_ID=correlation-1".into()),
            reconsume_times: None,
            unit_mode: false,
            born_host: "127.0.0.1:1001".into(),
            store_host: "127.0.0.1:10911".into(),
            store_timestamp: 6,
        },
    );
    let binary_reply = vec![0, 255, 128, 42, 10];
    write_typed_body(
        output,
        "typed-reply-v2.bin",
        325,
        &SendMessageRequestHeaderV2 {
            a: "producers".into(),
            b: "orders".into(),
            c: "TBW102".into(),
            d: 4,
            e: 2,
            f: 1,
            g: 5,
            h: 7,
            i: Some("CORRELATION_ID=correlation-1".into()),
            j: Some(0),
            k: Some(false),
            l: None,
            m: Some(false),
        },
        binary_reply.clone(),
    );
    write_typed_body(
        output,
        "typed-reply-push.bin",
        326,
        &ReplyMessageRequestHeader {
            producer_group: "producers".into(),
            topic: "orders".into(),
            default_topic: "TBW102".into(),
            default_topic_queue_nums: 4,
            queue_id: 2,
            sys_flag: 1,
            born_timestamp: 5,
            flag: 7,
            properties: Some("CORRELATION_ID=correlation-1".into()),
            reconsume_times: None,
            unit_mode: false,
            born_host: "127.0.0.1:1001".into(),
            store_host: "127.0.0.1:10911".into(),
            store_timestamp: 6,
        },
        binary_reply,
    );
    write_typed(
        output,
        "typed-pull-response.bin",
        0,
        &PullMessageResponseHeader {
            suggest_which_broker_id: 0,
            next_begin_offset: 10,
            min_offset: 0,
            max_offset: 100,
            ..PullMessageResponseHeader::default()
        },
    );
    write_typed(
        output,
        "typed-send-response.bin",
        0,
        &SendMessageResponseHeader {
            msg_id: "msg-1".into(),
            queue_id: 2,
            queue_offset: 99,
            ..SendMessageResponseHeader::default()
        },
    );
    write_typed(
        output,
        "typed-notification.bin",
        200054,
        &NotificationRequestHeader {
            consumer_group: "workers".into(),
            topic: "orders".into(),
            queue_id: 1,
            poll_time: 1000,
            born_time: 1_712_345_678_901,
            order: Some(false),
            ..NotificationRequestHeader::default()
        },
    );
    write_typed(
        output,
        "typed-notification-response.bin",
        0,
        &NotificationResponseHeader {
            has_msg: true,
            polling_full: false,
        },
    );
    write_typed(
        output,
        "typed-reset-offset.bin",
        211,
        &ResetOffsetRequestHeader {
            group: "workers".into(),
            topic: "orders".into(),
            queue_id: 1,
            offset: Some(9),
            timestamp: 1_712_345_678_901,
            force: true,
        },
    );
    write_typed(
        output,
        "typed-pop.bin",
        200050,
        &PopMessageRequestHeader {
            consumer_group: "workers".into(),
            topic: "orders".into(),
            queue_id: 1,
            max_msg_nums: 16,
            invisible_time: 30_000,
            poll_time: 1_000,
            born_time: 1_712_345_678_901,
            init_mode: 0,
            order: Some(false),
            ..PopMessageRequestHeader::default()
        },
    );
    write_typed(
        output,
        "typed-pop-lite-response.bin",
        0,
        &PopLiteMessageResponseHeader {
            pop_time: 10,
            invisible_time: 20,
            revive_queue_id: 1,
            start_offset_info: Some("orders@0:100".into()),
            msg_offset_info: Some("orders@0:101".into()),
            order_count_info: Some("0".into()),
        },
    );
    write_typed(
        output,
        "typed-pop-response.bin",
        0,
        &PopMessageResponseHeader {
            pop_time: 10,
            invisible_time: 20,
            revive_queue_id: 1,
            rest_num: 3,
            start_offset_info: Some("orders@0:100".into()),
            msg_offset_info: Some("orders@0:101".into()),
            order_count_info: Some("0".into()),
        },
    );
    write_typed(
        output,
        "typed-ack.bin",
        200051,
        &AckMessageRequestHeader {
            consumer_group: "workers".into(),
            topic: "orders".into(),
            queue_id: 1,
            extra_info: "startOffset popTime invisibleTime queueId".into(),
            offset: 9,
            lite_topic: None,
        },
    );
    write_typed(
        output,
        "typed-change-invisible.bin",
        200053,
        &ChangeInvisibleTimeRequestHeader {
            consumer_group: "workers".into(),
            topic: "orders".into(),
            queue_id: 1,
            extra_info: "startOffset popTime invisibleTime queueId".into(),
            offset: 9,
            invisible_time: 60_000,
            lite_topic: None,
            suspend: false,
        },
    );
}

fn write_oracle_header<H: CommandHeader>(
    output: &std::path::Path,
    name: &str,
    stem: &str,
    code: i32,
) {
    let path = format!(
        "{}/../../fixtures/remoting/{stem}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let metadata: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).expect("read Java header oracle"))
            .expect("parse Java header oracle");
    assert_eq!(metadata["java_client_version"], "5.5.1", "{stem}");
    assert_eq!(metadata["request_code"], code, "{stem}");
    let fields = serde_json::from_value(metadata["normalized_fields"]["ext_fields"].clone())
        .expect("parse Java ext fields");
    let header = H::decode_fields(&fields).expect("decode Java header oracle");
    write_typed(output, name, code, &header);
}

fn write_typed(output: &std::path::Path, name: &str, code: i32, header: &impl CommandHeader) {
    write_typed_body(output, name, code, header, Vec::new());
}

fn write_typed_body(
    output: &std::path::Path,
    name: &str,
    code: i32,
    header: &impl CommandHeader,
    body: Vec<u8>,
) {
    let mut ext_fields = std::collections::BTreeMap::new();
    header.encode_fields(&mut ext_fields);
    let command = RemotingCommand {
        code,
        language: Some(LanguageCode::Rust),
        opaque: 101,
        ext_fields: Some(ext_fields),
        body: if body.is_empty() { None } else { Some(body) },
        ..RemotingCommand::default()
    };
    fs::write(
        output.join(name),
        frame::encode(&command, frame::DEFAULT_MAX_FRAME_SIZE).expect("encode typed header"),
    )
    .expect("write typed header fixture");
}

fn write_body(output: &std::path::Path, name: &str, code: i32, body: &impl serde::Serialize) {
    write_body_bytes(
        output,
        name,
        code,
        serde_json::to_vec(body).expect("serialize typed body"),
    );
}

fn write_body_bytes(output: &std::path::Path, name: &str, code: i32, body: Vec<u8>) {
    let command = RemotingCommand {
        code,
        language: Some(LanguageCode::Rust),
        opaque: 102,
        body: Some(body),
        ..RemotingCommand::default()
    };
    fs::write(
        output.join(name),
        frame::encode(&command, frame::DEFAULT_MAX_FRAME_SIZE).expect("encode typed body frame"),
    )
    .expect("write typed body fixture");
}

fn generate_p4_bodies(output: &std::path::Path) {
    write_body(
        output,
        "body-query-assignment.bin",
        400,
        &QueryAssignmentRequestBody {
            topic: Some("orders".into()),
            consumer_group: Some("workers".into()),
            client_id: Some("client".into()),
            strategy_name: Some("AllocateMessageQueueAveragely".into()),
            message_model: Some(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering),
        },
    );
    write_body(
        output,
        "body-set-request-mode.bin",
        401,
        &SetMessageRequestModeRequestBody {
            topic: Some("orders".into()),
            consumer_group: Some("workers".into()),
            mode: Some(MessageRequestMode::Pop),
            pop_share_queue_num: 2,
        },
    );
    write_body(
        output,
        "body-lite-subscription.bin",
        200071,
        &LiteSubscriptionCtlRequestBody {
            subscription_set: Some(vec![LiteSubscriptionDto {
                action: Some(LiteSubscriptionAction::CompleteAdd),
                client_id: Some("client".into()),
                group: Some("workers".into()),
                topic: Some("orders".into()),
                lite_topic_set: Some(vec!["lite-orders".into()]),
                offset_option: Some(OffsetOption {
                    option_type: Some(OffsetOptionType::Policy),
                    value: 1,
                }),
                version: 2,
            }]),
        },
    );
    write_body(
        output,
        "body-batch-ack.bin",
        200151,
        &BatchAckMessageRequestBody {
            broker_name: Some("broker-a".into()),
            acks: Some(vec![BatchAck {
                consumer_group: Some("workers".into()),
                topic: Some("orders".into()),
                retry: Some("0".into()),
                start_offset: 100,
                queue_id: 1,
                revive_queue_id: 0,
                pop_time: 10,
                invisible_time: 20,
                bit_set: Some(vec![0b0000_0101]),
            }]),
        },
    );
    write_body_bytes(
        output,
        "body-reset-offset.bin",
        212,
        ResetOffsetBody {
            offset_table: Some(vec![(
                rocketmq_classic_common::message::MessageQueue::new("orders", "broker-a", 1),
                9,
            )]),
        }
        .encode_java_json()
        .expect("encode Java-compatible reset body"),
    );
    write_body_bytes(
        output,
        "body-consumer-status.bin",
        223,
        GetConsumerStatusBody {
            message_queue_table: vec![(
                rocketmq_classic_common::message::MessageQueue::new("orders", "broker-a", 1),
                9,
            )],
            consumer_table: std::collections::BTreeMap::new(),
        }
        .encode_java_json()
        .expect("encode Java-compatible consumer status body"),
    );
    write_body(
        output,
        "body-consume-directly.bin",
        309,
        &ConsumeMessageDirectlyResult {
            order: false,
            auto_commit: true,
            consume_result: Some(ConsumeMessageResult::CrSuccess),
            remark: None,
            spent_time_mills: 2,
        },
    );
    let queue = rocketmq_classic_common::message::MessageQueue::new("orders", "broker-a", 1);
    for (name, body) in [
        (
            "body-consumer-list-absent.bin",
            ConsumerListByGroupResponseBody {
                consumer_id_list: None,
            },
        ),
        (
            "body-consumer-list-empty.bin",
            ConsumerListByGroupResponseBody {
                consumer_id_list: Some(vec![]),
            },
        ),
        (
            "body-consumer-list-populated.bin",
            ConsumerListByGroupResponseBody {
                consumer_id_list: Some(vec![Some("client-a".into()), Some("client-b".into())]),
            },
        ),
    ] {
        write_body(output, name, 38, &body);
    }
    write_body(
        output,
        "body-lock-request.bin",
        41,
        &LockBatchRequestBody {
            consumer_group: Some("workers".into()),
            client_id: Some("client".into()),
            only_this_broker: false,
            mq_set: vec![queue.clone()],
        },
    );
    write_body(
        output,
        "body-unlock-request.bin",
        42,
        &UnlockBatchRequestBody {
            consumer_group: Some("workers".into()),
            client_id: Some("client".into()),
            only_this_broker: true,
            mq_set: vec![queue.clone()],
        },
    );
    write_body(
        output,
        "body-lock-response.bin",
        41,
        &LockBatchResponseBody {
            lock_okmq_set: vec![queue.clone()],
        },
    );
    write_body(
        output,
        "body-unlock-response.bin",
        42,
        &LockBatchResponseBody {
            lock_okmq_set: vec![queue],
        },
    );
}
