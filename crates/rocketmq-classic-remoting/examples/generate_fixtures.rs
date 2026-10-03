use std::{env, fs, path::PathBuf};

use rocketmq_classic_protocol::{
    codes::{LanguageCode, SerializeType},
    header::{
        CommandHeader, GetRouteInfoRequestHeader, PullMessageRequestHeader,
        QueryConsumerOffsetRequestHeader, SendMessageRequestHeaderV1, SendMessageRequestHeaderV2,
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
    for (name, code, serialization) in [
        ("json-route", 105, SerializeType::Json),
        ("rocketmq-transaction", 37, SerializeType::RocketMq),
    ] {
        let command = RemotingCommand {
            code,
            language: LanguageCode::Rust,
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
}

fn write_typed(output: &std::path::Path, name: &str, code: i32, header: &impl CommandHeader) {
    let mut ext_fields = std::collections::BTreeMap::new();
    header.encode_fields(&mut ext_fields);
    let command = RemotingCommand {
        code,
        language: LanguageCode::Rust,
        opaque: 101,
        ext_fields: Some(ext_fields),
        ..RemotingCommand::default()
    };
    fs::write(
        output.join(name),
        frame::encode(&command, frame::DEFAULT_MAX_FRAME_SIZE).expect("encode typed header"),
    )
    .expect("write typed header fixture");
}
