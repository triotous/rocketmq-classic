use std::{env, fs, path::PathBuf};

use rocketmq_classic_protocol::codes::{LanguageCode, SerializeType};
use rocketmq_classic_remoting::{codec::frame, command::RemotingCommand};

fn main() {
    let output = env::args_os().nth(1).map(PathBuf::from).expect(
        "usage: cargo run -p rocketmq-classic-remoting --example generate_fixtures -- <directory>",
    );
    fs::create_dir_all(&output).expect("create output directory");
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
