use std::{fs, path::PathBuf};

use bytes::BytesMut;
use rocketmq_classic_protocol::{codes::SerializeType, header::CommandHeader};
use rocketmq_classic_remoting::{
    CodecError,
    codec::{
        frame::{self, DEFAULT_MAX_FRAME_SIZE},
        json_header, rocketmq_header,
    },
    command::RemotingCommand,
};

#[test]
fn nullable_language_matches_invoked_java_json_and_binary_boundaries() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["nullableSettings"];
    let command = RemotingCommand {
        code: 34,
        language: None,
        ..Default::default()
    };
    let json = json_header::encode(&command, DEFAULT_MAX_FRAME_SIZE).unwrap();
    let fields: serde_json::Value = serde_json::from_slice(&json).unwrap();
    assert_eq!(
        fields.get("language").is_some(),
        java["JSON"]["languageKeyPresent"]
    );
    assert_eq!(
        json_header::decode(&json).unwrap().language,
        Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
    );
    assert_eq!(java["JSON"]["decodedLanguage"], "JAVA");
    let unknown_language = json_header::decode(br#"{"code":34,"language":"not-a-language"}"#)
        .unwrap()
        .language;
    assert_eq!(
        unknown_language.is_none(),
        java["JSON"]["unknownLanguageDecodedNull"]
    );
    assert_eq!(
        unknown_language, None,
        "Java LanguageCode.getCode(String) returns null for an unknown name"
    );
    let explicit_null = br#"{"code":34,"language":null}"#;
    assert_eq!(
        json_header::decode(explicit_null)
            .unwrap()
            .language
            .is_none(),
        java["JSON"]["explicitNullDecodedLanguageNull"]
    );
    assert!(rocketmq_header::encode(&command, DEFAULT_MAX_FRAME_SIZE).is_err());
    assert_eq!(java["ROCKETMQ"]["outcome"], "NullPointerException");
}

#[test]
fn unknown_binary_language_code_matches_java_value_of_byte_null() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let unknown = &fixture["languageCodeMethodCases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["case"] == "byte" && case["input"] == -1)
        .unwrap()["decoded"];
    assert!(unknown.is_null());

    let command = RemotingCommand {
        language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
        ..Default::default()
    };
    let mut bytes = rocketmq_header::encode(&command, DEFAULT_MAX_FRAME_SIZE).unwrap();
    bytes[2] = u8::MAX;
    assert_eq!(rocketmq_header::decode(&bytes).unwrap().language, None);
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/remoting")
}

#[test]
fn p2_v01_java_fixtures_cross_decode_for_both_wire_serializations() {
    let mut seen_json = false;
    let mut seen_rocketmq = false;
    for entry in fs::read_dir(fixture_dir()).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|extension| extension != "bin") {
            continue;
        }
        let frame_bytes = fs::read(&path).unwrap();
        let mut input = BytesMut::from(frame_bytes.as_slice());
        let command = frame::decode_next(&mut input, DEFAULT_MAX_FRAME_SIZE)
            .unwrap()
            .unwrap();
        assert!(input.is_empty());
        if path
            .file_name()
            .is_some_and(|name| name == "route-001-get-existing-topic.bin")
        {
            assert_eq!(
                command
                    .ext_fields
                    .as_ref()
                    .unwrap()
                    .get("oracle")
                    .map(String::as_str),
                Some("5.5.1")
            );
        }
        match command.serialization {
            SerializeType::Json => seen_json = true,
            SerializeType::RocketMq => seen_rocketmq = true,
            SerializeType::Unknown(code) => panic!("unknown serialization {code}"),
        }
    }
    assert!(seen_json && seen_rocketmq);
    let private_large_code = fs::read(fixture_dir().join("private-large-code.bin")).unwrap();
    let mut input = BytesMut::from(private_large_code.as_slice());
    assert_eq!(
        frame::decode_next(&mut input, DEFAULT_MAX_FRAME_SIZE)
            .unwrap()
            .unwrap()
            .code,
        4_614
    );
    let unknown_code = RemotingCommand {
        code: 987_654,
        ..RemotingCommand::default()
    };
    let unknown_frame = frame::encode(&unknown_code, DEFAULT_MAX_FRAME_SIZE).unwrap();
    let mut input = BytesMut::from(unknown_frame.as_slice());
    assert_eq!(
        frame::decode_next(&mut input, DEFAULT_MAX_FRAME_SIZE)
            .unwrap()
            .unwrap()
            .code,
        987_654
    );
}

#[test]
fn p2_v01_private_codec_matches_java_fast_and_ordinary_header_layouts() {
    let proof_bytes = fs::read(fixture_dir().join("header-proof-001.bin")).unwrap();
    let mut proof_input = BytesMut::from(proof_bytes.as_slice());
    let proof = frame::decode_next(&mut proof_input, DEFAULT_MAX_FRAME_SIZE)
        .unwrap()
        .unwrap();
    let raw_fields = proof.ext_fields.as_ref().unwrap();
    let typed = rocketmq_classic_protocol::header::ProofHeader::decode_fields(raw_fields).unwrap();
    assert_eq!(typed.topic, "fixture-topic");
    assert_eq!(raw_fields.get("oracle").map(String::as_str), Some("5.5.1"));
    for name in ["header-fast-001.bin", "header-ordinary-001.bin"] {
        let bytes = fs::read(fixture_dir().join(name)).unwrap();
        let mut input = BytesMut::from(bytes.as_slice());
        let command = frame::decode_next(&mut input, DEFAULT_MAX_FRAME_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(command.remark.as_deref(), Some("header-equivalent"));
        assert_eq!(
            command.ext_fields.as_ref().unwrap().get("topic").unwrap(),
            "fixture-topic"
        );
    }
    let fast = fs::read(fixture_dir().join("header-fast-001.bin")).unwrap();
    let ordinary = fs::read(fixture_dir().join("header-ordinary-001.bin")).unwrap();
    let mut fast = BytesMut::from(fast.as_slice());
    let mut ordinary = BytesMut::from(ordinary.as_slice());
    assert_eq!(
        frame::decode_next(&mut fast, DEFAULT_MAX_FRAME_SIZE).unwrap(),
        frame::decode_next(&mut ordinary, DEFAULT_MAX_FRAME_SIZE).unwrap()
    );
}

#[test]
fn p2_v03_every_split_and_concatenated_frames_preserve_incomplete_bytes() {
    let command = RemotingCommand {
        code: 34,
        remark: Some("split-test".into()),
        body: Some(vec![1, 2, 3]),
        ..RemotingCommand::default()
    };
    let bytes = frame::encode(&command, DEFAULT_MAX_FRAME_SIZE).unwrap();
    for split in 0..bytes.len() {
        let mut input = BytesMut::from(&bytes[..split]);
        let before = input.clone();
        assert!(
            frame::decode_next(&mut input, DEFAULT_MAX_FRAME_SIZE)
                .unwrap()
                .is_none()
        );
        assert_eq!(input, before, "input changed at split {split}");
        input.extend_from_slice(&bytes[split..]);
        assert_eq!(
            frame::decode_next(&mut input, DEFAULT_MAX_FRAME_SIZE).unwrap(),
            Some(command.clone())
        );
        assert!(input.is_empty());
    }
    let mut joined = BytesMut::from(bytes.as_slice());
    joined.extend_from_slice(&bytes);
    assert_eq!(
        frame::decode_next(&mut joined, DEFAULT_MAX_FRAME_SIZE).unwrap(),
        Some(command.clone())
    );
    assert_eq!(
        frame::decode_next(&mut joined, DEFAULT_MAX_FRAME_SIZE).unwrap(),
        Some(command)
    );
    assert!(joined.is_empty());
}

#[test]
fn p2_v02_rejects_invalid_lengths_limits_and_unknown_serialization() {
    for declared in [0u32, 1, 3, 0x8000_0000, u32::MAX] {
        let mut input = BytesMut::from(declared.to_be_bytes().as_slice());
        assert!(frame::decode_next(&mut input, DEFAULT_MAX_FRAME_SIZE).is_err());
    }
    let mut too_large = BytesMut::from((DEFAULT_MAX_FRAME_SIZE as u32).to_be_bytes().as_slice());
    assert!(matches!(
        frame::decode_next(&mut too_large, DEFAULT_MAX_FRAME_SIZE),
        Err(CodecError::FrameTooLarge { .. })
    ));
    let max_wire_length = 0x00ff_ffffu32;
    let mut max_frame_prefix = BytesMut::from(max_wire_length.to_be_bytes().as_slice());
    assert!(matches!(
        frame::decode_next(&mut max_frame_prefix, 0x0100_0003),
        Ok(None)
    ));
    let mut invalid_serialization = BytesMut::from(&[0, 0, 0, 4, 2, 0, 0, 0][..]);
    assert!(matches!(
        frame::decode_next(&mut invalid_serialization, DEFAULT_MAX_FRAME_SIZE),
        Err(CodecError::UnsupportedSerialization(2))
    ));
    let mut header_larger_than_payload = BytesMut::from(&[0, 0, 0, 4, 0, 0xff, 0xff, 0xff][..]);
    assert!(matches!(
        frame::decode_next(&mut header_larger_than_payload, DEFAULT_MAX_FRAME_SIZE),
        Err(CodecError::HeaderTooLarge { .. })
    ));
    let mut truncated_nested_header = BytesMut::from(&[0, 0, 0, 28, 1, 0, 0, 24][..]);
    truncated_nested_header.extend_from_slice(&[0, 10, 12, 0, 0]);
    truncated_nested_header.extend_from_slice(&0i32.to_be_bytes());
    truncated_nested_header.extend_from_slice(&0i32.to_be_bytes());
    truncated_nested_header.extend_from_slice(&0i32.to_be_bytes());
    truncated_nested_header.extend_from_slice(&5i32.to_be_bytes());
    truncated_nested_header.extend_from_slice(&[1, 2, 3]);
    assert!(matches!(
        frame::decode_next(&mut truncated_nested_header, DEFAULT_MAX_FRAME_SIZE),
        Err(CodecError::MalformedHeader("RocketMQ", _))
    ));
    let mut command_frame = BytesMut::from(&[0, 0, 0, 32, 1, 0, 0, 28][..]);
    command_frame.extend_from_slice(&[0, 10, 12, 0, 0]);
    command_frame.extend_from_slice(&0i32.to_be_bytes());
    command_frame.extend_from_slice(&0i32.to_be_bytes());
    command_frame.extend_from_slice(&0i32.to_be_bytes());
    command_frame.extend_from_slice(&7i32.to_be_bytes());
    command_frame.extend_from_slice(&[0, 1, 0xff]);
    command_frame.extend_from_slice(&0i32.to_be_bytes());
    assert!(matches!(
        frame::decode_next(&mut command_frame, DEFAULT_MAX_FRAME_SIZE),
        Err(CodecError::InvalidUtf8)
    ));
    let command = RemotingCommand::default();
    let encoded = frame::encode(&command, DEFAULT_MAX_FRAME_SIZE).unwrap();
    assert!(frame::encode(&command, encoded.len() - 1).is_err());
    assert_eq!(frame::encode(&command, encoded.len()).unwrap(), encoded);
    let too_wide = RemotingCommand {
        code: 32_768,
        serialization: SerializeType::RocketMq,
        ..RemotingCommand::default()
    };
    assert!(matches!(
        frame::encode(&too_wide, DEFAULT_MAX_FRAME_SIZE),
        Err(CodecError::FieldTooWide("code"))
    ));
}
