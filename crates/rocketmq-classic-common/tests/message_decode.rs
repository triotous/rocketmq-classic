use base64::Engine;
use rocketmq_classic_common::{
    compression::{compress, decompress_limited},
    message::{DecodeError, DecodeLimits, DecodeOptions, decode_many, decode_one},
    sysflag::message::{self, CompressionType},
};
use serde_json::Value;

fn fixture() -> Value {
    serde_json::from_str(include_str!("../../../fixtures/messages/records.json")).unwrap()
}

fn record(case: &Value) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD
        .decode(case["recordBase64"].as_str().unwrap())
        .unwrap()
}

fn encoded_body(record: &[u8]) -> &[u8] {
    let sys_flag = i32::from_be_bytes(record[36..40].try_into().unwrap());
    let born_host_length = if sys_flag & message::BORNHOST_V6_FLAG == 0 {
        8
    } else {
        20
    };
    let store_host_length = if sys_flag & message::STOREHOSTADDRESS_V6_FLAG == 0 {
        8
    } else {
        20
    };
    let body_length_offset =
        4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + born_host_length + 8 + store_host_length + 4 + 8;
    let body_length = i32::from_be_bytes(
        record[body_length_offset..body_length_offset + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    &record[body_length_offset + 4..body_length_offset + 4 + body_length]
}

#[test]
fn java_551_message_records_decode_to_the_pinned_results() {
    let fixture = fixture();
    assert_eq!(
        fixture["provenance"]["sourceCommit"],
        "efa1be59d6fc747ad220f036c86030f05f0419f9"
    );
    for case in fixture["cases"].as_array().unwrap() {
        let bytes = record(case);
        let (decoded, consumed) = decode_one(
            &bytes,
            DecodeOptions {
                verify_crc: true,
                ..DecodeOptions::default()
            },
            DecodeLimits::default(),
        )
        .unwrap_or_else(|error| panic!("{}: {error}", case["name"]));
        let expected = &case["decoded"];
        assert_eq!(consumed, bytes.len(), "{}", case["name"]);
        assert_eq!(decoded.message().topic(), expected["topic"]);
        assert_eq!(
            decoded.message().body(),
            base64::engine::general_purpose::STANDARD
                .decode(expected["bodyBase64"].as_str().unwrap())
                .unwrap()
        );
        assert_eq!(decoded.message().flag(), expected["flag"]);
        assert_eq!(decoded.body_crc(), expected["bodyCRC"]);
        assert_eq!(decoded.queue_id(), expected["queueId"]);
        assert_eq!(decoded.queue_offset(), expected["queueOffset"]);
        assert_eq!(decoded.commit_log_offset(), expected["commitLogOffset"]);
        assert_eq!(decoded.sys_flag(), expected["sysFlag"]);
        assert_eq!(decoded.born_timestamp(), expected["bornTimestamp"]);
        assert_eq!(decoded.store_timestamp(), expected["storeTimestamp"]);
        assert_eq!(decoded.born_host().unwrap(), expected["bornHost"]);
        assert_eq!(decoded.store_host().unwrap(), expected["storeHost"]);
        assert_eq!(decoded.reconsume_times(), expected["reconsumeTimes"]);
        assert_eq!(
            decoded.prepared_transaction_offset(),
            expected["preparedTransactionOffset"]
        );
        assert_eq!(decoded.message_id().unwrap(), expected["msgId"]);
        assert_eq!(
            serde_json::to_value(decoded.message().properties()).unwrap(),
            expected["properties"]
        );
        assert_eq!(decoded.store_size() as usize, bytes.len());
    }
}

#[test]
fn decode_many_handles_concatenated_records_and_requires_complete_progress() {
    let fixture = fixture();
    let cases = fixture["cases"].as_array().unwrap();
    let joined: Vec<u8> = cases.iter().flat_map(record).collect();
    let decoded = decode_many(&joined, DecodeOptions::default(), DecodeLimits::default()).unwrap();
    assert_eq!(decoded.len(), cases.len());
    let mut offset = 0;
    for case in cases {
        let bytes = record(case);
        for end in 0..bytes.len() {
            assert!(
                decode_one(
                    &bytes[..end],
                    DecodeOptions::default(),
                    DecodeLimits::default()
                )
                .is_err(),
                "{} prefix length {end}",
                case["name"]
            );
        }
        offset += bytes.len();
    }
    assert_eq!(offset, joined.len());
}

#[test]
fn body_decode_options_match_java_and_limits_reject_oversized_fields() {
    let fixture = fixture();
    let cases = fixture["cases"].as_array().unwrap();
    for case in cases {
        let bytes = record(case);
        for option in case["decodeOptions"].as_array().unwrap() {
            let decoded = decode_one(
                &bytes,
                DecodeOptions {
                    read_body: option["readBody"].as_bool().unwrap(),
                    decompress_body: option["decompressBody"].as_bool().unwrap(),
                    set_properties_string: false,
                    verify_crc: true,
                },
                DecodeLimits::default(),
            )
            .unwrap_or_else(|error| panic!("{}: {error}", case["name"]))
            .0;
            if let Some(expected_body) = option["bodyBase64"].as_str() {
                assert_eq!(
                    decoded.message().body(),
                    base64::engine::general_purpose::STANDARD
                        .decode(expected_body)
                        .unwrap(),
                    "{} readBody={} decompressBody={}",
                    case["name"],
                    option["readBody"],
                    option["decompressBody"]
                );
            } else {
                assert!(
                    decoded.message().body().is_empty(),
                    "{} readBody={} decompressBody={}",
                    case["name"],
                    option["readBody"],
                    option["decompressBody"]
                );
            }
            assert_eq!(
                decoded.sys_flag(),
                option["sysFlag"].as_i64().unwrap() as i32
            );
        }
        let with_properties_string = decode_one(
            &bytes,
            DecodeOptions {
                set_properties_string: true,
                verify_crc: true,
                ..DecodeOptions::default()
            },
            DecodeLimits::default(),
        )
        .unwrap_or_else(|error| panic!("{}: {error}", case["name"]))
        .0;
        assert_eq!(
            serde_json::to_value(with_properties_string.message().properties()).unwrap(),
            case["decoded"]["propertiesWithString"],
            "{} propertiesString",
            case["name"]
        );
    }

    for case in cases.iter().filter(|case| {
        case["name"]
            .as_str()
            .is_some_and(|name| name.starts_with("compressed-"))
    }) {
        let compressed = record(case);
        let preserved = decode_one(
            &compressed,
            DecodeOptions {
                read_body: true,
                decompress_body: false,
                set_properties_string: false,
                verify_crc: true,
            },
            DecodeLimits::default(),
        )
        .unwrap_or_else(|error| panic!("{}: {error}", case["name"]))
        .0;
        assert_ne!(preserved.sys_flag() & message::COMPRESSED_FLAG, 0);
        assert_eq!(preserved.message().body(), encoded_body(&compressed));
        for decompress_body in [false, true] {
            let skipped = decode_one(
                &compressed,
                DecodeOptions {
                    read_body: false,
                    decompress_body,
                    set_properties_string: false,
                    verify_crc: true,
                },
                DecodeLimits::default(),
            )
            .unwrap_or_else(|error| panic!("{}: {error}", case["name"]))
            .0;
            assert!(skipped.message().body().is_empty());
            assert_ne!(skipped.sys_flag() & message::COMPRESSED_FLAG, 0);
        }
    }

    let topic_record = record(&cases[0]);
    let sys_flag = i32::from_be_bytes(topic_record[36..40].try_into().unwrap());
    let born_host_length = if sys_flag & message::BORNHOST_V6_FLAG == 0 {
        8
    } else {
        20
    };
    let store_host_length = if sys_flag & message::STOREHOSTADDRESS_V6_FLAG == 0 {
        8
    } else {
        20
    };
    let body_length_offset =
        4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + born_host_length + 8 + store_host_length + 4 + 8;
    let body_length = i32::from_be_bytes(
        topic_record[body_length_offset..body_length_offset + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    let topic_length_offset = body_length_offset + 4 + body_length;
    let mut invalid_topic_size = topic_record;
    invalid_topic_size[topic_length_offset] = i8::MAX as u8;
    assert!(matches!(
        decode_one(
            &invalid_topic_size,
            DecodeOptions::default(),
            DecodeLimits::default()
        ),
        Err(DecodeError::Truncated { field: "topic", .. })
    ));
    let limits = DecodeLimits {
        max_decompressed_body_bytes: 12,
        ..DecodeLimits::default()
    };
    for case in cases.iter().filter(|case| {
        case["name"]
            .as_str()
            .is_some_and(|name| name.starts_with("compressed-"))
    }) {
        assert!(
            matches!(
                decode_one(&record(case), DecodeOptions::default(), limits),
                Err(DecodeError::LimitExceeded {
                    field: "decompressed body",
                    ..
                })
            ),
            "{}",
            case["name"]
        );
    }

    let mut empty_compressed = record(
        cases
            .iter()
            .find(|case| case["name"] == "compressed-zlib-v1")
            .unwrap(),
    );
    let body_length_offset = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 8 + 8 + 8 + 4 + 8;
    let body_length = i32::from_be_bytes(
        empty_compressed[body_length_offset..body_length_offset + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    empty_compressed.drain(body_length_offset + 4..body_length_offset + 4 + body_length);
    let empty_record_size = empty_compressed.len() as i32;
    empty_compressed[0..4].copy_from_slice(&empty_record_size.to_be_bytes());
    empty_compressed[body_length_offset..body_length_offset + 4]
        .copy_from_slice(&0_i32.to_be_bytes());
    empty_compressed[8..12].copy_from_slice(&123_i32.to_be_bytes());
    let decoded_empty = decode_one(
        &empty_compressed,
        DecodeOptions {
            verify_crc: true,
            ..DecodeOptions::default()
        },
        DecodeLimits::default(),
    )
    .unwrap()
    .0;
    assert!(decoded_empty.message().body().is_empty());
    assert_ne!(decoded_empty.sys_flag() & message::COMPRESSED_FLAG, 0);

    let limits = DecodeLimits {
        max_total_decoded_bytes: 16,
        ..DecodeLimits::default()
    };
    assert!(matches!(
        decode_many(&record(&cases[0]), DecodeOptions::default(), limits),
        Err(DecodeError::AggregateLimitExceeded { .. })
    ));
}

#[test]
fn malformed_magic_lengths_and_crc_fail_as_structured_errors() {
    let case = &fixture()["cases"][0];
    let valid = record(case);
    for end in 0..valid.len() {
        assert!(
            decode_one(
                &valid[..end],
                DecodeOptions::default(),
                DecodeLimits::default()
            )
            .is_err()
        );
    }
    let mut bad_magic = valid.clone();
    bad_magic[4..8].copy_from_slice(&0_i32.to_be_bytes());
    assert!(matches!(
        decode_one(
            &bad_magic,
            DecodeOptions::default(),
            DecodeLimits::default()
        ),
        Err(DecodeError::UnknownMagic { .. })
    ));
    let mut bad_store_size = valid.clone();
    bad_store_size[..4].copy_from_slice(&i32::MAX.to_be_bytes());
    assert!(matches!(
        decode_one(
            &bad_store_size,
            DecodeOptions::default(),
            DecodeLimits::default()
        ),
        Err(DecodeError::LimitExceeded {
            field: "record",
            ..
        })
    ));
    let mut bad_body_length = valid.clone();
    let body_length_offset = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 8 + 8 + 8 + 4 + 8;
    bad_body_length[body_length_offset..body_length_offset + 4]
        .copy_from_slice(&i32::MAX.to_be_bytes());
    assert!(
        decode_one(
            &bad_body_length,
            DecodeOptions::default(),
            DecodeLimits::default()
        )
        .is_err()
    );
    let mut bad_properties_length = valid.clone();
    let body_length_offset = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 8 + 8 + 8 + 4 + 8;
    let body_length = i32::from_be_bytes(
        valid[body_length_offset..body_length_offset + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    let topic_length_offset = body_length_offset + 4 + body_length;
    let topic_length = valid[topic_length_offset] as usize;
    let properties_length_offset = topic_length_offset + 1 + topic_length;
    bad_properties_length[properties_length_offset..properties_length_offset + 2]
        .copy_from_slice(&(-1_i16).to_be_bytes());
    assert!(matches!(
        decode_one(
            &bad_properties_length,
            DecodeOptions::default(),
            DecodeLimits::default()
        ),
        Err(DecodeError::NegativeLength {
            field: "properties",
            ..
        })
    ));
    let compressed = record(
        fixture()["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "compressed-zstd-v1")
            .unwrap(),
    );
    let mut unknown_compression = compressed;
    let sys_flag = i32::from_be_bytes(unknown_compression[36..40].try_into().unwrap());
    unknown_compression[36..40].copy_from_slice(
        &((sys_flag & !message::COMPRESSION_TYPE_COMPARATOR) | (4 << 8)).to_be_bytes(),
    );
    assert!(matches!(
        decode_one(
            &unknown_compression,
            DecodeOptions::default(),
            DecodeLimits::default()
        ),
        Err(DecodeError::CompressionType(_))
    ));
    let mut bad_crc = valid.clone();
    bad_crc[8..12].copy_from_slice(&0_i32.to_be_bytes());
    assert!(matches!(
        decode_one(
            &bad_crc,
            DecodeOptions {
                read_body: true,
                decompress_body: true,
                set_properties_string: false,
                verify_crc: true
            },
            DecodeLimits::default()
        ),
        Err(DecodeError::CrcMismatch)
    ));
}

#[test]
fn all_message_codecs_stop_when_the_decompressed_output_cap_is_exceeded() {
    let input = vec![b'X'; 32 * 1024];
    for codec in CompressionType::values() {
        let encoded = compress(codec, &input, 3).unwrap();
        assert!(decompress_limited(codec, &encoded, 128).is_err(), "{codec}");
    }
}
