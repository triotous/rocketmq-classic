use base64::Engine;
use rocketmq_classic_common::{
    message::{
        DecodeError, DecodeOptions, Message, MessageExt, MessageHostAddress,
        MessageHostAddressBytesError, TopicFilterType, decode_many, decode_one,
    },
    sysflag::message,
};
use serde_json::Value;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

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
        assert_eq!(
            decoded.born_host().unwrap().to_string(),
            expected["bornHost"]
        );
        assert_eq!(
            decoded.store_host().unwrap().to_string(),
            expected["storeHost"]
        );
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
fn message_ext_socket_address_api_matches_java_551() {
    let fixture = fixture();
    let expected = &fixture["messageExtHostApi"];
    let born = MessageHostAddress::from_ip_address(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)), 10911);
    let store = MessageHostAddress::unresolved("broker.invalid", 10912);
    let mut message = MessageExt::default();
    message.set_born_host(Some(born.clone()));
    message.set_store_host(Some(store.clone()));

    assert_eq!(expected["bornHostClass"], "java.net.InetSocketAddress");
    assert_eq!(born.to_string(), expected["bornHostToString"]);
    assert_eq!(
        message.born_host_string().as_deref(),
        expected["bornHostString"].as_str()
    );
    assert_eq!(
        message.born_host_name_string(),
        expected["bornHostNameString"].as_str()
    );
    assert_eq!(
        born.socket_address_string(),
        expected["ipv4SocketAddressString"]
            .as_str()
            .map(str::to_owned)
    );
    let born_bytes = born.network_bytes().unwrap();
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(&born_bytes),
        expected["bornHostBytes"]
    );
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(&born_bytes),
        expected["bornHostBufferBytes"]
    );
    assert_eq!(message.born_host_bytes().unwrap(), born_bytes);
    let mut born_output = [0_u8; 8];
    assert_eq!(message.write_born_host_bytes(&mut born_output), Ok(8));
    assert_eq!(born_output.as_slice(), born_bytes);
    let mut short_output = [0_u8; 7];
    assert_eq!(
        message.write_born_host_bytes(&mut short_output),
        Err(MessageHostAddressBytesError::BufferTooSmall {
            needed: 8,
            actual: 7
        })
    );
    assert_eq!(expected["bornHostGetterRetainsObject"], true);
    assert_eq!(expected["storeHostClass"], "java.net.InetSocketAddress");
    assert_eq!(store.to_string(), expected["storeHostToString"]);
    assert!(!store.is_resolved());
    assert_eq!(store.socket_address_string(), None);
    assert_eq!(expected["storeHostUnresolved"], true);
    assert_eq!(expected["storeHostGetterRetainsObject"], true);
    assert_eq!(
        message.store_host_bytes(),
        Err(MessageHostAddressBytesError::UnresolvedAddress)
    );
    assert_eq!(expected["unresolvedHostBytesThrow"], true);
    let constructed = MessageExt::with_socket_addresses(
        7,
        13,
        Some(born.clone()),
        17,
        Some(store.clone()),
        Some("message-id".to_owned()),
    );
    assert_eq!(constructed.queue_id(), expected["constructorQueueId"]);
    assert_eq!(
        constructed.born_timestamp(),
        expected["constructorBornTimestamp"]
    );
    assert_eq!(
        constructed.born_host().unwrap().to_string(),
        expected["constructorBornHost"]
    );
    assert_eq!(
        constructed.store_timestamp(),
        expected["constructorStoreTimestamp"]
    );
    assert_eq!(
        constructed.store_host().unwrap().to_string(),
        expected["constructorStoreHost"]
    );
    assert_eq!(
        constructed.message_id(),
        expected["constructorMessageId"].as_str()
    );

    let ipv6 = MessageHostAddress::from_ip_address(
        IpAddr::V6("2001:db8::20".parse::<Ipv6Addr>().unwrap()),
        10915,
    );
    assert_eq!(ipv6.to_string(), expected["ipv6HostToString"]);
    assert_eq!(
        ipv6.host_address_string().as_deref(),
        expected["ipv6HostString"].as_str()
    );
    assert_eq!(
        ipv6.host_name(),
        expected["ipv6HostNameString"].as_str().unwrap()
    );
    assert_eq!(
        ipv6.socket_address_string(),
        expected["ipv6SocketAddressString"]
            .as_str()
            .map(str::to_owned)
    );
    let ipv6_bytes = ipv6.network_bytes().unwrap();
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(&ipv6_bytes),
        expected["ipv6HostBytes"]
    );
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(&ipv6_bytes),
        expected["ipv6HostBufferBytes"]
    );
    let mut ipv6_message = MessageExt::default();
    ipv6_message.set_store_host(Some(ipv6.clone()));
    assert_eq!(ipv6_message.store_host_bytes().unwrap(), ipv6_bytes);
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(ipv6_message.store_host_bytes().unwrap()),
        expected["ipv6StoreHostBytes"]
    );

    let from_host = MessageHostAddress::new("127.0.0.1", 10913);
    assert_eq!(from_host.to_string(), expected["stringHostToString"]);
    assert_eq!(
        from_host.host_address_string().as_deref(),
        expected["stringHostAddress"].as_str()
    );
    assert_eq!(
        from_host.host_name(),
        expected["stringHostName"].as_str().unwrap()
    );

    let encoded = serde_json::to_value(&message).unwrap();
    assert_eq!(encoded["bornHost"], expected["bornHostToString"]);
    assert_eq!(encoded["storeHost"], expected["storeHostToString"]);
    let decoded: MessageExt = serde_json::from_value(encoded).unwrap();
    assert_eq!(decoded.born_host(), Some(&born));
    assert_eq!(decoded.store_host(), Some(&store));
}

#[test]
fn message_ext_system_flag_helpers_match_java_551() {
    let expected = &fixture()["messageExtSystemFlagsApi"];
    let mut message = MessageExt::default();
    assert_eq!(expected["topicSysFlagAbsentIsNull"], true);
    assert_eq!(expected["groupSysFlagAbsentIsNull"], true);
    assert_eq!(message.topic_sys_flag().unwrap(), None);
    assert_eq!(message.group_sys_flag().unwrap(), None);

    message.set_topic_sys_flag(Some(73));
    message.set_group_sys_flag(Some(-19));
    assert_eq!(message.topic_sys_flag().unwrap(), Some(73));
    assert_eq!(message.group_sys_flag().unwrap(), Some(-19));
    assert_eq!(expected["topicSysFlagSet"], 73);
    assert_eq!(expected["groupSysFlagSet"], -19);
    message.set_topic_sys_flag(None);
    message.set_group_sys_flag(None);
    assert_eq!(expected["topicSysFlagCleared"], true);
    assert_eq!(expected["groupSysFlagCleared"], true);
    assert_eq!(message.topic_sys_flag().unwrap(), None);
    assert_eq!(message.group_sys_flag().unwrap(), None);

    for key in [
        rocketmq_classic_common::message::constants::PROPERTY_TRANSIENT_TOPIC_CONFIG,
        rocketmq_classic_common::message::constants::PROPERTY_TRANSIENT_GROUP_CONFIG,
    ] {
        message.message_mut().set_system_property(key, "").unwrap();
    }
    assert_eq!(expected["topicSysFlagEmptyIsNull"], true);
    assert_eq!(expected["groupSysFlagEmptyIsNull"], true);
    assert_eq!(message.topic_sys_flag().unwrap(), None);
    assert_eq!(message.group_sys_flag().unwrap(), None);

    for key in [
        rocketmq_classic_common::message::constants::PROPERTY_TRANSIENT_TOPIC_CONFIG,
        rocketmq_classic_common::message::constants::PROPERTY_TRANSIENT_GROUP_CONFIG,
    ] {
        message
            .message_mut()
            .set_system_property(key, "bad")
            .unwrap();
    }
    assert_eq!(expected["topicSysFlagMalformedThrows"], true);
    assert_eq!(expected["groupSysFlagMalformedThrows"], true);
    assert!(message.topic_sys_flag().is_err());
    assert!(message.group_sys_flag().is_err());
    message.set_topic_sys_flag(Some(1));
    message
        .message_mut()
        .set_system_property(
            rocketmq_classic_common::message::constants::PROPERTY_TRANSIENT_TOPIC_CONFIG,
            "١",
        )
        .unwrap();
    assert_eq!(
        message.topic_sys_flag().unwrap(),
        Some(expected["topicSysFlagUnicodeDigit"].as_i64().unwrap() as i32)
    );

    message.set_sys_flag(0x40);
    message.set_born_host_v6_flag();
    assert_eq!(message.sys_flag(), expected["bornV6FlagAfterSetter"]);
    message.set_store_host_address_v6_flag();
    assert_eq!(message.sys_flag(), expected["storeV6FlagAfterSetter"]);
    assert_eq!(expected["singleTagFilterType"], "SINGLE_TAG");
    assert_eq!(
        MessageExt::parse_topic_filter_type(0x42),
        TopicFilterType::MultiTag
    );
    assert_eq!(expected["multiTagFilterType"], "MULTI_TAG");
    assert_eq!(
        MessageExt::parse_topic_filter_type(0),
        TopicFilterType::SingleTag
    );

    let mut diagnostic = Message::default();
    diagnostic.set_topic("invoice");
    diagnostic.set_body(vec![0, 255, 42]);
    diagnostic.set_flag(5);
    diagnostic.set_system_property("KEY", "value").unwrap();
    let diagnostic = MessageExt::new(diagnostic);
    assert_eq!(
        diagnostic.to_string(),
        expected["messageExtToString"].as_str().unwrap()
    );
}

#[test]
fn decode_many_handles_concatenated_records_and_requires_complete_progress() {
    let fixture = fixture();
    let cases = fixture["cases"].as_array().unwrap();
    let joined: Vec<u8> = cases.iter().flat_map(record).collect();
    let decoded = decode_many(&joined, DecodeOptions::default()).unwrap();
    assert_eq!(decoded.len(), cases.len());
    let mut offset = 0;
    for case in cases {
        let bytes = record(case);
        for end in 0..bytes.len() {
            assert!(
                decode_one(&bytes[..end], DecodeOptions::default()).is_err(),
                "{} prefix length {end}",
                case["name"]
            );
        }
        offset += bytes.len();
    }
    assert_eq!(offset, joined.len());
}

#[test]
fn body_decode_options_match_java_and_reject_truncated_topics() {
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
        decode_one(&invalid_topic_size, DecodeOptions::default()),
        Err(DecodeError::Truncated { field: "topic", .. })
    ));
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
    )
    .unwrap()
    .0;
    assert!(decoded_empty.message().body().is_empty());
    assert_ne!(decoded_empty.sys_flag() & message::COMPRESSED_FLAG, 0);
}

#[test]
fn malformed_magic_lengths_and_crc_fail_as_structured_errors() {
    let case = &fixture()["cases"][0];
    let valid = record(case);
    for end in 0..valid.len() {
        assert!(decode_one(&valid[..end], DecodeOptions::default()).is_err());
    }
    let mut bad_magic = valid.clone();
    bad_magic[4..8].copy_from_slice(&0_i32.to_be_bytes());
    assert!(matches!(
        decode_one(&bad_magic, DecodeOptions::default()),
        Err(DecodeError::UnknownMagic { .. })
    ));
    let mut bad_store_size = valid.clone();
    bad_store_size[..4].copy_from_slice(&i32::MAX.to_be_bytes());
    assert!(matches!(
        decode_one(&bad_store_size, DecodeOptions::default()),
        Err(DecodeError::LimitExceeded {
            field: "record",
            ..
        })
    ));
    let mut bad_body_length = valid.clone();
    let body_length_offset = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 8 + 8 + 8 + 4 + 8;
    bad_body_length[body_length_offset..body_length_offset + 4]
        .copy_from_slice(&i32::MAX.to_be_bytes());
    assert!(decode_one(&bad_body_length, DecodeOptions::default()).is_err());
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
        decode_one(&bad_properties_length, DecodeOptions::default()),
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
        decode_one(&unknown_compression, DecodeOptions::default()),
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
            }
        ),
        Err(DecodeError::CrcMismatch)
    ));
}
