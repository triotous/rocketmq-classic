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
fn message_public_api_matches_java_551_fixture() {
    let expected = &fixture()["messagePublicApi"];
    let default_message = Message::default();
    assert_eq!(default_message.topic_nullable(), None);
    assert_eq!(default_message.body_nullable(), None);
    assert_eq!(default_message.get_properties(), None);
    assert_eq!(default_message.to_string(), expected["defaultToString"]);
    assert_eq!(expected["messageEqualsSelf"], true);
    assert_eq!(expected["messageEqualsDistinctSameState"], false);
    assert_eq!(expected["messageExtEqualsSelf"], true);
    assert_eq!(expected["messageExtEqualsDistinctSameState"], false);
    let default_message_ext = MessageExt::default();
    assert_eq!(default_message_ext, default_message_ext);
    assert_ne!(default_message_ext, MessageExt::default());
    let default_after_lookup = default_message.clone();
    assert_eq!(default_after_lookup.get_property("missing"), None);
    assert_eq!(
        default_after_lookup
            .get_properties()
            .map(|properties| properties.is_empty()),
        Some(true)
    );
    assert_eq!(expected["defaultTopicIsNull"], true);
    assert_eq!(expected["defaultBodyIsNull"], true);
    assert_eq!(expected["defaultPropertiesIsNull"], true);
    assert_eq!(expected["propertyLookupInitializesProperties"], true);

    let nullable_constructor = Message::new_nullable(None, None);
    assert_eq!(nullable_constructor.topic_nullable(), None);
    assert_eq!(nullable_constructor.body_nullable(), None);
    assert!(nullable_constructor.is_wait_store_msg_ok());
    assert_eq!(expected["nullableConstructorTopicIsNull"], true);
    assert_eq!(expected["nullableConstructorBodyIsNull"], true);
    assert_eq!(expected["nullableConstructorWaitStore"], true);

    let nullable_tags_constructor = Message::with_tags(None, None, None);
    assert_eq!(nullable_tags_constructor.topic_nullable(), None);
    assert_eq!(nullable_tags_constructor.body_nullable(), None);
    assert!(!nullable_tags_constructor.has_property("TAGS"));
    assert_eq!(expected["nullableTagsConstructorTopicIsNull"], true);
    assert_eq!(expected["nullableTagsConstructorBodyIsNull"], true);
    assert_eq!(expected["nullableTagsConstructorHasTags"], false);

    let nullable_keys_constructor = Message::with_tags_and_keys(None, None, None, None);
    assert_eq!(nullable_keys_constructor.topic_nullable(), None);
    assert_eq!(nullable_keys_constructor.body_nullable(), None);
    assert!(!nullable_keys_constructor.has_property("TAGS"));
    assert!(!nullable_keys_constructor.has_property("KEYS"));
    assert_eq!(expected["nullableKeysConstructorTopicIsNull"], true);
    assert_eq!(expected["nullableKeysConstructorBodyIsNull"], true);
    assert_eq!(expected["nullableKeysConstructorHasTags"], false);
    assert_eq!(expected["nullableKeysConstructorHasKeys"], false);

    let nullable_fields = Message::with_nullable_fields(None, None, None, -3, None, false);
    assert_eq!(nullable_fields.topic_nullable(), None);
    assert_eq!(nullable_fields.body_nullable(), None);
    assert!(!nullable_fields.is_wait_store_msg_ok());
    assert_eq!(expected["nullableFieldsTopicIsNull"], true);
    assert_eq!(expected["nullableFieldsBodyIsNull"], true);
    assert_eq!(expected["nullableFieldsWaitStore"], false);

    let mut nullable_setters = Message::new("before", [1]);
    nullable_setters.set_topic_nullable(None);
    nullable_setters.set_body_nullable(None);
    assert_eq!(nullable_setters.topic_nullable(), None);
    assert_eq!(nullable_setters.body_nullable(), None);
    assert_eq!(expected["nullableSetterTopicIsNull"], true);
    assert_eq!(expected["nullableSetterBodyIsNull"], true);

    let mut nullable_properties = Message::default();
    nullable_properties.set_tags_nullable(None);
    nullable_properties.set_buyer_id_nullable(None);
    nullable_properties.set_instance_id_nullable(None);
    for (property, getter_is_null, remains_present) in [
        ("TAGS", "nullTagsGetterIsNull", "nullTagsRemainsPresent"),
        (
            "BUYER_ID",
            "nullBuyerGetterIsNull",
            "nullBuyerRemainsPresent",
        ),
        (
            "INSTANCE_ID",
            "nullInstanceGetterIsNull",
            "nullInstanceRemainsPresent",
        ),
    ] {
        assert_eq!(nullable_properties.get_property(property), None);
        assert_eq!(
            nullable_properties.has_property(property),
            expected[remains_present]
        );
        assert_eq!(expected[getter_is_null], true);
    }
    assert_eq!(
        serde_json::to_value(nullable_properties.property_entries().collect::<Vec<_>>()).unwrap(),
        expected["nullablePropertyEntries"]
    );
    let nullable_map = serde_json::to_value(&nullable_properties).unwrap();
    assert!(nullable_map["topic"].is_null());
    assert!(nullable_map["body"].is_null());
    assert!(nullable_map["properties"]["TAGS"].is_null());
    let nullable_round_trip: Message = serde_json::from_value(nullable_map).unwrap();
    assert!(nullable_round_trip.topic_nullable().is_none());
    assert!(nullable_round_trip.body_nullable().is_none());
    assert!(nullable_round_trip.has_property("TAGS"));
    assert_eq!(nullable_round_trip.get_property("TAGS"), None);

    let mut nullable_tag_string = Message::default();
    nullable_tag_string.set_tags_nullable(None);
    assert_eq!(
        nullable_tag_string.to_string(),
        expected["nullablePropertyString"]
    );
    assert_eq!(
        nullable_properties.put_user_property_nullable(None, Some("value".to_owned())),
        Err(rocketmq_classic_common::message::MessagePropertyError::EmptyKey)
    );
    assert_eq!(
        nullable_properties.put_user_property_nullable(Some("key".to_owned()), None),
        Err(rocketmq_classic_common::message::MessagePropertyError::EmptyValue)
    );
    assert_eq!(expected["nullPropertyNameRejected"], true);
    assert_eq!(expected["nullPropertyValueRejected"], true);
    let mut whitespace_property = Message::default();
    assert_eq!(
        whitespace_property.put_user_property("\u{00a0}", "value"),
        Ok(None)
    );
    assert_eq!(
        whitespace_property.put_user_property("key", "\u{2003}"),
        Ok(None)
    );
    assert_eq!(
        whitespace_property
            .put_user_property_nullable(Some("custom".to_owned()), Some("\u{00a0}".to_owned())),
        Ok(None)
    );
    assert_eq!(
        whitespace_property.put_user_property("\u{001f} ", "value"),
        Err(rocketmq_classic_common::message::MessagePropertyError::EmptyKey)
    );
    assert_eq!(expected["nonBreakingSpaceUserPropertyNameAccepted"], true);
    assert_eq!(expected["emSpaceUserPropertyValueAccepted"], true);
    assert_eq!(expected["asciiControlOnlyUserPropertyRejected"], true);
    let null_name_lookup = Message::default();
    assert_eq!(null_name_lookup.get_nullable_property(None), None);
    assert_eq!(
        null_name_lookup
            .get_properties()
            .map(|properties| properties.is_empty()),
        Some(true)
    );
    assert_eq!(expected["nullNameLookupIsNull"], true);
    assert_eq!(expected["nullNameLookupInitializesProperties"], true);
    let null_name_contains = Message::default();
    assert!(!null_name_contains.has_nullable_property(None));
    assert_eq!(null_name_contains.get_properties(), None);
    assert_eq!(expected["nullNameContainsIsFalse"], true);
    assert_eq!(expected["nullNameContainsKeepsPropertiesNull"], true);
    let null_user_name = Message::default();
    assert_eq!(null_user_name.get_nullable_user_property(None), None);
    assert_eq!(expected["nullUserNameLookupIsNull"], true);
    let mut collection_with_null = Message::default();
    collection_with_null
        .set_keys_from_nullable(Some(vec![Some("left"), None, Some("right")]))
        .unwrap();
    assert_eq!(
        collection_with_null.keys(),
        expected["collectionKeysWithNullMember"].as_str()
    );
    assert_eq!(expected["collectionKeysWithNullMember"], "left null right");
    let mut null_collection = Message::default();
    assert_eq!(
        null_collection.set_keys_from_nullable(None::<Vec<Option<&str>>>),
        Err(rocketmq_classic_common::message::MessageKeyCollectionError::NullCollection)
    );
    assert_eq!(expected["nullCollectionKeysThrows"], true);

    let mut message = Message::with_fields(
        "orders",
        Some("green"),
        Some("first second"),
        17,
        [0, 255, 42],
        false,
    );
    assert_eq!(message.topic(), expected["topic"]);
    assert_eq!(message.tags(), expected["tags"].as_str());
    assert_eq!(message.keys(), expected["keys"].as_str());
    assert_eq!(message.flag(), expected["flag"]);
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(message.body()),
        expected["body"]
    );
    assert_eq!(message.is_wait_store_msg_ok(), expected["waitStore"]);
    assert_eq!(
        message.get_property("TAGS"),
        expected["propertyTags"].as_str()
    );
    assert_eq!(
        message.get_user_property("TAGS"),
        expected["userPropertyTags"].as_str()
    );
    assert_eq!(message.get_property("missing"), None);
    assert_eq!(message.has_property("TAGS"), expected["hasTags"]);
    assert_eq!(message.has_property("missing"), expected["hasMissing"]);

    message.set_keys_from(["third", "fourth"]);
    assert_eq!(message.keys(), expected["collectionKeys"].as_str());
    message.set_buyer_id("buyer-1");
    message.set_instance_id("instance-1");
    assert_eq!(message.buyer_id(), expected["buyerId"].as_str());
    assert_eq!(
        message.get_property("INSTANCE_ID"),
        expected["instanceIdProperty"].as_str()
    );

    message.set_delay_time_level(-7);
    message.set_delay_time_sec(i64::MIN);
    message.set_delay_time_ms(9_007_199_254_740_993);
    message.set_deliver_time_ms(i64::MAX);
    assert_eq!(
        message.delay_time_level().unwrap(),
        expected["delayTimeLevel"]
    );
    assert_eq!(message.delay_time_sec().unwrap(), expected["delayTimeSec"]);
    assert_eq!(message.delay_time_ms().unwrap(), expected["delayTimeMs"]);
    assert_eq!(
        message.deliver_time_ms().unwrap(),
        expected["deliverTimeMs"]
    );

    let absent = Message::default();
    assert_eq!(
        absent.delay_time_level().unwrap(),
        expected["absentDelayTimeLevel"]
    );
    assert_eq!(
        absent.delay_time_sec().unwrap(),
        expected["absentDelayTimeSec"]
    );
    assert_eq!(
        absent.delay_time_ms().unwrap(),
        expected["absentDelayTimeMs"]
    );
    assert_eq!(
        absent.deliver_time_ms().unwrap(),
        expected["absentDeliverTimeMs"]
    );

    let mut unicode = Message::default();
    unicode.set_system_property("DELAY", "١٢").unwrap();
    unicode
        .set_system_property("TIMER_DELAY_SEC", "+٣٤")
        .unwrap();
    assert_eq!(
        unicode.delay_time_level().unwrap(),
        expected["unicodeDelayTimeLevel"]
    );
    assert_eq!(
        unicode.delay_time_sec().unwrap(),
        expected["unicodeDelayTimeSec"]
    );

    unicode.set_system_property("DELAY", "2147483648").unwrap();
    assert_eq!(
        unicode.delay_time_level().is_err(),
        expected["overflowDelayTimeLevelThrows"]
    );
    unicode
        .set_system_property("TIMER_DELAY_MS", "9223372036854775808")
        .unwrap();
    assert_eq!(
        unicode.delay_time_ms().is_err(),
        expected["overflowDelayTimeMsThrows"]
    );

    for case in expected["numericPropertyInputs"].as_array().unwrap() {
        let property = case["property"].as_str().unwrap();
        let input = case["input"].as_str().unwrap();
        let mut candidate = Message::default();
        candidate.set_system_property(property, input).unwrap();
        let parsed = match property {
            "DELAY" => candidate.delay_time_level().map(i64::from),
            "TIMER_DELAY_SEC" => candidate.delay_time_sec(),
            "TIMER_DELAY_MS" => candidate.delay_time_ms(),
            "TIMER_DELIVER_MS" => candidate.deliver_time_ms(),
            _ => panic!("unexpected Java property {property}"),
        };
        assert_eq!(parsed.is_ok(), case["accepted"], "{property}={input:?}");
        if let Some(value) = case["value"].as_i64() {
            assert_eq!(parsed.unwrap(), value, "{property}={input:?}");
        }
    }

    let mut diagnostic = Message::new("invoice", [0, 255, 42]);
    assert_eq!(diagnostic.to_string(), expected["messageToString"]);
    diagnostic
        .get_properties_mut()
        .unwrap()
        .insert("DIRECT".to_owned(), "through-map".to_owned());
    assert_eq!(
        diagnostic.get_property("DIRECT"),
        expected["directMapMutation"].as_str()
    );

    let mut extended = MessageExt::new(message);
    extended.set_topic("changed-through-inherited-api");
    assert_eq!(extended.topic(), "changed-through-inherited-api");
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
