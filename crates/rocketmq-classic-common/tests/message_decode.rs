use base64::Engine;
use rocketmq_classic_common::{
    message::{
        BLANK_MAGIC_CODE, CHARSET_UTF8, CompactMessageError, DecodeError, DecodeOptions,
        MESSAGE_FLAG_POSITION, MESSAGE_MAGIC_CODE, MESSAGE_MAGIC_CODE_POSITION,
        MESSAGE_MAGIC_CODE_V1, MESSAGE_MAGIC_CODE_V2, MESSAGE_PHYSIC_OFFSET_POSITION,
        MESSAGE_STORE_TIMESTAMP_POSITION, Message, MessageClientExt, MessageExt,
        MessageHostAddress, MessageHostAddressBytesError, MessageId, MessageIdError,
        MessageProperties, MessageVersion, MessageVersionError, PHY_POS_POSITION,
        PROPERTY_SEPARATOR, QUEUE_OFFSET_POSITION, SYSFLAG_POSITION, TopicFilterType,
        append_crc32_property, client_decode, client_decodes, client_decodes_with_body,
        count_inner_messages, create_message_id, create_message_id_from_host_bytes,
        decode_compact_message, decode_compact_messages, decode_inner_batch, decode_many,
        decode_many_as_client, decode_message_id, decode_one, decode_properties_nullable,
        decode_record_properties, encode_compact_message, encode_compact_messages,
        encode_message_ext, encode_message_ext_uniquely, encode_nullable_property_entries,
        encode_properties_nullable,
    },
    sysflag::message,
};
use serde_json::Value;
use std::collections::BTreeMap;
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
        assert_eq!(
            serde_json::to_value(decode_record_properties(&bytes).unwrap()).unwrap(),
            expected["recordProperties"],
            "{}",
            case["name"]
        );
        assert_eq!(decoded.topic(), expected["topic"]);
        assert_eq!(
            decoded.body().to_vec(),
            base64::engine::general_purpose::STANDARD
                .decode(expected["bodyBase64"].as_str().unwrap())
                .unwrap()
        );
        assert_eq!(decoded.flag(), expected["flag"]);
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
            serde_json::to_value(decoded.properties()).unwrap(),
            expected["properties"]
        );
        assert_eq!(decoded.store_size() as usize, bytes.len());
    }
}

#[test]
fn message_ext_scalar_public_api_matches_java_551_fixture() {
    let expected = &fixture()["messageExtPublicApi"];
    let defaults = MessageExt::default();
    assert_eq!(
        defaults.queue_id(),
        expected["defaultQueueId"].as_i64().unwrap() as i32
    );
    assert_eq!(
        defaults.born_timestamp(),
        expected["defaultBornTimestamp"].as_i64().unwrap()
    );
    assert_eq!(
        defaults.store_timestamp(),
        expected["defaultStoreTimestamp"].as_i64().unwrap()
    );
    assert_eq!(defaults.message_id(), expected["defaultMessageId"].as_str());
    assert_eq!(
        defaults.sys_flag(),
        expected["defaultSysFlag"].as_i64().unwrap() as i32
    );
    assert_eq!(
        defaults.body_crc(),
        expected["defaultBodyCrc"].as_i64().unwrap() as i32
    );
    assert_eq!(
        defaults.queue_offset(),
        expected["defaultQueueOffset"].as_i64().unwrap()
    );
    assert_eq!(
        defaults.commit_log_offset(),
        expected["defaultCommitLogOffset"].as_i64().unwrap()
    );
    assert_eq!(
        defaults.store_size(),
        expected["defaultStoreSize"].as_i64().unwrap() as i32
    );
    assert_eq!(
        defaults.reconsume_times(),
        expected["defaultReconsumeTimes"].as_i64().unwrap() as i32
    );
    assert_eq!(
        defaults.prepared_transaction_offset(),
        expected["defaultPreparedTransactionOffset"]
            .as_i64()
            .unwrap()
    );

    let mut message = MessageExt::default();
    message.set_queue_id(-17);
    message.set_born_timestamp(i64::MIN + 17);
    message.set_store_timestamp(i64::MAX - 17);
    message.set_message_id(Some("message-ext-id".to_owned()));
    message.set_sys_flag(-33);
    message.set_body_crc(i32::MIN);
    message.set_queue_offset(i64::MIN);
    message.set_commit_log_offset(i64::MAX);
    message.set_store_size(-21);
    message.set_reconsume_times(-4);
    message.set_prepared_transaction_offset(i64::MAX - 1);
    assert_eq!(
        message.queue_id(),
        expected["queueId"].as_i64().unwrap() as i32
    );
    assert_eq!(
        message.born_timestamp(),
        expected["bornTimestamp"].as_i64().unwrap()
    );
    assert_eq!(
        message.store_timestamp(),
        expected["storeTimestamp"].as_i64().unwrap()
    );
    assert_eq!(message.message_id(), expected["messageId"].as_str());
    assert_eq!(
        message.sys_flag(),
        expected["sysFlag"].as_i64().unwrap() as i32
    );
    assert_eq!(
        message.body_crc(),
        expected["bodyCrc"].as_i64().unwrap() as i32
    );
    assert_eq!(
        message.queue_offset(),
        expected["queueOffset"].as_i64().unwrap()
    );
    assert_eq!(
        message.commit_log_offset(),
        expected["commitLogOffset"].as_i64().unwrap()
    );
    assert_eq!(
        message.store_size(),
        expected["storeSize"].as_i64().unwrap() as i32
    );
    assert_eq!(
        message.reconsume_times(),
        expected["reconsumeTimes"].as_i64().unwrap() as i32
    );
    assert_eq!(
        message.prepared_transaction_offset(),
        expected["preparedTransactionOffset"].as_i64().unwrap()
    );
}

#[test]
fn message_client_ext_api_matches_java_551_fixture() {
    let expected = &fixture()["messageClientExtApi"];
    let mut message = MessageClientExt::default();
    assert_eq!(
        message.message_id().as_deref(),
        expected["defaultMessageId"].as_str()
    );
    assert_eq!(
        message.offset_message_id(),
        expected["defaultOffsetMessageId"].as_str()
    );
    message.set_message_id(Some("ignored-initial".to_owned()));
    assert_eq!(
        message.message_id().as_deref(),
        expected["setMessageIdOnDefault"].as_str()
    );
    assert_eq!(
        message.offset_message_id(),
        expected["setMessageIdDoesNotSetOffset"].as_str()
    );
    message.set_offset_message_id(Some("offset-1".to_owned()));
    assert_eq!(
        message.offset_message_id(),
        expected["offsetMessageId"].as_str()
    );
    assert_eq!(
        message.message_id().as_deref(),
        expected["messageIdFallsBackToOffset"].as_str()
    );
    message
        .set_system_property(
            rocketmq_classic_common::message::constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            "unique-1",
        )
        .unwrap();
    assert_eq!(
        message.message_id().as_deref(),
        expected["messageIdPrefersUnique"].as_str()
    );
    assert_eq!(
        message.offset_message_id(),
        expected["offsetRemainsWhenUniqueExists"].as_str()
    );
    message.set_message_id(Some("ignored-after-unique".to_owned()));
    assert_eq!(
        message.message_id().as_deref(),
        expected["setMessageIdDoesNotReplaceUnique"].as_str()
    );
    message
        .set_system_property(
            rocketmq_classic_common::message::constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            "unique-2",
        )
        .unwrap();
    assert_eq!(
        message.message_id().as_deref(),
        expected["messageIdReadsLiveUniqueProperty"].as_str()
    );
    message.set_offset_message_id(Some("offset-2".to_owned()));
    assert_eq!(
        message.offset_message_id(),
        expected["offsetMessageIdReplaced"].as_str()
    );
    assert_eq!(
        message.message_id().as_deref(),
        expected["uniqueSurvivesOffsetReplacement"].as_str()
    );
    message.clear_system_property(
        rocketmq_classic_common::message::constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
    );
    assert_eq!(
        message.message_id().as_deref(),
        expected["messageIdFallsBackAfterUniqueClear"].as_str()
    );
    message.set_message_id(Some("ignored-after-clear".to_owned()));
    assert_eq!(
        message.message_id().as_deref(),
        expected["setMessageIdDoesNotReplaceOffsetFallback"].as_str()
    );
}

#[test]
fn client_message_decoder_apis_match_java_551() {
    let fixture = fixture();
    let cases = fixture["cases"].as_array().unwrap();
    let mut joined = Vec::new();

    for (index, case) in cases.iter().enumerate() {
        let bytes = record(case);
        let (decoded, consumed) =
            client_decode(&bytes, true).unwrap_or_else(|error| panic!("{}: {error}", case["name"]));
        assert_eq!(consumed, bytes.len(), "{}", case["name"]);
        assert_eq!(
            decoded.message_id(),
            case["clientDecoded"]["msgId"].as_str()
        );
        assert_eq!(
            decoded.offset_message_id(),
            case["clientDecoded"]["offsetMsgId"].as_str()
        );

        if index < 2 {
            joined.extend_from_slice(&bytes);
        }
    }

    let batch_options = DecodeOptions {
        read_body: false,
        decompress_body: false,
        set_properties_string: true,
        verify_crc: true,
    };
    let batch = decode_many_as_client(&joined, batch_options).unwrap();
    let expected = &cases[..2];
    assert_eq!(batch.len(), expected.len());
    for (message, case) in batch.iter().zip(expected) {
        assert!(message.body().is_empty());
        assert_eq!(
            message.message_id(),
            case["clientDecoded"]["msgId"].as_str()
        );
        assert_eq!(
            message.offset_message_id(),
            case["clientDecoded"]["offsetMsgId"].as_str()
        );
        assert_eq!(
            message.properties().contains_key("propertiesString"),
            !case["decoded"]["properties"]
                .as_object()
                .unwrap()
                .is_empty()
        );
    }

    let default_batch = client_decodes(&joined).unwrap();
    assert_eq!(default_batch.len(), expected.len());
    let body_skipped_batch = client_decodes_with_body(&joined, false).unwrap();
    assert_eq!(body_skipped_batch.len(), expected.len());
    assert!(
        body_skipped_batch
            .iter()
            .all(|message| message.body().is_empty())
    );
}

#[test]
fn message_ext_socket_address_api_matches_java_551() {
    let fixture = fixture();
    let expected = &fixture["messageExtHostApi"];
    let born = MessageHostAddress::from_ip_address(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)), 10911);
    let store = MessageHostAddress::unresolved("broker.invalid", 10912);
    let mut message = MessageExt::default();
    assert_eq!(
        message.broker_name(),
        expected["brokerNameDefault"].as_str()
    );
    message.set_broker_name(Some("broker-a".to_owned()));
    assert_eq!(message.broker_name(), expected["brokerNameSet"].as_str());
    message.set_broker_name(None);
    assert_eq!(
        message.broker_name(),
        expected["brokerNameCleared"].as_str()
    );
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
fn message_id_api_matches_java_551() {
    let expected = &fixture()["messageExtHostApi"];
    let ipv4 = MessageHostAddress::from_ip_address(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)), 10911);
    let ipv6 = MessageHostAddress::from_ip_address(
        IpAddr::V6("2001:db8::20".parse::<Ipv6Addr>().unwrap()),
        10915,
    );
    let ipv4_offset = -123_456_789_012_345_i64;
    let ipv6_offset = i64::MIN + 17;

    let ipv4_id = create_message_id(&ipv4, ipv4_offset).unwrap();
    let ipv6_id = create_message_id(&ipv6, ipv6_offset).unwrap();
    assert_eq!(ipv4_id, expected["messageIdIpv4Created"]);
    assert_eq!(ipv6_id, expected["messageIdIpv6Created"]);
    assert_eq!(
        create_message_id_from_host_bytes(&ipv4.network_bytes().unwrap(), ipv4_offset).unwrap(),
        expected["messageIdIpv4CreatedFromBytes"]
    );
    assert_eq!(
        create_message_id_from_host_bytes(&ipv6.network_bytes().unwrap(), ipv6_offset).unwrap(),
        expected["messageIdIpv6CreatedFromBytes"]
    );

    let decoded_ipv4 = decode_message_id(&ipv4_id).unwrap();
    assert_eq!(
        decoded_ipv4
            .address()
            .unwrap()
            .host_address_string()
            .as_deref(),
        expected["messageIdIpv4DecodedHost"].as_str()
    );
    assert_eq!(
        decoded_ipv4.address().unwrap().port(),
        expected["messageIdIpv4DecodedPort"]
    );
    assert_eq!(decoded_ipv4.offset(), ipv4_offset);
    assert_eq!(
        decoded_ipv4.offset(),
        expected["messageIdIpv4DecodedOffset"]
    );
    let decoded_ipv6 = decode_message_id(&ipv6_id).unwrap();
    assert_eq!(
        decoded_ipv6
            .address()
            .unwrap()
            .host_address_string()
            .as_deref(),
        expected["messageIdIpv6DecodedHost"].as_str()
    );
    assert_eq!(
        decoded_ipv6.address().unwrap().port(),
        expected["messageIdIpv6DecodedPort"]
    );
    assert_eq!(decoded_ipv6.offset(), ipv6_offset);
    assert_eq!(
        decoded_ipv6.offset(),
        expected["messageIdIpv6DecodedOffset"]
    );
    assert_eq!(
        decode_message_id(&format!("{ipv6_id}FFFF"))
            .unwrap()
            .offset(),
        expected["messageIdIpv6ExtraBytesIgnored"]
    );

    let mut mutable_id = MessageId::new(Some(ipv4), 42);
    mutable_id.set_address(Some(ipv6.clone()));
    mutable_id.set_offset(-17);
    assert_eq!(
        mutable_id
            .address()
            .unwrap()
            .host_address_string()
            .as_deref(),
        expected["messageIdSetterHost"].as_str()
    );
    assert_eq!(
        mutable_id.address().unwrap().port(),
        expected["messageIdSetterPort"]
    );
    assert_eq!(mutable_id.offset(), expected["messageIdSetterOffset"]);
    mutable_id.set_address(None);
    assert_eq!(mutable_id.address(), None);
    assert_eq!(expected["messageIdNullableAddress"], true);
    assert_eq!(expected["messageIdNullableSetter"], true);

    assert_eq!(
        create_message_id(&MessageHostAddress::unresolved("broker.invalid", 10911), 0),
        Err(MessageIdError::UnresolvedAddress)
    );
    assert_eq!(
        create_message_id_from_host_bytes(&[0; 4], 0),
        Err(MessageIdError::InvalidAddressLength { length: 4 })
    );
    assert_eq!(
        decode_message_id("").unwrap_err(),
        MessageIdError::EmptyMessageId
    );
    assert_eq!(expected["messageIdEmptyThrowsNullPointer"], true);
    assert_eq!(
        decode_message_id("0").unwrap_err(),
        MessageIdError::InsufficientBytes {
            required: 28,
            available: 0
        }
    );
    assert_eq!(expected["messageIdShortThrowsBufferUnderflow"], true);
    assert_eq!(
        decode_message_id("7F000001FFFFFFFF0000000000003039").unwrap_err(),
        MessageIdError::InvalidPort { port: -1 }
    );
    assert_eq!(expected["messageIdBadPortThrowsIllegalArgument"], true);
    let invalid_hex = decode_message_id("GG00000100002A9F0000000000003039").unwrap();
    assert_eq!(
        invalid_hex
            .address()
            .unwrap()
            .host_address_string()
            .as_deref(),
        expected["messageIdInvalidHexHost"].as_str()
    );
}

#[test]
fn crc32_property_helpers_match_both_java_buffers() {
    let expected = &fixture()["messageDecoderCrcApi"]["cases"];
    for case in expected.as_array().unwrap() {
        let mut bytes = Vec::new();
        append_crc32_property(&mut bytes, case["crc32"].as_i64().unwrap() as i32);
        let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
        assert_eq!(encoded, case["byteBufferBase64"]);
        assert_eq!(case["byteBufferBase64"], case["byteBufBase64"]);
    }
}

#[test]
fn message_decoder_public_constants_match_java_551() {
    let expected = &fixture()["messageDecoderPublicApi"];
    assert_eq!(CHARSET_UTF8, expected["charsetUtf8"]);
    assert_eq!(MESSAGE_MAGIC_CODE, expected["messageMagicCode"]);
    assert_eq!(MESSAGE_MAGIC_CODE_V1, expected["messageMagicCode"]);
    assert_eq!(MESSAGE_MAGIC_CODE_V2, expected["messageMagicCodeV2"]);
    assert_eq!(BLANK_MAGIC_CODE, expected["blankMagicCode"]);
    assert_eq!(
        MESSAGE_MAGIC_CODE_POSITION,
        expected["messageMagicCodePosition"].as_u64().unwrap() as usize
    );
    assert_eq!(
        MESSAGE_FLAG_POSITION,
        expected["messageFlagPosition"].as_u64().unwrap() as usize
    );
    assert_eq!(
        MESSAGE_PHYSIC_OFFSET_POSITION,
        expected["messagePhysicOffsetPosition"].as_u64().unwrap() as usize
    );
    assert_eq!(
        MESSAGE_STORE_TIMESTAMP_POSITION,
        expected["messageStoreTimestampPosition"].as_u64().unwrap() as usize
    );
    assert_eq!(
        PHY_POS_POSITION,
        expected["phyPosPosition"].as_u64().unwrap() as usize
    );
    assert_eq!(
        QUEUE_OFFSET_POSITION,
        expected["queueOffsetPosition"].as_u64().unwrap() as usize
    );
    assert_eq!(
        SYSFLAG_POSITION,
        expected["sysflagPosition"].as_u64().unwrap() as usize
    );
    assert_eq!(
        rocketmq_classic_common::message::NAME_VALUE_SEPARATOR as i32,
        expected["nameValueSeparator"]
    );
    assert_eq!(PROPERTY_SEPARATOR as i32, expected["propertySeparator"]);
}

#[test]
fn message_constants_match_java_551() {
    use rocketmq_classic_common::message::constants;

    let expected = &fixture()["messageConstantsApi"];
    let message_const = [
        ("PROPERTY_KEYS", constants::PROPERTY_KEYS),
        ("PROPERTY_TAGS", constants::PROPERTY_TAGS),
        (
            "PROPERTY_WAIT_STORE_MSG_OK",
            constants::PROPERTY_WAIT_STORE_MSG_OK,
        ),
        (
            "PROPERTY_DELAY_TIME_LEVEL",
            constants::PROPERTY_DELAY_TIME_LEVEL,
        ),
        ("PROPERTY_RETRY_TOPIC", constants::PROPERTY_RETRY_TOPIC),
        ("PROPERTY_ORIGIN_GROUP", constants::PROPERTY_ORIGIN_GROUP),
        ("PROPERTY_REAL_TOPIC", constants::PROPERTY_REAL_TOPIC),
        ("PROPERTY_REAL_QUEUE_ID", constants::PROPERTY_REAL_QUEUE_ID),
        (
            "PROPERTY_TRANSACTION_PREPARED",
            constants::PROPERTY_TRANSACTION_PREPARED,
        ),
        (
            "PROPERTY_PRODUCER_GROUP",
            constants::PROPERTY_PRODUCER_GROUP,
        ),
        ("PROPERTY_MIN_OFFSET", constants::PROPERTY_MIN_OFFSET),
        ("PROPERTY_MAX_OFFSET", constants::PROPERTY_MAX_OFFSET),
        ("PROPERTY_BUYER_ID", constants::PROPERTY_BUYER_ID),
        (
            "PROPERTY_ORIGIN_MESSAGE_ID",
            constants::PROPERTY_ORIGIN_MESSAGE_ID,
        ),
        ("PROPERTY_TRANSFER_FLAG", constants::PROPERTY_TRANSFER_FLAG),
        (
            "PROPERTY_CORRECTION_FLAG",
            constants::PROPERTY_CORRECTION_FLAG,
        ),
        ("PROPERTY_MQ2_FLAG", constants::PROPERTY_MQ2_FLAG),
        (
            "PROPERTY_RECONSUME_TIME",
            constants::PROPERTY_RECONSUME_TIME,
        ),
        ("PROPERTY_MSG_REGION", constants::PROPERTY_MSG_REGION),
        ("PROPERTY_TRACE_SWITCH", constants::PROPERTY_TRACE_SWITCH),
        (
            "PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX",
            constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
        ),
        ("PROPERTY_TRANS_OFFSET", constants::PROPERTY_TRANS_OFFSET),
        (
            "PROPERTY_EXTEND_UNIQ_INFO",
            constants::PROPERTY_EXTEND_UNIQ_INFO,
        ),
        (
            "PROPERTY_MAX_RECONSUME_TIMES",
            constants::PROPERTY_MAX_RECONSUME_TIMES,
        ),
        (
            "PROPERTY_CONSUME_START_TIMESTAMP",
            constants::PROPERTY_CONSUME_START_TIMESTAMP,
        ),
        ("PROPERTY_PRIORITY", constants::PROPERTY_PRIORITY),
        ("PROPERTY_INNER_NUM", constants::PROPERTY_INNER_NUM),
        ("PROPERTY_INNER_BASE", constants::PROPERTY_INNER_BASE),
        ("DUP_INFO", constants::DUP_INFO),
        (
            "PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS",
            constants::PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS,
        ),
        (
            "PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET",
            constants::PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET,
        ),
        (
            "PROPERTY_TRANSACTION_ID",
            constants::PROPERTY_TRANSACTION_ID,
        ),
        (
            "PROPERTY_TRANSACTION_CHECK_TIMES",
            constants::PROPERTY_TRANSACTION_CHECK_TIMES,
        ),
        ("PROPERTY_INSTANCE_ID", constants::PROPERTY_INSTANCE_ID),
        (
            "PROPERTY_CORRELATION_ID",
            constants::PROPERTY_CORRELATION_ID,
        ),
        (
            "PROPERTY_MESSAGE_REPLY_TO_CLIENT",
            constants::PROPERTY_MESSAGE_REPLY_TO_CLIENT,
        ),
        ("PROPERTY_MESSAGE_TTL", constants::PROPERTY_MESSAGE_TTL),
        (
            "PROPERTY_REPLY_MESSAGE_ARRIVE_TIME",
            constants::PROPERTY_REPLY_MESSAGE_ARRIVE_TIME,
        ),
        (
            "PROPERTY_PUSH_REPLY_TIME",
            constants::PROPERTY_PUSH_REPLY_TIME,
        ),
        ("PROPERTY_CLUSTER", constants::PROPERTY_CLUSTER),
        ("PROPERTY_MESSAGE_TYPE", constants::PROPERTY_MESSAGE_TYPE),
        ("PROPERTY_POP_CK", constants::PROPERTY_POP_CK),
        ("PROPERTY_POP_CK_OFFSET", constants::PROPERTY_POP_CK_OFFSET),
        (
            "PROPERTY_FIRST_POP_TIME",
            constants::PROPERTY_FIRST_POP_TIME,
        ),
        ("PROPERTY_SHARDING_KEY", constants::PROPERTY_SHARDING_KEY),
        ("PROPERTY_LITE_TOPIC", constants::PROPERTY_LITE_TOPIC),
        (
            "PROPERTY_FORWARD_QUEUE_ID",
            constants::PROPERTY_FORWARD_QUEUE_ID,
        ),
        ("PROPERTY_REDIRECT", constants::PROPERTY_REDIRECT),
        (
            "PROPERTY_INNER_MULTI_DISPATCH",
            constants::PROPERTY_INNER_MULTI_DISPATCH,
        ),
        (
            "PROPERTY_INNER_MULTI_QUEUE_OFFSET",
            constants::PROPERTY_INNER_MULTI_QUEUE_OFFSET,
        ),
        ("PROPERTY_TRACE_CONTEXT", constants::PROPERTY_TRACE_CONTEXT),
        (
            "PROPERTY_TIMER_DELAY_SEC",
            constants::PROPERTY_TIMER_DELAY_SEC,
        ),
        (
            "PROPERTY_TIMER_DELIVER_MS",
            constants::PROPERTY_TIMER_DELIVER_MS,
        ),
        ("PROPERTY_BORN_HOST", constants::PROPERTY_BORN_HOST),
        (
            "PROPERTY_BORN_TIMESTAMP",
            constants::PROPERTY_BORN_TIMESTAMP,
        ),
        (
            "PROPERTY_TRANSIENT_PREFIX",
            constants::PROPERTY_TRANSIENT_PREFIX,
        ),
        (
            "PROPERTY_TRANSIENT_TOPIC_CONFIG",
            constants::PROPERTY_TRANSIENT_TOPIC_CONFIG,
        ),
        (
            "PROPERTY_TRANSIENT_GROUP_CONFIG",
            constants::PROPERTY_TRANSIENT_GROUP_CONFIG,
        ),
        ("KEY_SEPARATOR", constants::KEY_SEPARATOR),
        ("INDEX_KEY_TYPE", constants::INDEX_KEY_TYPE),
        ("INDEX_UNIQUE_TYPE", constants::INDEX_UNIQUE_TYPE),
        ("INDEX_TAG_TYPE", constants::INDEX_TAG_TYPE),
        ("TIMER_ENGINE_TYPE", constants::TIMER_ENGINE_TYPE),
        (
            "TIMER_ENGINE_ROCKSDB_TIMELINE",
            constants::TIMER_ENGINE_ROCKSDB_TIMELINE,
        ),
        (
            "TIMER_ENGINE_FILE_TIME_WHEEL",
            constants::TIMER_ENGINE_FILE_TIME_WHEEL,
        ),
        (
            "PROPERTY_TIMER_ENQUEUE_MS",
            constants::PROPERTY_TIMER_ENQUEUE_MS,
        ),
        (
            "PROPERTY_TIMER_DEQUEUE_MS",
            constants::PROPERTY_TIMER_DEQUEUE_MS,
        ),
        (
            "PROPERTY_TIMER_ROLL_TIMES",
            constants::PROPERTY_TIMER_ROLL_TIMES,
        ),
        ("PROPERTY_TIMER_OUT_MS", constants::PROPERTY_TIMER_OUT_MS),
        (
            "PROPERTY_TIMER_DEL_UNIQKEY",
            constants::PROPERTY_TIMER_DEL_UNIQKEY,
        ),
        (
            "PROPERTY_TIMER_ROLL_LABEL",
            constants::PROPERTY_TIMER_ROLL_LABEL,
        ),
        (
            "PROPERTY_TIMER_DELAY_LEVEL",
            constants::PROPERTY_TIMER_DELAY_LEVEL,
        ),
        (
            "PROPERTY_TIMER_DELAY_MS",
            constants::PROPERTY_TIMER_DELAY_MS,
        ),
        ("PROPERTY_CRC32", constants::PROPERTY_CRC32),
        (
            "PROPERTY_DLQ_ORIGIN_TOPIC",
            constants::PROPERTY_DLQ_ORIGIN_TOPIC,
        ),
        (
            "PROPERTY_DLQ_ORIGIN_MESSAGE_ID",
            constants::PROPERTY_DLQ_ORIGIN_MESSAGE_ID,
        ),
    ]
    .into_iter()
    .collect::<BTreeMap<_, _>>();
    assert_eq!(message_const.len(), 76);
    assert_eq!(
        serde_json::to_value(message_const).unwrap(),
        expected["messageConst"]
    );

    let mix_all = BTreeMap::from([
        ("REPLY_TOPIC_POSTFIX", constants::REPLY_TOPIC_POSTFIX),
        ("REPLY_MESSAGE_FLAG", constants::REPLY_MESSAGE_FLAG),
    ]);
    assert_eq!(serde_json::to_value(mix_all).unwrap(), expected["mixAll"]);

    let mut reserved_names = {
        constants::STRING_HASH_SET
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter_map(|property| property.clone())
            .collect::<Vec<_>>()
    };
    reserved_names.sort();
    assert_eq!(
        serde_json::to_value(reserved_names).unwrap(),
        expected["reservedNames"]
    );
}

#[test]
fn message_version_api_matches_java_551_fixture() {
    let expected = &fixture()["messageVersionApi"];
    let versions = [MessageVersion::V1, MessageVersion::V2];
    assert_eq!(expected["values"].as_array().unwrap().len(), versions.len());

    for (index, version) in versions.into_iter().enumerate() {
        let item = &expected["values"][index];
        assert_eq!(version.name(), item["name"]);
        assert_eq!(version.to_string(), item["toString"]);
        assert_eq!(version.ordinal(), item["ordinal"].as_i64().unwrap() as i32);
        assert_eq!(version.magic_code(), item["magicCode"]);
        assert_eq!(version.topic_length_size(), item["topicLengthSize"]);
        assert_eq!(
            MessageVersion::value_of_name(Some(item["valueOfName"].as_str().unwrap())).unwrap(),
            version
        );
        assert_eq!(
            MessageVersion::value_of_magic_code(version.magic_code()).unwrap(),
            version
        );
        assert_eq!(item["valueOfMagicCode"], version.name());
        let other = if index == 0 {
            MessageVersion::V2
        } else {
            MessageVersion::V1
        };
        assert_eq!(
            version.java_compare_to(other),
            version.ordinal() - other.ordinal()
        );
        if index == 0 {
            assert!(item["compareToPrevious"].is_null());
        } else {
            assert_eq!(
                version.java_compare_to(versions[index - 1]),
                item["compareToPrevious"]
            );
        }

        let read_bytes: &[u8] = match version {
            MessageVersion::V1 => &[0x55, 0xff, 0x7f],
            MessageVersion::V2 => &[0x55, 0xff, 0xfe, 0x7f, 0xff],
        };
        let mut relative = &read_bytes[1..];
        assert_eq!(
            version.get_topic_length(&mut relative).unwrap(),
            item["relativeTopicLength"]
        );
        assert_eq!(
            relative.len(),
            read_bytes.len() - 1 - version.topic_length_size()
        );
        assert_eq!(
            item["relativePositionAfterRead"].as_u64().unwrap() as usize,
            1 + version.topic_length_size()
        );
        let indexed = version
            .get_topic_length_at(
                read_bytes,
                if version == MessageVersion::V1 { 2 } else { 3 },
            )
            .unwrap();
        assert_eq!(indexed, item["indexedTopicLength"]);

        let mut appended = vec![0];
        version.put_topic_length(&mut appended, 0x123456);
        assert_eq!(
            base64::engine::general_purpose::STANDARD.encode(&appended),
            item["putBytesBase64"]
        );
        assert_eq!(
            appended.len(),
            item["positionAfterWrite"].as_u64().unwrap() as usize
        );
    }

    for case in expected["narrowingCases"].as_array().unwrap() {
        let version =
            MessageVersion::value_of_name(Some(case["version"].as_str().unwrap())).unwrap();
        let topic_length = case["topicLength"].as_i64().unwrap() as i32;
        let mut bytes = Vec::new();
        version.put_topic_length(&mut bytes, topic_length);
        assert_eq!(
            base64::engine::general_purpose::STANDARD.encode(&bytes),
            case["bytesBase64"]
        );
        let mut input = bytes.as_slice();
        assert_eq!(
            version.get_topic_length(&mut input).unwrap(),
            case["readBack"]
        );
        assert!(input.is_empty());
    }

    assert_eq!(
        MessageVersion::value_of_name(None),
        Err(MessageVersionError::NullEnumName)
    );
    assert_eq!(
        MessageVersion::value_of_name(Some("MESSAGE_VERSION_V3")),
        Err(MessageVersionError::UnknownEnumName(
            "MESSAGE_VERSION_V3".to_owned()
        ))
    );
    assert_eq!(
        expected["invalidMagicCodeException"],
        "IllegalArgumentException"
    );
    assert!(matches!(
        MessageVersion::value_of_magic_code(0),
        Err(MessageVersionError::InvalidMagicCode(0))
    ));
    assert_eq!(
        expected["unknownEnumNameException"],
        "IllegalArgumentException"
    );
    assert_eq!(expected["nullEnumNameException"], "NullPointerException");

    let mut truncated = &[0xff][..];
    assert!(matches!(
        MessageVersion::V2.get_topic_length(&mut truncated),
        Err(MessageVersionError::Truncated {
            offset: 0,
            needed: 2,
            available: 1
        })
    ));
    assert_eq!(truncated, &[0xff]);
    assert_eq!(
        expected["relativeUnderflowException"],
        "BufferUnderflowException"
    );
    assert_eq!(
        expected["indexedOutOfBoundsException"],
        "IndexOutOfBoundsException"
    );
    assert!(matches!(
        MessageVersion::V2.get_topic_length_at(&[0], 0),
        Err(MessageVersionError::Truncated {
            offset: 0,
            needed: 2,
            available: 1
        })
    ));
}

#[test]
fn message_decoder_property_map_methods_match_java_551() {
    let expected = &fixture()["messageDecoderPropertiesApi"];
    assert_eq!(encode_properties_nullable(None), expected["nullMapEncoded"]);
    assert_eq!(
        encode_properties_nullable(Some(&MessageProperties::new())),
        expected["emptyMapEncoded"]
    );
    let null_value: [(Option<&str>, Option<&str>); 1] = [(Some("ignored"), None)];
    assert_eq!(
        encode_nullable_property_entries(Some(&null_value)),
        expected["nullValueEncoded"]
    );
    let null_key: [(Option<&str>, Option<&str>); 1] = [(None, Some("value"))];
    assert_eq!(
        encode_nullable_property_entries(Some(&null_key)),
        expected["nullKeyEncoded"]
    );
    assert_eq!(
        serde_json::to_value(decode_properties_nullable(None)).unwrap(),
        expected["nullInputDecoded"]
    );
    assert_eq!(
        serde_json::to_value(decode_properties_nullable(Some(""))).unwrap(),
        expected["emptyInputDecoded"]
    );
    let encoded = expected["encodedInput"].as_str().unwrap();
    assert_eq!(
        serde_json::to_value(decode_properties_nullable(Some(encoded))).unwrap(),
        expected["encodedInputDecoded"]
    );
}

#[test]
fn compact_message_codec_and_inner_batch_match_java_551() {
    let expected = &fixture()["messageCompactApi"];
    let mut first = Message::default();
    first.set_body([0, 255, 42]);
    first.set_flag(-17);
    first.set_system_property("k", "v").unwrap();
    let mut second = Message::default();
    second.set_body(Vec::new());

    let first_bytes = encode_compact_message(&first).unwrap();
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(&first_bytes),
        expected["singleEncodedBase64"]
    );
    let batch_bytes = encode_compact_messages(&[first, second]).unwrap();
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(&batch_bytes),
        expected["batchEncodedBase64"]
    );
    assert_eq!(
        count_inner_messages(&batch_bytes).unwrap(),
        expected["innerMessageCount"].as_u64().unwrap() as usize
    );
    assert_eq!(
        count_inner_messages(&[]).unwrap(),
        expected["emptyInnerMessageCount"].as_u64().unwrap() as usize
    );

    let (decoded_first, consumed) = decode_compact_message(&first_bytes).unwrap();
    assert_eq!(consumed, first_bytes.len());
    assert_eq!(decoded_first.topic_nullable(), None);
    assert_eq!(decoded_first.flag(), expected["singleDecoded"]["flag"]);
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(decoded_first.body()),
        expected["singleDecoded"]["bodyBase64"]
    );
    assert_eq!(
        serde_json::to_value(decoded_first.properties_nullable().unwrap()).unwrap(),
        expected["singleDecoded"]["properties"]
    );

    let decoded_batch = decode_compact_messages(&batch_bytes).unwrap();
    assert_eq!(decoded_batch.len(), 2);
    for (message, case) in decoded_batch
        .iter()
        .zip(expected["batchDecoded"].as_array().unwrap())
    {
        assert_eq!(message.topic_nullable(), None);
        assert_eq!(message.flag(), case["flag"]);
        assert_eq!(
            base64::engine::general_purpose::STANDARD.encode(message.body()),
            case["bodyBase64"]
        );
        assert_eq!(
            serde_json::to_value(message.properties_nullable().unwrap()).unwrap(),
            case["properties"]
        );
    }

    let mut outer = MessageExt::default();
    outer.set_topic("outer-topic");
    outer.set_body(batch_bytes.clone());
    outer.set_queue_id(7);
    outer.set_queue_offset(100);
    outer.set_sys_flag(0x40);
    outer.set_born_timestamp(11);
    outer.set_born_host(Some(MessageHostAddress::new("192.0.2.10", 10911)));
    outer.set_store_timestamp(12);
    outer.set_store_host(Some(MessageHostAddress::new("198.51.100.20", 10912)));
    outer.set_commit_log_offset(13);
    outer.set_wait_store_msg_ok(false).unwrap();
    let expanded = decode_inner_batch(&outer).unwrap();
    assert_eq!(expanded.len(), 2);
    for (message, case) in expanded
        .iter()
        .zip(expected["expanded"].as_array().unwrap())
    {
        assert_eq!(message.topic(), case["topic"]);
        assert_eq!(message.queue_id(), case["queueId"]);
        assert_eq!(message.queue_offset(), case["queueOffset"]);
        assert_eq!(message.flag(), case["flag"]);
        assert_eq!(
            base64::engine::general_purpose::STANDARD.encode(message.body()),
            case["bodyBase64"]
        );
        assert_eq!(
            serde_json::to_value(message.properties()).unwrap(),
            case["properties"]
        );
        assert_eq!(message.is_wait_store_msg_ok(), case["waitStore"]);
        assert_eq!(message.sys_flag(), case["sysFlag"]);
        assert_eq!(message.born_timestamp(), case["bornTimestamp"]);
        assert_eq!(message.born_host().unwrap().to_string(), case["bornHost"]);
        assert_eq!(message.store_timestamp(), case["storeTimestamp"]);
        assert_eq!(message.store_host().unwrap().to_string(), case["storeHost"]);
        assert_eq!(message.commit_log_offset(), case["commitLogOffset"]);
    }

    assert_eq!(
        encode_compact_message(&Message::default()),
        Err(CompactMessageError::MissingBody)
    );
    assert!(expected["nullBodyEncodeThrows"].as_bool().unwrap());
    assert!(
        expected["shortDecodeThrowsBufferUnderflow"]
            .as_bool()
            .unwrap()
    );
    assert!(expected["negativeBodyDecodeThrows"].as_bool().unwrap());
    assert!(
        expected["incompleteCountThrowsIllegalArgument"]
            .as_bool()
            .unwrap()
    );
    assert_eq!(
        decode_compact_message(&[0]).unwrap_err(),
        CompactMessageError::IncompleteField {
            field: "total size",
            length: 4
        }
    );
    let mut negative_body = vec![0; 16];
    negative_body.extend_from_slice(&(-1_i32).to_be_bytes());
    assert_eq!(
        decode_compact_message(&negative_body).unwrap_err(),
        CompactMessageError::NegativeLength {
            field: "body",
            length: -1
        }
    );
    assert_eq!(
        count_inner_messages(&[0, 0, 0, 5]).unwrap_err(),
        CompactMessageError::IncompleteInnerMessage {
            offset: 0,
            size: 5,
            available: 4
        }
    );
}

#[test]
fn message_ext_encoder_matches_java_551() {
    fn message_ext_for_encoding(sys_flag: i32, born_ip: &str, store_ip: &str) -> MessageExt {
        let mut message_ext = MessageExt::default();
        message_ext.set_topic("orders");
        message_ext.set_body([0, 255, 42, 55]);
        message_ext.set_flag(-23);
        message_ext.set_system_property("k", "v").unwrap();
        message_ext.set_body_crc(0x1234_5678);
        message_ext.set_queue_id(7);
        message_ext.set_queue_offset(23);
        message_ext.set_commit_log_offset(4567);
        message_ext.set_sys_flag(sys_flag);
        message_ext.set_born_timestamp(123);
        message_ext.set_born_host(Some(MessageHostAddress::from_ip_address(
            born_ip.parse().unwrap(),
            10_911,
        )));
        message_ext.set_store_timestamp(234);
        message_ext.set_store_host(Some(MessageHostAddress::from_ip_address(
            store_ip.parse().unwrap(),
            10_912,
        )));
        message_ext.set_reconsume_times(2);
        message_ext.set_prepared_transaction_offset(345);
        message_ext
    }

    let expected = &fixture()["messageExtEncodeApi"];
    let ipv4 = message_ext_for_encoding(0, "192.0.2.10", "198.51.100.20");
    let ipv4_encoded = encode_message_ext(&ipv4, false).unwrap();
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(&ipv4_encoded),
        expected["ipv4StandardBase64"]
    );
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .encode(encode_message_ext_uniquely(&ipv4, false).unwrap()),
        expected["ipv4UniqueBase64"]
    );

    let ipv6_flag = message::BORNHOST_V6_FLAG | message::STOREHOSTADDRESS_V6_FLAG;
    let ipv6 = message_ext_for_encoding(ipv6_flag, "2001:db8::10", "2001:db8::20");
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(encode_message_ext(&ipv6, false).unwrap()),
        expected["ipv6StandardBase64"]
    );
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .encode(encode_message_ext_uniquely(&ipv6, false).unwrap()),
        expected["ipv6UniqueBase64"]
    );

    let mut padded = ipv4.clone();
    padded.set_store_size(ipv4_encoded.len() as i32 + 3);
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .encode(encode_message_ext(&padded, false).unwrap()),
        expected["paddedStandardBase64"]
    );

    let mut too_small = ipv4.clone();
    too_small.set_store_size(1);
    assert!(encode_message_ext(&too_small, false).is_err());
    assert_eq!(expected["tooSmallStoreSizeThrows"], true);

    for case in expected["compressed"].as_array().unwrap() {
        let sys_flag = message::COMPRESSED_FLAG | case["compressionType"].as_i64().unwrap() as i32;
        let compressed = message_ext_for_encoding(sys_flag, "192.0.2.10", "198.51.100.20");
        let encoded = encode_message_ext(&compressed, true).unwrap();
        let (decoded, consumed) = decode_one(&encoded, DecodeOptions::default()).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(
            base64::engine::general_purpose::STANDARD.encode(decoded.body()),
            case["decodedBodyBase64"]
        );
        assert_eq!(decoded.sys_flag(), case["decodedSysFlag"]);
    }
}

#[test]
fn nullable_message_property_keys_match_java_551() {
    let expected = &fixture()["messagePublicApi"];
    let mut message = Message::default();
    assert_eq!(
        message.set_property_nullable(None, Some("value".to_owned())),
        None
    );
    assert_eq!(
        message.get_nullable_property(None).as_deref(),
        Some("value")
    );
    assert!(message.has_nullable_property(None));
    assert_eq!(expected["nullKeyPropertyPresent"], true);
    assert_eq!(expected["nullKeyPropertyValue"], "value");
    assert_eq!(message.to_string(), expected["nullKeyPropertyString"]);
    let entries = message.nullable_property_entries().collect::<Vec<_>>();
    assert_eq!(entries, vec![(None, Some("value".to_owned()))]);
    assert_eq!(
        encode_nullable_property_entries(Some(&entries)),
        expected["nullKeyPropertyEncoded"]
    );

    let serialized = serde_json::to_value(&message).unwrap();
    assert_eq!(serialized["nullKeyProperty"]["value"], "value");
    let round_trip: Message = serde_json::from_value(serialized).unwrap();
    assert_eq!(
        round_trip.get_nullable_property(None).as_deref(),
        Some("value")
    );
    assert!(round_trip.has_nullable_property(None));

    let mut compact_message = Message::new("orders", [1, 2, 3]);
    compact_message.set_property_nullable(None, Some("value".to_owned()));
    let compact = encode_compact_message(&compact_message).unwrap();
    let (decoded_compact, consumed) = decode_compact_message(&compact).unwrap();
    assert_eq!(consumed, compact.len());
    assert_eq!(
        decoded_compact.get_property("null").as_deref(),
        Some("value")
    );

    let mut message_ext = MessageExt::default();
    message_ext.set_topic("orders");
    message_ext.set_body([1, 2, 3]);
    message_ext.set_property_nullable(None, Some("value".to_owned()));
    message_ext.set_born_host(Some(MessageHostAddress::from_ip_address(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        10_911,
    )));
    message_ext.set_store_host(Some(MessageHostAddress::from_ip_address(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        10_912,
    )));
    let record = encode_message_ext(&message_ext, false).unwrap();
    let record_properties = decode_record_properties(&record).unwrap().unwrap();
    assert_eq!(
        record_properties.get("null").map(String::as_str),
        Some("value")
    );

    let previous = message.set_property_nullable(None, None);
    assert_eq!(previous, Some(Some("value".to_owned())));
    assert!(message.has_nullable_property(None));
    assert_eq!(message.get_nullable_property(None), None);
    assert_eq!(expected["nullKeyNullValuePresent"], true);
    assert_eq!(expected["nullKeyNullValueEncoded"], "");
    assert_eq!(
        encode_nullable_property_entries(Some(
            &message.nullable_property_entries().collect::<Vec<_>>()
        )),
        expected["nullKeyNullValueEncoded"]
    );
    assert_eq!(message.clear_property_nullable(None), Some(None));
    assert!(!message.has_nullable_property(None));
    assert!(message.get_properties().is_some());
    assert_eq!(expected["nullKeyPropertyCleared"], true);
    assert_eq!(expected["nullKeyPropertyMapRemainsPresent"], true);

    let mut nullable_properties =
        rocketmq_classic_common::message::NullableMessageProperties::new();
    nullable_properties.insert(None, Some("value".to_owned()));
    nullable_properties.insert(Some("nullable".to_owned()), None);
    message.set_nullable_properties(Some(nullable_properties));
    assert_eq!(
        message.get_nullable_property(None).as_deref(),
        Some("value")
    );
    assert_eq!(message.get_property("nullable").as_deref(), None);
    assert!(message.has_property("nullable"));
    message.set_properties(None);
    assert!(!message.has_nullable_property(None));

    let mut null_key_replacement =
        rocketmq_classic_common::message::NullableMessageProperties::new();
    null_key_replacement.insert(None, Some("replacement".to_owned()));
    message.set_nullable_properties(Some(null_key_replacement));
    assert_eq!(
        message.get_nullable_property(None).as_deref(),
        Some("replacement")
    );
    assert_eq!(expected["setPropertiesNullKeyPresent"], true);
    assert_eq!(expected["setPropertiesNullKeyValue"], "replacement");
    assert_eq!(
        encode_nullable_property_entries(Some(
            &message.nullable_property_entries().collect::<Vec<_>>()
        )),
        expected["setPropertiesNullKeyEncoded"]
    );
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

    let mut direct_setters = Message::default();
    assert_eq!(
        direct_setters.transaction_id(),
        expected["defaultTransactionId"].as_str()
    );
    assert_eq!(direct_setters.set_tags("setter-tag"), Ok(None));
    assert_eq!(direct_setters.set_keys("setter-key"), Ok(None));
    assert_eq!(direct_setters.set_wait_store_msg_ok(false), Ok(None));
    direct_setters.set_transaction_id(Some("transaction-setter".to_owned()));
    assert_eq!(
        direct_setters.tags().as_deref(),
        expected["setTagsGetter"].as_str()
    );
    assert_eq!(
        direct_setters.keys().as_deref(),
        expected["setKeysStringGetter"].as_str()
    );
    assert_eq!(
        direct_setters.is_wait_store_msg_ok(),
        expected["setWaitStoreMsgOKFalse"]
    );
    assert_eq!(
        direct_setters
            .get_property(rocketmq_classic_common::message::constants::PROPERTY_WAIT_STORE_MSG_OK)
            .as_deref(),
        expected["setWaitStoreMsgOKProperty"].as_str()
    );
    assert_eq!(
        direct_setters.transaction_id(),
        expected["setTransactionIdGetter"].as_str()
    );
    assert_eq!(
        direct_setters.set_wait_store_msg_ok(true),
        Ok(Some("false".to_owned()))
    );
    assert_eq!(
        direct_setters.is_wait_store_msg_ok(),
        expected["setWaitStoreMsgOKTrue"]
    );
    direct_setters.set_tags_nullable(None);
    direct_setters.set_keys_nullable(None);
    direct_setters.set_transaction_id(None);
    assert!(direct_setters.has_property("TAGS"));
    assert!(direct_setters.has_property("KEYS"));
    assert_eq!(direct_setters.tags(), None);
    assert_eq!(direct_setters.keys(), None);
    assert_eq!(direct_setters.transaction_id(), None);
    assert_eq!(expected["nullSetterTagsRemainsPresent"], true);
    assert_eq!(expected["nullSetterTagsGetter"], serde_json::Value::Null);
    assert_eq!(expected["nullSetterKeysRemainsPresent"], true);
    assert_eq!(expected["nullSetterKeysGetter"], serde_json::Value::Null);
    assert_eq!(expected["nullSetterTransactionId"], serde_json::Value::Null);

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
    let mut priority = Message::default();
    assert_eq!(
        priority.priority(),
        expected["absentPriority"].as_i64().unwrap() as i32
    );
    assert_eq!(expected["absentPriorityInitializesProperties"], true);
    assert_eq!(
        priority
            .get_properties_mut()
            .map(|properties| properties.is_empty()),
        Some(true)
    );
    assert_eq!(priority.set_priority(17), Ok(()));
    assert_eq!(
        priority.priority(),
        expected["setPriority"].as_i64().unwrap() as i32
    );
    assert_eq!(
        priority
            .get_property(rocketmq_classic_common::message::constants::PROPERTY_PRIORITY)
            .as_deref(),
        Some(expected["setPriorityProperty"].as_str().unwrap())
    );
    assert_eq!(
        priority.set_priority(-1),
        Err(rocketmq_classic_common::message::MessagePropertyError::NegativePriority)
    );
    assert_eq!(expected["negativePriorityRejected"], true);
    assert_eq!(
        rocketmq_classic_common::message::MessagePropertyError::NegativePriority.to_string(),
        expected["negativePriorityErrorMessage"].as_str().unwrap()
    );
    assert_eq!(
        priority.priority(),
        expected["priorityAfterNegative"].as_i64().unwrap() as i32
    );
    assert_eq!(priority.set_priority(0), Ok(()));
    assert_eq!(
        priority.priority(),
        expected["zeroPriority"].as_i64().unwrap() as i32
    );
    for input in expected["priorityInputs"].as_array().unwrap() {
        let mut candidate = Message::default();
        candidate
            .set_system_property(
                rocketmq_classic_common::message::constants::PROPERTY_PRIORITY,
                input["input"].as_str().unwrap(),
            )
            .unwrap();
        assert_eq!(
            candidate.priority(),
            input["priority"].as_i64().unwrap() as i32
        );
    }
    let dynamic_reserved_property = "__RMQ_DYNAMIC_RESERVED_FIXTURE__";
    let dynamic_reserved_key = Some(dynamic_reserved_property.to_owned());
    let dynamic_was_reserved = {
        let mut properties = rocketmq_classic_common::message::constants::STRING_HASH_SET
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let existed = properties.contains(&dynamic_reserved_key);
        properties.insert(dynamic_reserved_key.clone());
        existed
    };
    let dynamic_reserved_result =
        Message::default().put_user_property(dynamic_reserved_property, "value");
    if !dynamic_was_reserved {
        rocketmq_classic_common::message::constants::STRING_HASH_SET
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&dynamic_reserved_key);
    }
    assert_eq!(
        dynamic_reserved_result,
        Err(rocketmq_classic_common::message::MessagePropertyError::ReservedProperty)
    );
    let null_reserved_was_reserved = {
        let mut properties = rocketmq_classic_common::message::constants::STRING_HASH_SET
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let existed = properties.contains(&None);
        properties.insert(None);
        existed
    };
    let null_reserved_result =
        Message::default().put_user_property_nullable(None, Some("value".to_owned()));
    if !null_reserved_was_reserved {
        rocketmq_classic_common::message::constants::STRING_HASH_SET
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&None);
    }
    assert_eq!(
        null_reserved_result,
        Err(rocketmq_classic_common::message::MessagePropertyError::ReservedProperty)
    );
    assert_eq!(expected["mutatedNullReservedPropertyPresent"], true);
    assert_eq!(
        expected["nullReservedPropertyExceptionClass"],
        "java.lang.RuntimeException"
    );
    let empty_reserved_key = Some(String::new());
    let empty_reserved_was_reserved = {
        let mut properties = rocketmq_classic_common::message::constants::STRING_HASH_SET
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let existed = properties.contains(&empty_reserved_key);
        properties.insert(empty_reserved_key.clone());
        existed
    };
    let empty_reserved_result = Message::default().put_user_property("", "value");
    if !empty_reserved_was_reserved {
        rocketmq_classic_common::message::constants::STRING_HASH_SET
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&empty_reserved_key);
    }
    assert_eq!(
        empty_reserved_result,
        Err(rocketmq_classic_common::message::MessagePropertyError::ReservedProperty)
    );
    assert_eq!(expected["mutatedEmptyReservedPropertyPresent"], true);
    assert_eq!(
        expected["emptyReservedPropertyExceptionClass"],
        "java.lang.RuntimeException"
    );
    let keys_was_reserved = rocketmq_classic_common::message::constants::STRING_HASH_SET
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(&Some(
            rocketmq_classic_common::message::constants::PROPERTY_KEYS.to_owned(),
        ));
    let removed_reserved_result = Message::default().put_user_property(
        rocketmq_classic_common::message::constants::PROPERTY_KEYS,
        "user-value",
    );
    if keys_was_reserved {
        rocketmq_classic_common::message::constants::STRING_HASH_SET
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(Some(
                rocketmq_classic_common::message::constants::PROPERTY_KEYS.to_owned(),
            ));
    }
    assert!(keys_was_reserved);
    assert_eq!(removed_reserved_result, Ok(None));
    assert_eq!(expected["mutatedReservedPropertyRejected"], true);
    assert_eq!(expected["removedReservedPropertyAccepted"], true);
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

    let mut replaced_properties = Message::default();
    replaced_properties.set_properties(Some(std::collections::BTreeMap::from([(
        "old".to_owned(),
        "value".to_owned(),
    )])));
    assert_eq!(
        replaced_properties.get_property("old").as_deref(),
        Some("value")
    );
    assert_eq!(expected["setPropertiesInitialValue"], "value");
    replaced_properties.set_properties(Some(std::collections::BTreeMap::from([(
        "new".to_owned(),
        "replacement".to_owned(),
    )])));
    assert!(!replaced_properties.has_property("old"));
    assert_eq!(
        replaced_properties.get_property("new").as_deref(),
        Some("replacement")
    );
    assert_eq!(expected["setPropertiesReplacesOldKey"], true);
    assert_eq!(expected["setPropertiesReplacementValue"], "replacement");
    replaced_properties.set_properties(Some(std::collections::BTreeMap::new()));
    assert_eq!(
        replaced_properties
            .get_properties()
            .map(|map| map.is_empty()),
        Some(true)
    );
    assert_eq!(expected["setPropertiesEmptyMapIsPresent"], true);
    replaced_properties.set_properties(None);
    assert_eq!(replaced_properties.get_properties(), None);
    assert_eq!(expected["setPropertiesNullMapIsNull"], true);

    assert_eq!(expected["deepCopyPropertiesNullMapIsNull"], true);
    assert_eq!(expected["deepCopyPropertiesReturnsDistinctMap"], true);
    assert_eq!(expected["deepCopyPropertiesPreservesNullKeyAndValue"], true);
    assert_eq!(expected["deepCopyPropertiesInitialValue"], "original");
    assert_eq!(expected["deepCopyPropertiesIgnoresSourceMutation"], true);
    assert_eq!(
        expected["deepCopyPropertiesSourceIgnoresCopyMutation"],
        true
    );
    let mut accessor_clone_source = Message::new("clone-topic", [0, 255, 42]);
    accessor_clone_source.set_flag(29);
    accessor_clone_source.set_transaction_id(Some("source-transaction".to_owned()));
    accessor_clone_source
        .put_user_property("clone-key", "clone-value")
        .unwrap();
    let accessor_clone = accessor_clone_source.clone_message();
    assert!(!std::ptr::eq(&accessor_clone, &accessor_clone_source));
    assert_eq!(expected["accessorCloneIsDistinct"], true);
    assert_eq!(accessor_clone.topic(), expected["accessorCloneTopic"]);
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(accessor_clone.body()),
        expected["accessorCloneBodyBase64"]
    );
    assert_eq!(accessor_clone.flag(), expected["accessorCloneFlag"]);
    assert_eq!(
        accessor_clone.get_property("clone-key").as_deref(),
        expected["accessorCloneProperty"].as_str()
    );
    assert_eq!(accessor_clone.transaction_id(), None);
    assert_eq!(expected["accessorCloneTransactionIdIsNull"], true);
    assert_eq!(
        accessor_clone_source.transaction_id(),
        expected["accessorCloneSourceTransactionId"].as_str()
    );
    let mut alias_source = Message::new("clone-alias-topic", [1, 2, 3]);
    alias_source.set_properties(Some(std::collections::BTreeMap::from([(
        "shared".to_owned(),
        "initial".to_owned(),
    )])));
    let mut alias_clone = alias_source.clone_message();
    assert_eq!(expected["accessorCloneSharesBodyArray"], true);
    assert_eq!(expected["accessorCloneSharesPropertiesMap"], true);
    alias_clone.body_mut().unwrap()[0] = expected["accessorCloneBodyMutationVisibleInSource"]
        .as_u64()
        .unwrap() as u8;
    assert_eq!(
        alias_source.body()[0],
        expected["accessorCloneBodyMutationVisibleInSource"]
            .as_u64()
            .unwrap() as u8
    );
    alias_source
        .set_system_property("source-only", "source")
        .unwrap();
    assert_eq!(
        alias_clone.get_property("source-only").as_deref(),
        expected["accessorClonePropertyMutationVisibleInClone"].as_str()
    );
    alias_clone
        .set_system_property("clone-only", "clone")
        .unwrap();
    assert_eq!(
        alias_source.get_property("clone-only").as_deref(),
        expected["accessorClonePropertyMutationVisibleInSource"].as_str()
    );
    alias_clone.set_body([7, 8]);
    assert_eq!(expected["accessorCloneSetBodyDetachesArray"], true);
    assert_eq!(
        alias_source.body()[0],
        expected["accessorCloneSetBodyLeavesSourceArray"]
            .as_u64()
            .unwrap() as u8
    );
    alias_clone.set_properties(Some(std::collections::BTreeMap::from([(
        "detached".to_owned(),
        "value".to_owned(),
    )])));
    assert_eq!(expected["accessorCloneSetPropertiesDetachesMap"], true);
    alias_clone
        .set_system_property("clone-after-detach", "clone")
        .unwrap();
    assert_eq!(
        expected["accessorCloneDetachedPropertyMutationLeavesSource"],
        !alias_source.has_property("clone-after-detach")
    );
    let mut independent_clone = accessor_clone.clone();
    independent_clone.set_body([9, 8, 7]);
    independent_clone
        .put_user_property("copy-only", "copy")
        .unwrap();
    assert_eq!(accessor_clone_source.body().as_ref(), &[0, 255, 42]);
    assert_eq!(accessor_clone_source.get_property("copy-only"), None);
    let mut copy_source = rocketmq_classic_common::message::NullableMessageProperties::new();
    copy_source.insert(None, None);
    copy_source.insert(Some("present".to_owned()), Some("original".to_owned()));
    let mut copied_properties = copy_source.clone();
    copy_source.insert(Some("source-only".to_owned()), Some("source".to_owned()));
    assert!(!copied_properties.contains_key(&Some("source-only".to_owned())));
    copied_properties.insert(Some("copy-only".to_owned()), Some("copy".to_owned()));
    assert!(!copy_source.contains_key(&Some("copy-only".to_owned())));
    assert_eq!(copied_properties.get(&None), Some(&None));
    assert_eq!(
        copied_properties.get(&Some("present".to_owned())),
        Some(&Some("original".to_owned()))
    );
    let absent_properties: Option<rocketmq_classic_common::message::NullableMessageProperties> =
        None;
    assert_eq!(absent_properties.clone(), None);

    let helper_values = &expected["messageAccessorHelperValues"];
    let mut accessor_helpers = Message::default();
    for (property, value, key) in [
        (
            rocketmq_classic_common::message::constants::PROPERTY_TRANSFER_FLAG,
            "transfer",
            "transferFlag",
        ),
        (
            rocketmq_classic_common::message::constants::PROPERTY_CORRECTION_FLAG,
            "correction",
            "correctionFlag",
        ),
        (
            rocketmq_classic_common::message::constants::PROPERTY_ORIGIN_MESSAGE_ID,
            "origin-id",
            "originMessageId",
        ),
        (
            rocketmq_classic_common::message::constants::PROPERTY_MQ2_FLAG,
            "mq2",
            "mq2Flag",
        ),
        (
            rocketmq_classic_common::message::constants::PROPERTY_RECONSUME_TIME,
            "4",
            "reconsumeTime",
        ),
        (
            rocketmq_classic_common::message::constants::PROPERTY_MAX_RECONSUME_TIMES,
            "8",
            "maxReconsumeTimes",
        ),
        (
            rocketmq_classic_common::message::constants::PROPERTY_CONSUME_START_TIMESTAMP,
            "1700000000123",
            "consumeStartTimeStamp",
        ),
    ] {
        accessor_helpers
            .set_system_property(property, value)
            .unwrap();
        assert_eq!(
            accessor_helpers.get_property(property).as_deref(),
            helper_values[key].as_str()
        );
    }
    accessor_helpers.set_system_property_nullable(
        rocketmq_classic_common::message::constants::PROPERTY_TRANSFER_FLAG,
        None,
    );
    assert!(
        accessor_helpers
            .has_property(rocketmq_classic_common::message::constants::PROPERTY_TRANSFER_FLAG)
    );
    assert_eq!(
        accessor_helpers
            .get_property(rocketmq_classic_common::message::constants::PROPERTY_TRANSFER_FLAG),
        None
    );
    assert_eq!(helper_values["transferFlagNullIsPresent"], true);
    assert_eq!(
        helper_values["transferFlagNullValue"],
        serde_json::Value::Null
    );

    let mut collection_with_null = Message::default();
    collection_with_null
        .set_keys_from_nullable(Some(vec![Some("left"), None, Some("right")]))
        .unwrap();
    assert_eq!(
        collection_with_null.keys().as_deref(),
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
    assert_eq!(message.tags().as_deref(), expected["tags"].as_str());
    assert_eq!(message.keys().as_deref(), expected["keys"].as_str());
    assert_eq!(message.flag(), expected["flag"]);
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(message.body()),
        expected["body"]
    );
    assert_eq!(message.is_wait_store_msg_ok(), expected["waitStore"]);
    assert_eq!(
        message.get_property("TAGS").as_deref(),
        expected["propertyTags"].as_str()
    );
    assert_eq!(
        message.get_user_property("TAGS").as_deref(),
        expected["userPropertyTags"].as_str()
    );
    assert_eq!(message.get_property("missing"), None);
    assert_eq!(message.has_property("TAGS"), expected["hasTags"]);
    assert_eq!(message.has_property("missing"), expected["hasMissing"]);

    message.set_keys_from(["third", "fourth"]);
    assert_eq!(
        message.keys().as_deref(),
        expected["collectionKeys"].as_str()
    );
    message.set_buyer_id("buyer-1");
    message.set_instance_id("instance-1");
    assert_eq!(message.buyer_id().as_deref(), expected["buyerId"].as_str());
    assert_eq!(
        message.get_property("INSTANCE_ID").as_deref(),
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
        diagnostic.get_property("DIRECT").as_deref(),
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
        message.set_system_property(key, "").unwrap();
    }
    assert_eq!(expected["topicSysFlagEmptyIsNull"], true);
    assert_eq!(expected["groupSysFlagEmptyIsNull"], true);
    assert_eq!(message.topic_sys_flag().unwrap(), None);
    assert_eq!(message.group_sys_flag().unwrap(), None);

    for key in [
        rocketmq_classic_common::message::constants::PROPERTY_TRANSIENT_TOPIC_CONFIG,
        rocketmq_classic_common::message::constants::PROPERTY_TRANSIENT_GROUP_CONFIG,
    ] {
        message.set_system_property(key, "bad").unwrap();
    }
    assert_eq!(expected["topicSysFlagMalformedThrows"], true);
    assert_eq!(expected["groupSysFlagMalformedThrows"], true);
    assert!(message.topic_sys_flag().is_err());
    assert!(message.group_sys_flag().is_err());
    message.set_topic_sys_flag(Some(1));
    message
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
                    decoded.body().to_vec(),
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
                    decoded.body().is_empty(),
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
            serde_json::to_value(with_properties_string.properties()).unwrap(),
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
        assert_eq!(preserved.body().as_ref(), encoded_body(&compressed));
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
            assert!(skipped.body().is_empty());
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
    assert!(decoded_empty.body().is_empty());
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
