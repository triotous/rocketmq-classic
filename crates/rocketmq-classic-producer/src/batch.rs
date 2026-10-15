use rocketmq_classic_common::message::{Message, constants::*, encode_nullable_property_entries};
use rocketmq_classic_core::unique_id::UniqueIdGenerator;

use crate::{ProducerConfig, ProducerError, validation};

pub(crate) fn create_batch(
    messages: &[Message],
    config: &ProducerConfig,
    ids: &UniqueIdGenerator,
) -> Result<Message, ProducerError> {
    let Some(first) = messages.first() else {
        return Err(ProducerError::EmptyBatch);
    };
    let topic = first.topic();
    let wait_store_msg_ok = first.is_wait_store_msg_ok();
    let mut encoded_messages = Vec::with_capacity(messages.len());
    let mut total_size = 0usize;

    for message in messages {
        if message.topic() != topic {
            return Err(ProducerError::BatchTopicMismatch);
        }
        if message.is_wait_store_msg_ok() != wait_store_msg_ok {
            return Err(ProducerError::BatchWaitStoreMismatch);
        }
        if topic.starts_with("%RETRY%") {
            return Err(ProducerError::RetryBatchMessage);
        }
        validate_batch_delay(message)?;
        validation::validate_message(Some(message), config)?;

        let mut copy = message.clone();
        ids.set_if_missing(&mut copy);
        let encoded = encode_message(&copy)?;
        total_size = total_size
            .checked_add(encoded.len())
            .ok_or(ProducerError::BatchTooLarge {
                actual: usize::MAX,
                maximum: config.max_message_size(),
            })?;
        encoded_messages.push(encoded);
    }

    if total_size as i64 > config.max_message_size() as i64 {
        return Err(ProducerError::BatchTooLarge {
            actual: total_size,
            maximum: config.max_message_size(),
        });
    }

    let mut body = Vec::with_capacity(total_size);
    for encoded in encoded_messages {
        body.extend_from_slice(&encoded);
    }
    let mut batch = Message::new(topic, body);
    let _ = batch.set_wait_store_msg_ok(wait_store_msg_ok);
    ids.set_if_missing(&mut batch);
    Ok(batch)
}

fn validate_batch_delay(message: &Message) -> Result<(), ProducerError> {
    let property_positive = |property: &str, long: bool| -> Result<bool, ProducerError> {
        let Some(value) = message.properties().get(property) else {
            return Ok(false);
        };
        if long {
            value
                .parse::<i64>()
                .map(|value| value > 0)
                .map_err(|_| ProducerError::InvalidBatchDelay)
        } else {
            value
                .parse::<i32>()
                .map(|value| value > 0)
                .map_err(|_| ProducerError::InvalidBatchDelay)
        }
    };
    if property_positive(PROPERTY_DELAY_TIME_LEVEL, false)?
        || property_positive(PROPERTY_TIMER_DELAY_MS, true)?
        || property_positive(PROPERTY_TIMER_DELAY_SEC, true)?
        || property_positive(PROPERTY_TIMER_DELIVER_MS, true)?
    {
        Err(ProducerError::DelayedBatchMessage)
    } else {
        Ok(())
    }
}

fn encode_message(message: &Message) -> Result<Vec<u8>, ProducerError> {
    let body = message.body();
    let property_entries = message.nullable_property_entries().collect::<Vec<_>>();
    let properties = encode_nullable_property_entries(Some(&property_entries));
    let properties = properties.as_bytes();
    if properties.len() > i16::MAX as usize {
        return Err(ProducerError::BatchPropertiesTooLarge);
    }
    let body_len = u32::try_from(body.len()).map_err(|_| ProducerError::BatchTooLarge {
        actual: body.len(),
        maximum: i32::MAX,
    })?;
    let property_len =
        u16::try_from(properties.len()).map_err(|_| ProducerError::BatchPropertiesTooLarge)?;
    let size = 4usize
        .checked_add(4 + 4 + 4 + 4)
        .and_then(|size| size.checked_add(body.len()))
        .and_then(|size| size.checked_add(2 + properties.len()))
        .ok_or(ProducerError::BatchTooLarge {
            actual: usize::MAX,
            maximum: i32::MAX,
        })?;
    let size = u32::try_from(size).map_err(|_| ProducerError::BatchTooLarge {
        actual: size,
        maximum: i32::MAX,
    })?;

    let mut encoded = Vec::with_capacity(size as usize);
    encoded.extend_from_slice(&size.to_be_bytes());
    encoded.extend_from_slice(&0_i32.to_be_bytes()); // Java's batch MAGICCODE placeholder.
    encoded.extend_from_slice(&0_i32.to_be_bytes()); // Java's batch BODYCRC placeholder.
    encoded.extend_from_slice(&message.flag().to_be_bytes());
    encoded.extend_from_slice(&body_len.to_be_bytes());
    encoded.extend_from_slice(body);
    encoded.extend_from_slice(&property_len.to_be_bytes());
    encoded.extend_from_slice(properties);
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use rocketmq_classic_common::message::constants::PROPERTY_DELAY_TIME_LEVEL;

    use super::*;

    fn config() -> ProducerConfig {
        ProducerConfig::new("batch-producer").unwrap()
    }

    #[test]
    fn batch_body_matches_message_decoder_record_layout_and_does_not_mutate_inputs() {
        let config = config();
        let ids = UniqueIdGenerator::new(Some("10.10.10.111"));
        let first = Message::new("orders", b"first".to_vec());
        let second = Message::new("orders", b"second".to_vec());
        let batch = create_batch(&[first.clone(), second.clone()], &config, &ids).unwrap();
        assert_eq!(batch.topic(), "orders");
        assert_eq!(first.body(), b"first");
        assert_eq!(second.body(), b"second");
        assert!(
            !first
                .properties()
                .contains_key(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
        );
        let first_record_size = u32::from_be_bytes(batch.body()[0..4].try_into().unwrap()) as usize;
        assert_eq!(&batch.body()[4..12], &[0; 8]);
        let first_body_size = u32::from_be_bytes(batch.body()[16..20].try_into().unwrap()) as usize;
        let first_properties_size = u16::from_be_bytes(
            batch.body()[20 + first_body_size..22 + first_body_size]
                .try_into()
                .unwrap(),
        ) as usize;
        assert_eq!(
            first_record_size,
            22 + first_body_size + first_properties_size
        );
        assert_eq!(first_body_size, 5);
        let first_properties =
            std::str::from_utf8(&batch.body()[22 + first_body_size..first_record_size]).unwrap();
        assert!(first_properties.contains("UNIQ_KEY"));
        assert!(first_properties.contains("WAIT"));
        let second_record_size = u32::from_be_bytes(
            batch.body()[first_record_size..first_record_size + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        assert_eq!(second_record_size, batch.body().len() - first_record_size);
    }

    #[test]
    fn batch_checks_source_supported_rejections_and_aggregate_size() {
        let config = config();
        let ids = UniqueIdGenerator::new(Some("10.10.10.111"));
        assert!(matches!(
            create_batch(&[], &config, &ids),
            Err(ProducerError::EmptyBatch)
        ));
        let first = Message::new("orders", b"first".to_vec());
        let second = Message::new("payments", b"second".to_vec());
        assert!(matches!(
            create_batch(&[first, second], &config, &ids),
            Err(ProducerError::BatchTopicMismatch)
        ));

        let mut delayed = Message::new("orders", b"x".to_vec());
        delayed
            .set_system_property(PROPERTY_DELAY_TIME_LEVEL, "1")
            .unwrap();
        assert!(matches!(
            create_batch(&[delayed], &config, &ids),
            Err(ProducerError::DelayedBatchMessage)
        ));

        let mut small = ProducerConfig::new("batch-producer").unwrap();
        small.set_max_message_size(64);
        let messages = vec![Message::new("orders", b"x".to_vec()); 2];
        assert!(matches!(
            create_batch(&messages, &small, &ids),
            Err(ProducerError::BatchTooLarge { .. })
        ));
    }

    #[test]
    fn encoded_batch_size_limit_includes_inserted_unique_ids() {
        let messages = [
            Message::new("orders", b"first".to_vec()),
            Message::new("orders", b"second".to_vec()),
        ];
        let ids = UniqueIdGenerator::new(Some("10.10.10.111"));
        let baseline = create_batch(&messages, &config(), &ids).unwrap();
        let encoded_size = baseline.body().len();

        let mut exact_limit = config();
        exact_limit.set_max_message_size(encoded_size as i32);
        assert!(create_batch(&messages, &exact_limit, &ids).is_ok());

        let mut one_byte_too_small = config();
        one_byte_too_small.set_max_message_size(encoded_size as i32 - 1);
        assert!(matches!(
            create_batch(&messages, &one_byte_too_small, &ids),
            Err(ProducerError::BatchTooLarge { actual, .. }) if actual == encoded_size
        ));
    }

    #[test]
    fn batch_record_bytes_match_pinned_java_message_decoder_fixture() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let messages = [
            Message::new("orders", b"first".to_vec()),
            Message::new("orders", b"second".to_vec()),
        ];
        let mut actual = Vec::new();
        for message in &messages {
            actual.extend_from_slice(&encode_message(message).unwrap());
        }
        let mut actual_hex = String::with_capacity(actual.len() * 2);
        for byte in &actual {
            use std::fmt::Write;
            write!(&mut actual_hex, "{byte:02X}").unwrap();
        }
        assert_eq!(
            actual_hex,
            fixture["batchEncoding"]["bodyHex"].as_str().unwrap()
        );
        assert_eq!(
            actual.len(),
            fixture["batchEncoding"]["length"].as_u64().unwrap() as usize
        );
    }

    #[test]
    fn batch_message_with_null_property_key_uses_java_wire_key() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/messages/records.json")).unwrap();
        let expected = fixture["messagePublicApi"]["nullKeyPropertyEncoded"]
            .as_str()
            .unwrap();
        let mut message = Message::new("orders", b"body".to_vec());
        message.set_property_nullable(None, Some("value".to_owned()));

        let encoded = encode_message(&message).unwrap();
        let size = u32::from_be_bytes(encoded[..4].try_into().unwrap()) as usize;
        let body_length = u32::from_be_bytes(encoded[16..20].try_into().unwrap()) as usize;
        let properties_length = u16::from_be_bytes(
            encoded[20 + body_length..22 + body_length]
                .try_into()
                .unwrap(),
        ) as usize;
        let properties = std::str::from_utf8(&encoded[22 + body_length..size]).unwrap();
        assert_eq!(properties.len(), properties_length);
        assert!(properties.contains(expected));

        let batch = create_batch(&[message], &config(), &UniqueIdGenerator::new(None)).unwrap();
        let first_size = u32::from_be_bytes(batch.body()[..4].try_into().unwrap()) as usize;
        let first_body_length =
            u32::from_be_bytes(batch.body()[16..20].try_into().unwrap()) as usize;
        let first_properties_length = u16::from_be_bytes(
            batch.body()[20 + first_body_length..22 + first_body_length]
                .try_into()
                .unwrap(),
        ) as usize;
        let first_properties =
            std::str::from_utf8(&batch.body()[22 + first_body_length..first_size]).unwrap();
        assert_eq!(first_properties.len(), first_properties_length);
        assert!(first_properties.contains(expected));
    }

    #[test]
    fn batch_validation_decisions_match_pinned_java_fixtures() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let config = config();
        let ids = UniqueIdGenerator::new(Some("10.10.10.111"));
        for row in fixture["batchValidation"].as_array().unwrap() {
            let valid = match row["input"].as_str().unwrap() {
                "empty" => create_batch(&[], &config, &ids).is_ok(),
                "mixed-topics" => create_batch(
                    &[
                        Message::new("orders", b"one".to_vec()),
                        Message::new("payments", b"two".to_vec()),
                    ],
                    &config,
                    &ids,
                )
                .is_ok(),
                "wait-store-mismatch" => {
                    let mut second = Message::new("orders", b"two".to_vec());
                    second.set_wait_store_msg_ok(false).unwrap();
                    create_batch(
                        &[Message::new("orders", b"one".to_vec()), second],
                        &config,
                        &ids,
                    )
                    .is_ok()
                }
                "delayed" => {
                    let mut message = Message::new("orders", b"one".to_vec());
                    message
                        .set_system_property(PROPERTY_DELAY_TIME_LEVEL, "1")
                        .unwrap();
                    create_batch(&[message], &config, &ids).is_ok()
                }
                "retry-topic" => create_batch(
                    &[Message::new("%RETRY%workers", b"one".to_vec())],
                    &config,
                    &ids,
                )
                .is_ok(),
                "different-tags-allowed" => {
                    let mut first = Message::new("orders", b"one".to_vec());
                    let mut second = Message::new("orders", b"two".to_vec());
                    first.set_tags("one").unwrap();
                    second.set_tags("two").unwrap();
                    create_batch(&[first, second], &config, &ids).is_ok()
                }
                other => panic!("unknown Java batch fixture case: {other}"),
            };
            assert_eq!(valid, row["valid"].as_bool().unwrap(), "{row}");
        }
    }
}
