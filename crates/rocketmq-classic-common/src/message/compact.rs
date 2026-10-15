use thiserror::Error;

use super::{Message, MessageExt, MessageProperties, decode_properties, encode_properties};

/// Failures while encoding or decoding Java's compact inner-message records.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CompactMessageError {
    #[error("compact message is missing its {field}")]
    Truncated { field: &'static str },
    #[error("compact message has negative {field} length {length}")]
    NegativeLength { field: &'static str, length: i32 },
    #[error("compact message {field} length {length} exceeds the available bytes")]
    IncompleteField { field: &'static str, length: usize },
    #[error("compact message body is absent")]
    MissingBody,
    #[error("compact message properties length {length} exceeds Java's signed-short range")]
    PropertiesTooLong { length: usize },
    #[error("compact message size exceeds Java's signed-int range")]
    SizeOverflow,
    #[error("inner-message size {size} is smaller than its four-byte size field")]
    InvalidInnerSize { size: i32 },
    #[error(
        "inner-message at byte {offset} declares {size} bytes but only {available} are available"
    )]
    IncompleteInnerMessage {
        offset: usize,
        size: usize,
        available: usize,
    },
}

/// Encode a Java `MessageDecoder.encodeMessage` compact record.
pub fn encode_compact_message(message: &Message) -> Result<Vec<u8>, CompactMessageError> {
    let body = message
        .body_nullable()
        .ok_or(CompactMessageError::MissingBody)?;
    let empty_properties = MessageProperties::new();
    let properties = message.properties_nullable().unwrap_or(&empty_properties);
    let property_bytes = encode_properties(properties).into_bytes();
    let properties_length = i16::try_from(property_bytes.len()).map_err(|_| {
        CompactMessageError::PropertiesTooLong {
            length: property_bytes.len(),
        }
    })?;
    let body_length = i32::try_from(body.len()).map_err(|_| CompactMessageError::SizeOverflow)?;
    let total_size = 22_usize
        .checked_add(body.len())
        .and_then(|size| size.checked_add(property_bytes.len()))
        .filter(|size| i32::try_from(*size).is_ok())
        .ok_or(CompactMessageError::SizeOverflow)?;

    let mut record = Vec::with_capacity(total_size);
    record.extend_from_slice(&(total_size as i32).to_be_bytes());
    record.extend_from_slice(&0_i32.to_be_bytes()); // compact magic code
    record.extend_from_slice(&0_i32.to_be_bytes()); // body CRC
    record.extend_from_slice(&message.flag().to_be_bytes());
    record.extend_from_slice(&body_length.to_be_bytes());
    record.extend_from_slice(body);
    record.extend_from_slice(&properties_length.to_be_bytes());
    record.extend_from_slice(&property_bytes);
    Ok(record)
}

/// Decode one Java `MessageDecoder.decodeMessage` compact record.
/// Returns the message and the number of bytes consumed.
pub fn decode_compact_message(record: &[u8]) -> Result<(Message, usize), CompactMessageError> {
    let mut input = CompactCursor::new(record);
    input.i32("total size")?;
    input.i32("magic code")?;
    input.i32("body CRC")?;
    let flag = input.i32("flag")?;
    let body_length = input.i32("body length")?;
    let body_length =
        usize::try_from(body_length).map_err(|_| CompactMessageError::NegativeLength {
            field: "body",
            length: body_length,
        })?;
    let body = input.bytes(body_length, "body")?.to_vec();
    let properties_length = input.i16("properties length")?;
    let properties_length =
        usize::try_from(properties_length).map_err(|_| CompactMessageError::NegativeLength {
            field: "properties",
            length: i32::from(properties_length),
        })?;
    let properties = input.bytes(properties_length, "properties")?;
    let properties = decode_properties(&String::from_utf8_lossy(properties));
    Ok((
        Message::from_compact(body, flag, properties),
        input.position(),
    ))
}

/// Encode multiple compact messages into one concatenated buffer.
pub fn encode_compact_messages(messages: &[Message]) -> Result<Vec<u8>, CompactMessageError> {
    let mut records = Vec::new();
    for message in messages {
        let record = encode_compact_message(message)?;
        records
            .len()
            .checked_add(record.len())
            .ok_or(CompactMessageError::SizeOverflow)?;
        records.extend_from_slice(&record);
    }
    Ok(records)
}

/// Decode concatenated compact message records.
pub fn decode_compact_messages(records: &[u8]) -> Result<Vec<Message>, CompactMessageError> {
    let mut messages = Vec::new();
    let mut offset = 0usize;
    while offset < records.len() {
        let (message, consumed) = decode_compact_message(&records[offset..])?;
        if consumed == 0 {
            return Err(CompactMessageError::InvalidInnerSize { size: 0 });
        }
        offset = offset
            .checked_add(consumed)
            .ok_or(CompactMessageError::SizeOverflow)?;
        messages.push(message);
    }
    Ok(messages)
}

/// Count concatenated inner records using their declared total-size fields.
pub fn count_inner_messages(records: &[u8]) -> Result<usize, CompactMessageError> {
    let mut count = 0usize;
    let mut offset = 0usize;
    while offset < records.len() {
        let available = records.len() - offset;
        let size_bytes = records.get(offset..offset.saturating_add(4)).ok_or(
            CompactMessageError::Truncated {
                field: "inner message size",
            },
        )?;
        let size = i32::from_be_bytes(size_bytes.try_into().expect("four bytes were selected"));
        if size < 4 {
            return Err(CompactMessageError::InvalidInnerSize { size });
        }
        let size = usize::try_from(size)
            .map_err(|_| CompactMessageError::InvalidInnerSize { size: i32::MIN })?;
        if size > available {
            return Err(CompactMessageError::IncompleteInnerMessage {
                offset,
                size,
                available,
            });
        }
        offset = offset
            .checked_add(size)
            .ok_or(CompactMessageError::SizeOverflow)?;
        count = count
            .checked_add(1)
            .ok_or(CompactMessageError::SizeOverflow)?;
    }
    Ok(count)
}

/// Expand a compact body as Java `MessageDecoder.decodeMessage(MessageExt, list)` does.
pub fn decode_inner_batch(outer: &MessageExt) -> Result<Vec<MessageExt>, CompactMessageError> {
    let body = outer
        .message()
        .body_nullable()
        .ok_or(CompactMessageError::MissingBody)?;
    decode_compact_messages(body).map(|messages| {
        messages
            .into_iter()
            .enumerate()
            .map(|(index, message)| {
                let mut message = message;
                message.set_topic_nullable(outer.message().topic_nullable().map(str::to_owned));
                let _ = message.set_wait_store_msg_ok(outer.message().is_wait_store_msg_ok());
                let mut inner = MessageExt::new(message);
                inner.set_queue_id(outer.queue_id());
                inner.set_queue_offset(outer.queue_offset().wrapping_add(index as i64));
                inner.set_sys_flag(outer.sys_flag());
                inner.set_born_timestamp(outer.born_timestamp());
                inner.set_born_host(outer.born_host().cloned());
                inner.set_store_timestamp(outer.store_timestamp());
                inner.set_store_host(outer.store_host().cloned());
                inner.set_commit_log_offset(outer.commit_log_offset());
                inner
            })
            .collect()
    })
}

struct CompactCursor<'a> {
    record: &'a [u8],
    position: usize,
}

impl<'a> CompactCursor<'a> {
    fn new(record: &'a [u8]) -> Self {
        Self {
            record,
            position: 0,
        }
    }

    fn position(&self) -> usize {
        self.position
    }

    fn bytes(
        &mut self,
        length: usize,
        field: &'static str,
    ) -> Result<&'a [u8], CompactMessageError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(CompactMessageError::SizeOverflow)?;
        let bytes = self
            .record
            .get(self.position..end)
            .ok_or(CompactMessageError::IncompleteField { field, length })?;
        self.position = end;
        Ok(bytes)
    }

    fn i16(&mut self, field: &'static str) -> Result<i16, CompactMessageError> {
        let bytes = self.bytes(2, field)?;
        Ok(i16::from_be_bytes(
            bytes.try_into().expect("two bytes were selected"),
        ))
    }

    fn i32(&mut self, field: &'static str) -> Result<i32, CompactMessageError> {
        let bytes = self.bytes(4, field)?;
        Ok(i32::from_be_bytes(
            bytes.try_into().expect("four bytes were selected"),
        ))
    }
}
