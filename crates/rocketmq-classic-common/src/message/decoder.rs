use std::io;

use thiserror::Error;

use super::{MessageExt, decode_properties};
use crate::{compression::decompress_limited, sysflag::message};

pub const MESSAGE_MAGIC_CODE_V1: i32 = -626_843_481;
pub const MESSAGE_MAGIC_CODE_V2: i32 = -626_843_477;
pub const BLANK_MAGIC_CODE: i32 = -875_286_124;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeOptions {
    pub read_body: bool,
    pub decompress_body: bool,
    pub set_properties_string: bool,
    pub verify_crc: bool,
}

impl Default for DecodeOptions {
    fn default() -> Self {
        Self {
            read_body: true,
            decompress_body: true,
            set_properties_string: false,
            verify_crc: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeLimits {
    pub max_record_bytes: usize,
    pub max_decompressed_body_bytes: usize,
    pub max_total_decoded_bytes: usize,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_record_bytes: 16 * 1024 * 1024,
            max_decompressed_body_bytes: 64 * 1024 * 1024,
            max_total_decoded_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DecodeError {
    #[error("message record at byte {offset} is truncated while reading {field}")]
    Truncated { offset: usize, field: &'static str },
    #[error("message record at byte {offset} has invalid store size {size}")]
    InvalidStoreSize { offset: usize, size: i32 },
    #[error("message record at byte {offset} has unknown magic code {magic_code}")]
    UnknownMagic { offset: usize, magic_code: i32 },
    #[error("message record at byte {offset} has negative {field} length {length}")]
    NegativeLength {
        offset: usize,
        field: &'static str,
        length: i32,
    },
    #[error("message record {field} length {length} exceeds limit {limit}")]
    LimitExceeded {
        field: &'static str,
        length: usize,
        limit: usize,
    },
    #[error("message record declares {record_size} bytes but only {available} bytes are available")]
    IncompleteRecord {
        record_size: usize,
        available: usize,
    },
    #[error("message record has {remaining} trailing bytes after its properties")]
    TrailingBytes { remaining: usize },
    #[error("body CRC does not match the record CRC field")]
    CrcMismatch,
    #[error("message record uses an unsupported compression type: {0}")]
    CompressionType(String),
    #[error("message decompression failed: {0}")]
    Decompression(String),
    #[error("aggregate decoded data exceeds limit {limit} bytes")]
    AggregateLimitExceeded { limit: usize },
}

/// Decode one complete Broker message record at the start of `record`.
/// Returns the decoded message and number of consumed bytes.
pub fn decode_one(
    record: &[u8],
    options: DecodeOptions,
    limits: DecodeLimits,
) -> Result<(MessageExt, usize), DecodeError> {
    let mut input = Cursor::new(record);
    let start = input.position();
    let store_size = input.i32("store size")?;
    if store_size < 8 {
        return Err(DecodeError::InvalidStoreSize {
            offset: start,
            size: store_size,
        });
    }
    let record_size = store_size as usize;
    if record_size > limits.max_record_bytes {
        return Err(DecodeError::LimitExceeded {
            field: "record",
            length: record_size,
            limit: limits.max_record_bytes,
        });
    }
    if record.len() < record_size {
        return Err(DecodeError::IncompleteRecord {
            record_size,
            available: record.len(),
        });
    }
    let mut input = Cursor::new(&record[..record_size]);
    let _store_size = input.i32("store size")?;
    let magic_offset = input.position();
    let magic_code = input.i32("magic code")?;
    let topic_width = match magic_code {
        MESSAGE_MAGIC_CODE_V1 => 1,
        MESSAGE_MAGIC_CODE_V2 => 2,
        other => {
            return Err(DecodeError::UnknownMagic {
                offset: magic_offset,
                magic_code: other,
            });
        }
    };

    let body_crc = input.i32("body CRC")?;
    let queue_id = input.i32("queue ID")?;
    let flag = input.i32("flag")?;
    let queue_offset = input.i64("queue offset")?;
    let commit_log_offset = input.i64("physical offset")?;
    let mut sys_flag = input.i32("system flag")?;
    let born_timestamp = input.i64("born timestamp")?;
    let born_host = input.host("born host", sys_flag & message::BORNHOST_V6_FLAG != 0)?;
    let store_timestamp = input.i64("store timestamp")?;
    let store_host = input.host(
        "store host",
        sys_flag & message::STOREHOSTADDRESS_V6_FLAG != 0,
    )?;
    let reconsume_times = input.i32("reconsume count")?;
    let prepared_transaction_offset = input.i64("prepared transaction offset")?;

    let body_length_offset = input.position();
    let body_length = input.i32("body length")?;
    let body_length = nonnegative_length(body_length_offset, "body", body_length)?;
    let encoded_body = input.bytes(body_length, "body")?;
    let mut body = if options.read_body {
        encoded_body.to_vec()
    } else {
        Vec::new()
    };
    if options.verify_crc
        && options.read_body
        && body_length > 0
        && crc32(&body) & 0x7fff_ffff != body_crc as u32
    {
        return Err(DecodeError::CrcMismatch);
    }
    if options.read_body
        && options.decompress_body
        && body_length > 0
        && sys_flag & message::COMPRESSED_FLAG != 0
    {
        let compression_type = message::compression_type(sys_flag)
            .map_err(|error| DecodeError::CompressionType(error.to_string()))?;
        body = decompress_limited(compression_type, &body, limits.max_decompressed_body_bytes)
            .map_err(|error| {
                if error.kind() == io::ErrorKind::InvalidData
                    && error.to_string().contains("exceeds configured limit")
                {
                    DecodeError::LimitExceeded {
                        field: "decompressed body",
                        length: limits.max_decompressed_body_bytes.saturating_add(1),
                        limit: limits.max_decompressed_body_bytes,
                    }
                } else {
                    DecodeError::Decompression(error.to_string())
                }
            })?;
        sys_flag &= !message::COMPRESSED_FLAG;
    }

    let topic_length_offset = input.position();
    let topic_length = if topic_width == 1 {
        input.i8("topic length")? as i32
    } else {
        input.i16("topic length")? as i32
    };
    let topic_length = nonnegative_length(topic_length_offset, "topic", topic_length)?;
    let topic = String::from_utf8_lossy(input.bytes(topic_length, "topic")?).into_owned();

    let properties_length_offset = input.position();
    let properties_length = input.i16("properties length")? as i32;
    let properties_length =
        nonnegative_length(properties_length_offset, "properties", properties_length)?;
    let properties = String::from_utf8_lossy(input.bytes(properties_length, "properties")?);
    if input.remaining() != 0 {
        return Err(DecodeError::TrailingBytes {
            remaining: input.remaining(),
        });
    }

    let mut decoded_properties = decode_properties(&properties);
    if options.set_properties_string && properties_length > 0 {
        decoded_properties.insert("propertiesString".to_owned(), properties.into_owned());
    }
    let mut message = MessageExt::new(super::Message::from_decoded(
        topic,
        body,
        decoded_properties,
    ));
    message.message_mut().set_flag(flag);
    message.set_queue_id(queue_id);
    message.set_queue_offset(queue_offset);
    message.set_commit_log_offset(commit_log_offset);
    message.set_sys_flag(sys_flag);
    message.set_born_timestamp(born_timestamp);
    message.set_store_timestamp(store_timestamp);
    message.set_reconsume_times(reconsume_times);
    message.set_prepared_transaction_offset(prepared_transaction_offset);
    message.set_store_size(store_size);
    message.set_body_crc(body_crc);
    message.set_born_host(Some(born_host));
    message.set_store_host(Some(store_host.clone()));
    message.set_message_id(Some(create_message_id(&input, commit_log_offset)));
    Ok((message, record_size))
}

/// Decode concatenated Broker records, requiring each iteration to progress.
pub fn decode_many(
    records: &[u8],
    options: DecodeOptions,
    limits: DecodeLimits,
) -> Result<Vec<MessageExt>, DecodeError> {
    let mut decoded = Vec::new();
    let mut offset = 0usize;
    let mut decoded_bytes = 0usize;
    while offset < records.len() {
        let (message, consumed) = decode_one(&records[offset..], options, limits)?;
        if consumed == 0 {
            return Err(DecodeError::InvalidStoreSize { offset, size: 0 });
        }
        let property_bytes = message
            .message()
            .properties()
            .iter()
            .try_fold(0_usize, |size, (key, value)| {
                size.checked_add(key.len())
                    .and_then(|size| size.checked_add(value.len()))
                    .and_then(|size| size.checked_add(2 * std::mem::size_of::<String>()))
                    .and_then(|size| size.checked_add(3 * std::mem::size_of::<usize>()))
            })
            .ok_or(DecodeError::AggregateLimitExceeded {
                limit: limits.max_total_decoded_bytes,
            })?;
        decoded_bytes = decoded_bytes
            .checked_add(std::mem::size_of::<MessageExt>())
            .and_then(|size| size.checked_add(message.message().body().len()))
            .and_then(|size| size.checked_add(message.message().topic().len()))
            .and_then(|size| size.checked_add(property_bytes))
            .ok_or(DecodeError::AggregateLimitExceeded {
                limit: limits.max_total_decoded_bytes,
            })?;
        if decoded_bytes > limits.max_total_decoded_bytes {
            return Err(DecodeError::AggregateLimitExceeded {
                limit: limits.max_total_decoded_bytes,
            });
        }
        offset = offset
            .checked_add(consumed)
            .ok_or(DecodeError::AggregateLimitExceeded {
                limit: limits.max_total_decoded_bytes,
            })?;
        decoded.push(message);
    }
    Ok(decoded)
}

fn nonnegative_length(
    offset: usize,
    field: &'static str,
    length: i32,
) -> Result<usize, DecodeError> {
    usize::try_from(length).map_err(|_| DecodeError::NegativeLength {
        offset,
        field,
        length,
    })
}

fn create_message_id(input: &Cursor<'_>, offset: i64) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let host_start = input.store_host_start;
    let host_length = input.store_host_length;
    let mut bytes = Vec::with_capacity(host_length + 8);
    bytes.extend_from_slice(&input.record[host_start..host_start + host_length]);
    bytes.extend_from_slice(&offset.to_be_bytes());
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[usize::from(byte >> 4)] as char);
        encoded.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    encoded
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0_u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & 0_u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

struct Cursor<'a> {
    record: &'a [u8],
    position: usize,
    store_host_start: usize,
    store_host_length: usize,
}

impl<'a> Cursor<'a> {
    fn new(record: &'a [u8]) -> Self {
        Self {
            record,
            position: 0,
            store_host_start: 0,
            store_host_length: 0,
        }
    }

    fn position(&self) -> usize {
        self.position
    }

    fn remaining(&self) -> usize {
        self.record.len().saturating_sub(self.position)
    }

    fn bytes(&mut self, count: usize, field: &'static str) -> Result<&'a [u8], DecodeError> {
        let offset = self.position;
        let end = self
            .position
            .checked_add(count)
            .ok_or(DecodeError::Truncated { offset, field })?;
        let bytes = self
            .record
            .get(self.position..end)
            .ok_or(DecodeError::Truncated { offset, field })?;
        self.position = end;
        Ok(bytes)
    }

    fn i8(&mut self, field: &'static str) -> Result<i8, DecodeError> {
        Ok(self.bytes(1, field)?[0] as i8)
    }

    fn i16(&mut self, field: &'static str) -> Result<i16, DecodeError> {
        Ok(i16::from_be_bytes(
            self.bytes(2, field)?
                .try_into()
                .map_err(|_| DecodeError::Truncated {
                    offset: self.position,
                    field,
                })?,
        ))
    }

    fn i32(&mut self, field: &'static str) -> Result<i32, DecodeError> {
        Ok(i32::from_be_bytes(
            self.bytes(4, field)?
                .try_into()
                .map_err(|_| DecodeError::Truncated {
                    offset: self.position,
                    field,
                })?,
        ))
    }

    fn i64(&mut self, field: &'static str) -> Result<i64, DecodeError> {
        Ok(i64::from_be_bytes(
            self.bytes(8, field)?
                .try_into()
                .map_err(|_| DecodeError::Truncated {
                    offset: self.position,
                    field,
                })?,
        ))
    }

    fn host(&mut self, field: &'static str, ipv6: bool) -> Result<String, DecodeError> {
        let ip_length = if ipv6 { 16 } else { 4 };
        let start = self.position;
        let ip_bytes = self.bytes(ip_length, field)?;
        let ip = if ipv6 {
            let address =
                std::net::Ipv6Addr::from(<[u8; 16]>::try_from(ip_bytes).map_err(|_| {
                    DecodeError::Truncated {
                        offset: start,
                        field,
                    }
                })?);
            address
                .segments()
                .iter()
                .map(|segment| format!("{segment:x}"))
                .collect::<Vec<_>>()
                .join(":")
        } else {
            std::net::Ipv4Addr::new(ip_bytes[0], ip_bytes[1], ip_bytes[2], ip_bytes[3]).to_string()
        };
        let port = self.i32(field)?;
        self.store_host_start = start;
        self.store_host_length = ip_length + 4;
        if ipv6 {
            Ok(format!("/[{ip}]:{port}"))
        } else {
            Ok(format!("/{ip}:{port}"))
        }
    }
}
