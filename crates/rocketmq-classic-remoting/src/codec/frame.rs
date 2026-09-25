use bytes::{Buf, BytesMut};
use rocketmq_classic_protocol::codes::SerializeType;

use crate::{
    codec::{json_header, rocketmq_header},
    command::RemotingCommand,
    error::CodecError,
};

/// Default Java Netty frame limit; it includes the 4-byte length prefix.
pub const DEFAULT_MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

// ponytail: Java's FastCodesHeader optimization is covered by equivalent golden frames;
// keep the common encoder exact until Phase 18 measurements justify specialization.
pub fn encode(command: &RemotingCommand, max_frame_size: usize) -> Result<Vec<u8>, CodecError> {
    let max_header_length = max_frame_size.saturating_sub(8).min(0x00ff_ffff);
    let header = match command.serialization {
        SerializeType::Json => json_header::encode(command, max_header_length)?,
        SerializeType::RocketMq => rocketmq_header::encode(command, max_header_length)?,
        SerializeType::Unknown(code) => return Err(CodecError::UnsupportedSerialization(code)),
    };
    if header.len() > max_header_length || header.len() > 0x00ff_ffff {
        return Err(CodecError::FieldTooWide("header length"));
    }
    let body = command.body.as_deref().unwrap_or_default();
    let declared_length = 4usize
        .checked_add(header.len())
        .and_then(|length| length.checked_add(body.len()))
        .ok_or(CodecError::FieldTooWide("frame length"))?;
    let total_length = declared_length
        .checked_add(4)
        .ok_or(CodecError::FieldTooWide("frame length"))?;
    if total_length > max_frame_size {
        return Err(CodecError::FrameTooLarge {
            length: total_length,
            limit: max_frame_size,
        });
    }
    let declared =
        u32::try_from(declared_length).map_err(|_| CodecError::FieldTooWide("frame length"))?;
    let marked_header = (u32::from(command.serialization.code()) << 24) | header.len() as u32;
    let mut frame = Vec::with_capacity(total_length);
    frame.extend_from_slice(&declared.to_be_bytes());
    frame.extend_from_slice(&marked_header.to_be_bytes());
    frame.extend_from_slice(&header);
    frame.extend_from_slice(body);
    Ok(frame)
}

pub fn decode_next(
    input: &mut BytesMut,
    max_frame_size: usize,
) -> Result<Option<RemotingCommand>, CodecError> {
    if input.len() < 4 {
        return Ok(None);
    }
    let declared = u32::from_be_bytes(input[..4].try_into().unwrap());
    if declared > i32::MAX as u32 {
        return Err(CodecError::InvalidFrameLength(declared as i32));
    }
    let declared_length = declared as usize;
    if declared_length < 4 {
        return Err(CodecError::InvalidFrameLength(declared as i32));
    }
    let total_length = declared_length
        .checked_add(4)
        .ok_or(CodecError::InvalidFrameLength(declared as i32))?;
    if total_length > max_frame_size {
        return Err(CodecError::FrameTooLarge {
            length: total_length,
            limit: max_frame_size,
        });
    }
    if input.len() < 8 {
        return Ok(None);
    }
    let marked_header = u32::from_be_bytes(input[4..8].try_into().unwrap());
    let serialization = SerializeType::from_code((marked_header >> 24) as u8);
    if let SerializeType::Unknown(code) = serialization {
        return Err(CodecError::UnsupportedSerialization(code));
    }
    let header_length = (marked_header & 0x00ff_ffff) as usize;
    let payload_length = declared_length - 4;
    if header_length > payload_length {
        return Err(CodecError::HeaderTooLarge {
            header: header_length,
            payload: payload_length,
        });
    }
    if input.len() < total_length {
        return Ok(None);
    }
    let header_end = 8 + header_length;
    let decoded = match serialization {
        SerializeType::Json => json_header::decode(&input[8..header_end])?,
        SerializeType::RocketMq => rocketmq_header::decode(&input[8..header_end])?,
        SerializeType::Unknown(_) => unreachable!(),
    };
    let mut command = decoded;
    command.serialization = serialization;
    let body = &input[header_end..total_length];
    command.body = (!body.is_empty()).then(|| body.to_vec());
    input.advance(total_length);
    Ok(Some(command))
}
