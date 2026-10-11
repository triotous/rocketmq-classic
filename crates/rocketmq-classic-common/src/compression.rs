use std::io::{self, Read, Write};

use crate::sysflag::message::CompressionType;

/// Compress a message body using the RocketMQ wire format selected by sysFlag.
/// The LZ4 backend emits the framed format used by RocketMQ 5.5.1.
pub fn compress(compression_type: CompressionType, body: &[u8], level: i32) -> io::Result<Vec<u8>> {
    match compression_type {
        CompressionType::Zlib => {
            let level = match level {
                -1 => flate2::Compression::default(),
                0..=9 => flate2::Compression::new(level as u32),
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "zlib compression level must be -1 or between 0 and 9",
                    ));
                }
            };
            let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), level);
            encoder.write_all(body)?;
            encoder.finish()
        }
        CompressionType::Lz4 => compress_lz4_frame(body),
        CompressionType::Zstd => zstd::stream::encode_all(body, level),
    }
}

/// Decompress the RocketMQ message body according to its compression type.
pub fn decompress(compression_type: CompressionType, body: &[u8]) -> io::Result<Vec<u8>> {
    match compression_type {
        CompressionType::Zlib => {
            let mut decoder = flate2::read::ZlibDecoder::new(body);
            let mut result = Vec::new();
            decoder.read_to_end(&mut result)?;
            Ok(result)
        }
        CompressionType::Lz4 => decompress_lz4_frame(body),
        CompressionType::Zstd => zstd::stream::decode_all(body),
    }
}

const LZ4_FRAME_MAGIC: u32 = 0x184d_2204;
const LZ4_MAX_BLOCK: usize = 4 * 1024 * 1024;

fn compress_lz4_frame(body: &[u8]) -> io::Result<Vec<u8>> {
    let descriptor = [0x60, 0x70]; // v1, independent blocks, 4 MiB maximum block.
    let header_checksum = (xxhash_rust::xxh32::xxh32(&descriptor, 0) >> 8) as u8;
    let mut result = Vec::with_capacity(body.len() + 16);
    result.extend_from_slice(&LZ4_FRAME_MAGIC.to_le_bytes());
    result.extend_from_slice(&descriptor);
    result.push(header_checksum);

    for block in body.chunks(LZ4_MAX_BLOCK) {
        let compressed = lz4_flex::block::compress(block);
        let (encoded, uncompressed) = if compressed.len() >= block.len() {
            (block, true)
        } else {
            (compressed.as_slice(), false)
        };
        let size = u32::try_from(encoded.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "LZ4 block exceeds frame length",
            )
        })?;
        let tagged_size = if uncompressed { size | (1 << 31) } else { size };
        result.extend_from_slice(&tagged_size.to_le_bytes());
        result.extend_from_slice(encoded);
    }
    result.extend_from_slice(&0_u32.to_le_bytes());
    Ok(result)
}

fn decompress_lz4_frame(frame: &[u8]) -> io::Result<Vec<u8>> {
    fn invalid_frame() -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, "invalid LZ4 frame")
    }
    fn take<'a>(frame: &'a [u8], cursor: &mut usize, count: usize) -> io::Result<&'a [u8]> {
        let end = cursor.checked_add(count).ok_or_else(invalid_frame)?;
        let bytes = frame.get(*cursor..end).ok_or_else(invalid_frame)?;
        *cursor = end;
        Ok(bytes)
    }

    let mut cursor = 0;
    let magic = u32::from_le_bytes(
        take(frame, &mut cursor, 4)?
            .try_into()
            .map_err(|_| invalid_frame())?,
    );
    if magic != LZ4_FRAME_MAGIC {
        return Err(invalid_frame());
    }
    let flg = take(frame, &mut cursor, 1)?[0];
    let bd = take(frame, &mut cursor, 1)?[0];
    if flg >> 6 != 1 || flg & 0x20 == 0 || flg & 0x02 != 0 || bd & 0x8f != 0 {
        return Err(invalid_frame());
    }
    let max_block = match (bd >> 4) & 7 {
        4 => 64 * 1024,
        5 => 256 * 1024,
        6 => 1024 * 1024,
        7 => LZ4_MAX_BLOCK,
        _ => return Err(invalid_frame()),
    };
    let mut descriptor = vec![flg, bd];
    let expected_content_size = if flg & 0x08 != 0 {
        let bytes = take(frame, &mut cursor, 8)?;
        descriptor.extend_from_slice(bytes);
        Some(u64::from_le_bytes(
            bytes.try_into().map_err(|_| invalid_frame())?,
        ))
    } else {
        None
    };
    if flg & 0x01 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "LZ4 dictionaries are unsupported",
        ));
    }
    let header_checksum = take(frame, &mut cursor, 1)?[0];
    if header_checksum != (xxhash_rust::xxh32::xxh32(&descriptor, 0) >> 8) as u8 {
        return Err(invalid_frame());
    }

    let block_checksum = flg & 0x10 != 0;
    let content_checksum = flg & 0x04 != 0;
    let mut result = Vec::new();
    loop {
        let tagged = u32::from_le_bytes(
            take(frame, &mut cursor, 4)?
                .try_into()
                .map_err(|_| invalid_frame())?,
        );
        if tagged == 0 {
            break;
        }
        let raw = tagged & (1 << 31) != 0;
        let size = (tagged & !(1 << 31)) as usize;
        if size > max_block {
            return Err(invalid_frame());
        }
        let encoded = take(frame, &mut cursor, size)?;
        let old_len = result.len();
        if raw {
            result.len().checked_add(size).ok_or_else(invalid_frame)?;
            result.extend_from_slice(encoded);
        } else {
            let mut decoded = vec![0; max_block];
            let written = lz4_flex::block::decompress_into(encoded, &mut decoded)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            result
                .len()
                .checked_add(written)
                .ok_or_else(invalid_frame)?;
            result.extend_from_slice(&decoded[..written]);
        }
        if block_checksum {
            let checksum = u32::from_le_bytes(
                take(frame, &mut cursor, 4)?
                    .try_into()
                    .map_err(|_| invalid_frame())?,
            );
            if checksum != xxhash_rust::xxh32::xxh32(encoded, 0) {
                return Err(invalid_frame());
            }
        }
        if result.len() < old_len {
            return Err(invalid_frame());
        }
    }
    if content_checksum {
        let checksum = u32::from_le_bytes(
            take(frame, &mut cursor, 4)?
                .try_into()
                .map_err(|_| invalid_frame())?,
        );
        if checksum != xxhash_rust::xxh32::xxh32(&result, 0) {
            return Err(invalid_frame());
        }
    }
    if cursor != frame.len()
        || expected_content_size.is_some_and(|size| size != result.len() as u64)
    {
        return Err(invalid_frame());
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_java_compression_type_round_trips_message_bytes() {
        let body = vec![b'R'; 8_192];
        for kind in CompressionType::values() {
            let compressed = compress(kind, &body, 5).unwrap();
            assert_eq!(decompress(kind, &compressed).unwrap(), body, "{kind}");
        }
    }

    #[test]
    fn zlib_preserves_java_level_validation() {
        assert_eq!(
            compress(CompressionType::Zlib, b"body", 10)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert!(compress(CompressionType::Zlib, b"body", -1).is_ok());
    }
}
