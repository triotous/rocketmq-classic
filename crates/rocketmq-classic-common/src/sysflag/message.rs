pub const COMPRESSED_FLAG: i32 = 0x1;
pub const MULTI_TAGS_FLAG: i32 = 1 << 1;
pub const TRANSACTION_NOT_TYPE: i32 = 0;
pub const TRANSACTION_PREPARED_TYPE: i32 = 1 << 2;
pub const TRANSACTION_COMMIT_TYPE: i32 = 2 << 2;
pub const TRANSACTION_ROLLBACK_TYPE: i32 = 3 << 2;
pub const BORNHOST_V6_FLAG: i32 = 1 << 4;
pub const STOREHOSTADDRESS_V6_FLAG: i32 = 1 << 5;
pub const NEED_UNWRAP_FLAG: i32 = 1 << 6;
pub const INNER_BATCH_FLAG: i32 = 1 << 7;
pub const COMPRESSION_LZ4_TYPE: i32 = 1 << 8;
pub const COMPRESSION_ZSTD_TYPE: i32 = 2 << 8;
pub const COMPRESSION_ZLIB_TYPE: i32 = 3 << 8;
pub const COMPRESSION_TYPE_COMPARATOR: i32 = 7 << 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    Legacy,
    Lz4,
    Zstd,
    Zlib,
    Unknown(i32),
}

pub fn has_compressed_flag(flag: i32) -> bool {
    flag & COMPRESSED_FLAG != 0
}
pub fn clear_compressed_flag(flag: i32) -> i32 {
    flag & !COMPRESSED_FLAG
}
pub fn check(flag: i32, expected_flag: i32) -> bool {
    flag & expected_flag != 0
}

pub fn transaction_value(flag: i32) -> i32 {
    flag & TRANSACTION_ROLLBACK_TYPE
}
pub fn reset_transaction_value(flag: i32, value: i32) -> i32 {
    (flag & !TRANSACTION_ROLLBACK_TYPE) | value
}
pub fn compression_type(flag: i32) -> CompressionType {
    match (flag & COMPRESSION_TYPE_COMPARATOR) >> 8 {
        0 => CompressionType::Legacy,
        1 => CompressionType::Lz4,
        2 => CompressionType::Zstd,
        3 => CompressionType::Zlib,
        other => CompressionType::Unknown(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compression_type_and_flag_helpers_preserve_other_bits() {
        assert!(has_compressed_flag(COMPRESSED_FLAG | 0x4000));
        assert_eq!(clear_compressed_flag(COMPRESSED_FLAG | 0x4000), 0x4000);
        assert_eq!(compression_type(0), CompressionType::Legacy);
        assert_eq!(
            compression_type(COMPRESSION_ZSTD_TYPE),
            CompressionType::Zstd
        );
        assert_eq!(compression_type(4 << 8), CompressionType::Unknown(4));
    }

    #[test]
    fn check_tests_any_expected_mask_bit() {
        assert!(check(0b1010, 0b0010));
        assert!(!check(0b1000, 0b0010));
    }

    #[test]
    fn transaction_mask_is_replaced_without_changing_reserved_bits() {
        let flag = TRANSACTION_PREPARED_TYPE | 0x4000;
        assert_eq!(transaction_value(flag), TRANSACTION_PREPARED_TYPE);
        assert_eq!(
            reset_transaction_value(flag, TRANSACTION_COMMIT_TYPE),
            TRANSACTION_COMMIT_TYPE | 0x4000
        );
    }
}
