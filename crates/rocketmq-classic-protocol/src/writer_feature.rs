/// A Fastjson2 writer option accepted by inherited RocketMQ
/// `RemotingSerializable.encode(JSONWriter.Feature...)` adapters.
///
/// The variants and their order match the pinned Fastjson2 2.0.63 enum. The
/// adapters currently implement `WriteBooleanAsNumber`,
/// `WriteNonStringValueAsString`, `PrettyFormat`, `EscapeNoneAscii`,
/// `PrettyFormatWith2Space`, `PrettyFormatWith4Space`, `NotWriteEmptyArray`,
/// `IgnoreEmpty`, `NotWriteDefaultValue`, `WriteNonStringKeyAsString`,
/// `WriteLongAsString`, `WriteEnumUsingOrdinal`, `WriteEnumsUsingName`,
/// `WriteEnumUsingToString`, `UseSingleQuotes`, `UnquoteFieldName`,
/// `IgnoreNoneSerializable`, `ErrorOnNoneSerializable`, `BrowserCompatible`,
/// `BrowserSecure`, `OptimizedForAscii`, `WriteNameAsSymbol`, `NotWriteRootClassName`,
/// `NotWriteHashMapArrayListClassName`, `NotWriteNumberClassName`, and
/// `NotWriteSetClassName`, `WriteByteArrayAsBase64`, `WritePairAsJavaBean`,
/// `WriteThrowableClassName`, `WriterUtilDateAsMillis`,
/// `WriteBigDecimalAsPlain`, and `WriteFloatSpecialAsString`.
/// `OptimizedForAscii` is output-preserving because the Rust writer already
/// emits UTF-8 JSON.
/// `WriteEnumsUsingName` and `WriteEnumUsingToString` preserve output for the
/// mapped DTO graph: the invoked enum values already serialize as their Java
/// names, and their `toString()` values match those names.
/// `WriteNameAsSymbol` is output-preserving for RocketMQ's UTF-8 text JSON
/// encoder; Fastjson2 uses symbols in its JSONB type-name path. The four
/// `NotWrite*ClassName` features preserve output when supplied alone: the
/// mapped API does not enable `WriteClassName`, and pinned Fastjson only uses
/// three of these options while deciding whether to write type metadata; it
/// has no writer callsite for `NotWriteSetClassName`. The remaining four
/// options preserve output for the mapped RocketMQ DTO graph: it contains no
/// byte-array field (BatchAck's BitSet uses RocketMQ's custom serializer),
/// Pair field, Throwable field, or `java.util.Date` field. Other variants
/// return an explicit unsupported-feature error until their behavior is
/// mapped. The DTO graph also has no `BigDecimal`, `Float`, or `Double` fields,
/// so their number-format options preserve output.
#[repr(usize)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JavaJsonWriterFeature {
    FieldBased,
    IgnoreNoneSerializable,
    ErrorOnNoneSerializable,
    BeanToArray,
    WriteNulls,
    WriteMapNullValue,
    BrowserCompatible,
    NullAsDefaultValue,
    WriteBooleanAsNumber,
    /// Serialize booleans and numbers as JSON strings.
    WriteNonStringValueAsString,
    WriteClassName,
    NotWriteRootClassName,
    NotWriteHashMapArrayListClassName,
    NotWriteDefaultValue,
    WriteEnumsUsingName,
    WriteEnumUsingToString,
    IgnoreErrorGetter,
    PrettyFormat,
    ReferenceDetection,
    WriteNameAsSymbol,
    WriteBigDecimalAsPlain,
    UseSingleQuotes,
    MapSortField,
    WriteNullListAsEmpty,
    WriteNullStringAsEmpty,
    WriteNullNumberAsZero,
    WriteNullBooleanAsFalse,
    NotWriteEmptyArray,
    IgnoreEmpty,
    WriteNonStringKeyAsString,
    WritePairAsJavaBean,
    OptimizedForAscii,
    EscapeNoneAscii,
    WriteByteArrayAsBase64,
    IgnoreNonFieldGetter,
    LargeObject,
    WriteLongAsString,
    BrowserSecure,
    WriteEnumUsingOrdinal,
    WriteThrowableClassName,
    UnquoteFieldName,
    NotWriteSetClassName,
    NotWriteNumberClassName,
    SortMapEntriesByKeys,
    PrettyFormatWith2Space,
    PrettyFormatWith4Space,
    WriterUtilDateAsMillis,
    WriteFloatSpecialAsString,
}

impl JavaJsonWriterFeature {
    /// Returns all 48 features in the pinned Java enum order.
    pub const fn values() -> [Self; 48] {
        [
            Self::FieldBased,
            Self::IgnoreNoneSerializable,
            Self::ErrorOnNoneSerializable,
            Self::BeanToArray,
            Self::WriteNulls,
            Self::WriteMapNullValue,
            Self::BrowserCompatible,
            Self::NullAsDefaultValue,
            Self::WriteBooleanAsNumber,
            Self::WriteNonStringValueAsString,
            Self::WriteClassName,
            Self::NotWriteRootClassName,
            Self::NotWriteHashMapArrayListClassName,
            Self::NotWriteDefaultValue,
            Self::WriteEnumsUsingName,
            Self::WriteEnumUsingToString,
            Self::IgnoreErrorGetter,
            Self::PrettyFormat,
            Self::ReferenceDetection,
            Self::WriteNameAsSymbol,
            Self::WriteBigDecimalAsPlain,
            Self::UseSingleQuotes,
            Self::MapSortField,
            Self::WriteNullListAsEmpty,
            Self::WriteNullStringAsEmpty,
            Self::WriteNullNumberAsZero,
            Self::WriteNullBooleanAsFalse,
            Self::NotWriteEmptyArray,
            Self::IgnoreEmpty,
            Self::WriteNonStringKeyAsString,
            Self::WritePairAsJavaBean,
            Self::OptimizedForAscii,
            Self::EscapeNoneAscii,
            Self::WriteByteArrayAsBase64,
            Self::IgnoreNonFieldGetter,
            Self::LargeObject,
            Self::WriteLongAsString,
            Self::BrowserSecure,
            Self::WriteEnumUsingOrdinal,
            Self::WriteThrowableClassName,
            Self::UnquoteFieldName,
            Self::NotWriteSetClassName,
            Self::NotWriteNumberClassName,
            Self::SortMapEntriesByKeys,
            Self::PrettyFormatWith2Space,
            Self::PrettyFormatWith4Space,
            Self::WriterUtilDateAsMillis,
            Self::WriteFloatSpecialAsString,
        ]
    }

    /// Returns the matching Fastjson2 enum constant name.
    pub const fn name(self) -> &'static str {
        const NAMES: [&str; 48] = [
            "FieldBased",
            "IgnoreNoneSerializable",
            "ErrorOnNoneSerializable",
            "BeanToArray",
            "WriteNulls",
            "WriteMapNullValue",
            "BrowserCompatible",
            "NullAsDefaultValue",
            "WriteBooleanAsNumber",
            "WriteNonStringValueAsString",
            "WriteClassName",
            "NotWriteRootClassName",
            "NotWriteHashMapArrayListClassName",
            "NotWriteDefaultValue",
            "WriteEnumsUsingName",
            "WriteEnumUsingToString",
            "IgnoreErrorGetter",
            "PrettyFormat",
            "ReferenceDetection",
            "WriteNameAsSymbol",
            "WriteBigDecimalAsPlain",
            "UseSingleQuotes",
            "MapSortField",
            "WriteNullListAsEmpty",
            "WriteNullStringAsEmpty",
            "WriteNullNumberAsZero",
            "WriteNullBooleanAsFalse",
            "NotWriteEmptyArray",
            "IgnoreEmpty",
            "WriteNonStringKeyAsString",
            "WritePairAsJavaBean",
            "OptimizedForAscii",
            "EscapeNoneAscii",
            "WriteByteArrayAsBase64",
            "IgnoreNonFieldGetter",
            "LargeObject",
            "WriteLongAsString",
            "BrowserSecure",
            "WriteEnumUsingOrdinal",
            "WriteThrowableClassName",
            "UnquoteFieldName",
            "NotWriteSetClassName",
            "NotWriteNumberClassName",
            "SortMapEntriesByKeys",
            "PrettyFormatWith2Space",
            "PrettyFormatWith4Space",
            "WriterUtilDateAsMillis",
            "WriteFloatSpecialAsString",
        ];
        NAMES[self as usize]
    }

    /// Returns the Java ordinal.
    pub const fn ordinal(self) -> i32 {
        self as i32
    }

    pub(crate) const fn is_implemented(self) -> bool {
        matches!(
            self,
            Self::PrettyFormat
                | Self::PrettyFormatWith2Space
                | Self::PrettyFormatWith4Space
                | Self::EscapeNoneAscii
                | Self::WriteBooleanAsNumber
                | Self::WriteNonStringValueAsString
                | Self::NotWriteEmptyArray
                | Self::IgnoreEmpty
                | Self::NotWriteDefaultValue
                | Self::WriteNonStringKeyAsString
                | Self::WriteLongAsString
                | Self::WriteEnumUsingOrdinal
                | Self::WriteEnumsUsingName
                | Self::WriteEnumUsingToString
                | Self::BrowserCompatible
                | Self::OptimizedForAscii
                | Self::WriteNameAsSymbol
                | Self::NotWriteRootClassName
                | Self::NotWriteHashMapArrayListClassName
                | Self::NotWriteNumberClassName
                | Self::NotWriteSetClassName
                | Self::WriteByteArrayAsBase64
                | Self::WritePairAsJavaBean
                | Self::WriteThrowableClassName
                | Self::WriterUtilDateAsMillis
                | Self::WriteBigDecimalAsPlain
                | Self::WriteFloatSpecialAsString
                | Self::BrowserSecure
                | Self::UseSingleQuotes
                | Self::UnquoteFieldName
                | Self::IgnoreNoneSerializable
                | Self::ErrorOnNoneSerializable
        )
    }
}

#[cfg(test)]
mod tests {
    use super::JavaJsonWriterFeature;

    #[test]
    fn feature_names_and_ordinals_match_pinned_java_enum_inventory() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let java_names = fixture["remotingSerializationWriterFeatures"]
            .as_array()
            .unwrap();
        let rust_features = JavaJsonWriterFeature::values();
        assert_eq!(java_names.len(), rust_features.len());
        for (ordinal, (java_name, feature)) in java_names.iter().zip(rust_features).enumerate() {
            assert_eq!(feature.name(), java_name.as_str().unwrap());
            assert_eq!(feature.ordinal(), ordinal as i32);
        }
    }

    #[test]
    fn single_quoted_strings_match_fastjson_escaping() {
        let mut output = String::new();
        crate::route::write_java_json_string_with_browser_secure(
            "a'b\"c\\d\n漢<>()",
            true,
            false,
            &mut output,
        )
        .unwrap();
        assert_eq!(output, r#"'a\'b"c\\d\n漢<>()'"#);
        assert_eq!(
            crate::route::escape_none_ascii_json(&output),
            r#"'a\'b"c\\d\n\u6F22<>()'"#
        );
    }

    #[test]
    fn browser_secure_strings_escape_html_sensitive_characters() {
        let mut output = String::new();
        crate::route::write_java_json_string_with_browser_secure(
            "a'b\"c\\d\n漢<>()",
            true,
            true,
            &mut output,
        )
        .unwrap();
        assert_eq!(output, r#"'a\'b"c\\d\n漢\u003C\u003E\u0028\u0029'"#);
    }

    #[test]
    fn enum_ordinal_writer_covers_protocol_enum_fields() {
        let mut value = serde_json::json!([
            {"properties": {"mode": "POP", "action": "PARTIAL_ADD"}},
            {"messageRequestModeMap": {"topic": {"group": {"mode": "POP"}}}},
            {"action": "PARTIAL_ADD"},
            {"action": "PARTIAL_REMOVE"},
            {"action": "COMPLETE_ADD"},
            {"action": "COMPLETE_REMOVE"},
            {"consumeFromWhere": "CONSUME_FROM_LAST_OFFSET"},
            {"consumeFromWhere": "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST"},
            {"consumeFromWhere": "CONSUME_FROM_MIN_OFFSET"},
            {"consumeFromWhere": "CONSUME_FROM_MAX_OFFSET"},
            {"consumeFromWhere": "CONSUME_FROM_FIRST_OFFSET"},
            {"consumeFromWhere": "CONSUME_FROM_TIMESTAMP"},
            {"consumeResult": "CR_SUCCESS"},
            {"consumeResult": "CR_LATER"},
            {"consumeResult": "CR_ROLLBACK"},
            {"consumeResult": "CR_COMMIT"},
            {"consumeResult": "CR_THROW_EXCEPTION"},
            {"consumeResult": "CR_RETURN_NULL"},
            {"consumeType": "CONSUME_ACTIVELY"},
            {"consumeType": "CONSUME_PASSIVELY"},
            {"consumeType": "CONSUME_POP"},
            {"messageModel": "BROADCASTING"},
            {"messageModel": "CLUSTERING"},
            {"messageModel": "LITE_SELECTIVE"},
            {"mode": "PULL"},
            {"mode": "POP"},
            {"optionType": "POLICY"},
            {"optionType": "OFFSET"},
            {"optionType": "TAIL_N"},
            {"optionType": "TIMESTAMP"},
        ]);
        crate::route::apply_java_json_writer_value_features(&mut value, false, false, true, false);
        let expected = serde_json::json!([
            {"properties": {"mode": "POP", "action": "PARTIAL_ADD"}},
            {"messageRequestModeMap": {"topic": {"group": {"mode": 1}}}},
            {"action": 0}, {"action": 1}, {"action": 2}, {"action": 3},
            {"consumeFromWhere": 0}, {"consumeFromWhere": 1},
            {"consumeFromWhere": 2}, {"consumeFromWhere": 3},
            {"consumeFromWhere": 4}, {"consumeFromWhere": 5},
            {"consumeResult": 0}, {"consumeResult": 1}, {"consumeResult": 2},
            {"consumeResult": 3}, {"consumeResult": 4}, {"consumeResult": 5},
            {"consumeType": 0}, {"consumeType": 1}, {"consumeType": 2},
            {"messageModel": 0}, {"messageModel": 1}, {"messageModel": 2},
            {"mode": 0}, {"mode": 1},
            {"optionType": 0}, {"optionType": 1}, {"optionType": 2}, {"optionType": 3},
        ]);
        assert_eq!(value, expected);
    }
}
