/// A Fastjson2 writer option accepted by inherited RocketMQ
/// `RemotingSerializable.encode(JSONWriter.Feature...)` adapters.
///
/// The variants and their order match the pinned Fastjson2 2.0.63 enum. The
/// adapters currently implement `WriteBooleanAsNumber`,
/// `WriteNonStringValueAsString`, `PrettyFormat`, `EscapeNoneAscii`,
/// `PrettyFormatWith2Space`, `PrettyFormatWith4Space`, `NotWriteEmptyArray`,
/// `IgnoreEmpty`, `NotWriteDefaultValue`, and `WriteNonStringKeyAsString`; other
/// variants return an explicit unsupported-feature error until their behavior
/// is mapped.
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
}
