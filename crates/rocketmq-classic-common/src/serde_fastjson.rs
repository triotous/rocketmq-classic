//! Narrow JSON coercions observed while decoding RocketMQ DTOs with Java
//! Fastjson 2.0.63. These helpers preserve the Rust DTO types while matching
//! the pinned Java input conversions for scalar JSON values.

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fmt;

use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de, de::DeserializeOwned};
use serde_json::Value;

pub type NullableStringListTable = BTreeMap<String, Option<Vec<Option<String>>>>;
pub type NullableStringMap = BTreeMap<String, Option<String>>;

enum FastjsonStringValue {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
    Array(Vec<Self>),
    Object(Vec<(String, Self)>),
}

impl FastjsonStringValue {
    fn java_string(self) -> Result<String, serde_json::Error> {
        match self {
            Self::Null => Ok("null".to_owned()),
            Self::Bool(value) => Ok(value.to_string()),
            Self::Number(value) => Ok(value.to_string()),
            Self::String(value) => Ok(value),
            Self::Array(_) | Self::Object(_) => serde_json::to_string(&self),
        }
    }
}

impl<'de> Deserialize<'de> for FastjsonStringValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FastjsonStringValueVisitor;

        impl<'de> Visitor<'de> for FastjsonStringValueVisitor {
            type Value = FastjsonStringValue;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("any JSON value")
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(FastjsonStringValue::Null)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(FastjsonStringValue::Null)
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(FastjsonStringValue::Bool(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(FastjsonStringValue::Number(value.into()))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(FastjsonStringValue::Number(value.into()))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                serde_json::Number::from_f64(value)
                    .map(FastjsonStringValue::Number)
                    .ok_or_else(|| E::custom("non-finite number is not valid JSON"))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(FastjsonStringValue::String(value.to_owned()))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(FastjsonStringValue::String(value))
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element()? {
                    values.push(value);
                }
                Ok(FastjsonStringValue::Array(values))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut values = Vec::<(String, FastjsonStringValue)>::new();
                while let Some((key, value)) = map.next_entry()? {
                    if let Some((_, current)) = values.iter_mut().find(|(name, _)| *name == key) {
                        *current = value;
                    } else {
                        values.push((key, value));
                    }
                }
                Ok(FastjsonStringValue::Object(values))
            }
        }

        deserializer.deserialize_any(FastjsonStringValueVisitor)
    }
}

impl Serialize for FastjsonStringValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Null => serializer.serialize_unit(),
            Self::Bool(value) => serializer.serialize_bool(*value),
            Self::Number(value) => value.serialize(serializer),
            Self::String(value) => serializer.serialize_str(value),
            Self::Array(values) => {
                let mut sequence = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    sequence.serialize_element(value)?;
                }
                sequence.end()
            }
            Self::Object(values) => {
                let mut map = serializer.serialize_map(Some(values.len()))?;
                for (key, value) in values {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
        }
    }
}

/// Normalizes Fastjson's invoked nonstandard document whitespace and numeric
/// tokens before Serde parsing. Quoted text and numeric object keys are kept.
pub fn normalize_json_input(input: &str) -> String {
    let input = input.strip_prefix('\u{feff}').unwrap_or(input);
    let whitespace_normalized = normalize_fastjson_whitespace(input);
    normalize_fastjson_number_tokens(&whitespace_normalized)
}

/// Parses a JSON document with the Fastjson syntax cases recorded by the Java
/// compatibility oracle, then applies this module's field coercions.
pub fn from_fastjson_str<T: DeserializeOwned>(input: &str) -> Result<T, serde_json::Error> {
    let normalized = normalize_json_input(input);
    serde_json::from_str(&normalized)
}

pub fn from_fastjson_slice<T: DeserializeOwned>(input: &[u8]) -> Result<T, serde_json::Error> {
    let input = String::from_utf8_lossy(input);
    from_fastjson_str(&input)
}

pub fn from_fastjson_str_nullable<T: DeserializeOwned>(
    input: Option<&str>,
) -> Result<Option<T>, serde_json::Error> {
    let Some(input) = input else {
        return Ok(None);
    };
    let normalized = normalize_json_input(input);
    serde_json::from_str(&normalized)
}

pub fn from_fastjson_slice_nullable<T: DeserializeOwned>(
    input: Option<&[u8]>,
) -> Result<Option<T>, serde_json::Error> {
    let Some(input) = input else {
        return Ok(None);
    };
    let input = String::from_utf8_lossy(input);
    from_fastjson_str_nullable(Some(&input))
}

pub fn from_fastjson_list_nullable<T: DeserializeOwned>(
    input: Option<&[u8]>,
) -> Result<Option<Vec<Option<T>>>, serde_json::Error> {
    let Some(input) = input else {
        return Ok(None);
    };
    let input = String::from_utf8_lossy(input);
    let normalized = normalize_json_input(&input);
    serde_json::from_str(&normalized)
}

fn normalize_fastjson_whitespace(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut in_string = false;
    let mut escaped = false;

    for character in input.chars() {
        if in_string {
            output.push(character);
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                in_string = false;
            }
        } else if character == '"' {
            in_string = true;
            output.push(character);
        } else if character == '\u{000c}' {
            output.push(' ');
        } else {
            output.push(character);
        }
    }
    output
}

fn normalize_fastjson_number_tokens(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut index = 0;
    let mut in_string = false;
    let mut escaped = false;

    while index < input.len() {
        let Some(character) = input[index..].chars().next() else {
            break;
        };
        if in_string {
            output.push(character);
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                in_string = false;
            }
            index += character.len_utf8();
            continue;
        }
        if character == '"' {
            in_string = true;
            output.push(character);
            index += character.len_utf8();
            continue;
        }

        if matches!(bytes[index], b'+' | b'-' | b'0'..=b'9') {
            let token_start = index;
            while bytes.get(index).is_some_and(|byte| {
                !byte.is_ascii_whitespace()
                    && !matches!(*byte, b',' | b']' | b'}' | b'{' | b'[' | b':')
            }) {
                index += 1;
            }
            let token = &input[token_start..index];
            let mut next = index;
            while bytes.get(next).is_some_and(u8::is_ascii_whitespace) {
                next += 1;
            }
            if bytes.get(next) != Some(&b':') {
                if let Some(normalized) = normalize_fastjson_number_token(token) {
                    output.push_str(&normalized);
                    continue;
                }
            }
            output.push_str(token);
            continue;
        }

        output.push(character);
        index += character.len_utf8();
    }
    output
}

fn normalize_fastjson_number_token(token: &str) -> Option<String> {
    let bytes = token.as_bytes();
    let mut index = 0;
    let negative = bytes.first() == Some(&b'-');
    let positive = bytes.first() == Some(&b'+');
    if negative || positive {
        index += 1;
    }
    let integer_start = index;
    while bytes.get(index).is_some_and(u8::is_ascii_digit) {
        index += 1;
    }
    if index == integer_start {
        return None;
    }
    let integer_end = index;

    if bytes.get(index) == Some(&b'.') {
        index += 1;
        let fraction_start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if index == fraction_start {
            return None;
        }
    }
    if matches!(bytes.get(index), Some(b'e' | b'E')) {
        index += 1;
        if matches!(bytes.get(index), Some(b'+' | b'-')) {
            index += 1;
        }
        let exponent_start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if index == exponent_start {
            return None;
        }
    }
    if index != bytes.len() {
        return None;
    }

    let leading_zeroes = integer_end - integer_start > 1 && bytes[integer_start] == b'0';
    if !positive && !leading_zeroes {
        return None;
    }

    let mut significant_start = integer_start;
    while significant_start + 1 < integer_end && bytes[significant_start] == b'0' {
        significant_start += 1;
    }
    let mut normalized = String::with_capacity(token.len());
    if negative {
        normalized.push('-');
    }
    normalized.push_str(&token[significant_start..integer_end]);
    normalized.push_str(&token[integer_end..]);
    Some(normalized)
}

pub fn deserialize_nullable_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = FastjsonStringValue::deserialize(deserializer)?;
    match value {
        FastjsonStringValue::Null => Ok(None),
        FastjsonStringValue::String(value) => Ok(Some(value)),
        value => serde_json::to_string(&value)
            .map(Some)
            .map_err(de::Error::custom),
    }
}

pub fn deserialize_i32<'de, D>(deserializer: D) -> Result<i32, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    fastjson_integer::<D::Error>(&value, i32::MIN as i128, i32::MAX as i128)
        .map(|value| value as i32)
}

pub fn deserialize_i64<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    fastjson_integer::<D::Error>(&value, i64::MIN as i128, i64::MAX as i128)
        .map(|value| value as i64)
}

pub fn deserialize_i64_reject_object<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    if value.is_object() {
        return Err(de::Error::custom(
            "Fastjson cannot coerce JSON object to a Java long",
        ));
    }
    fastjson_integer::<D::Error>(&value, i64::MIN as i128, i64::MAX as i128)
        .map(|value| value as i64)
}

pub fn deserialize_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::Null => Ok(false),
        Value::Bool(value) => Ok(value),
        Value::Number(value) => {
            let value = value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()));
            Ok(matches!(value, Some(-1 | 1)))
        }
        Value::String(value) if value.eq_ignore_ascii_case("true") || value == "1" => Ok(true),
        Value::String(value) if value.eq_ignore_ascii_case("false") || value == "0" => Ok(false),
        value => Err(de::Error::custom(format!(
            "Fastjson cannot coerce {value} to a Java boolean"
        ))),
    }
}

pub fn deserialize_optional_long_string_map<'de, D>(
    deserializer: D,
) -> Result<Option<BTreeMap<i64, Option<String>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    let Value::Object(entries) = value else {
        return if value.is_null() {
            Ok(None)
        } else {
            Err(de::Error::custom("expected a JSON object or null"))
        };
    };
    let mut result = BTreeMap::new();
    for (key, value) in entries {
        let key =
            fastjson_integer::<D::Error>(&Value::String(key), i64::MIN as i128, i64::MAX as i128)?
                as i64;
        let value = if value.is_null() {
            None
        } else {
            Some(java_string(&value))
        };
        result.insert(key, value);
    }
    Ok(Some(result))
}

pub fn deserialize_optional_int_map<'de, D>(
    deserializer: D,
) -> Result<Option<BTreeMap<i32, i32>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    let Value::Object(entries) = value else {
        return if value.is_null() {
            Ok(None)
        } else {
            Err(de::Error::custom("expected a JSON object or null"))
        };
    };
    let mut result = BTreeMap::new();
    for (key, value) in entries {
        let key =
            fastjson_integer::<D::Error>(&Value::String(key), i32::MIN as i128, i32::MAX as i128)?
                as i32;
        if value.is_null() {
            return Err(de::Error::custom(
                "Fastjson mapping ID value causes Java NullPointerException",
            ));
        }
        let value = fastjson_integer::<D::Error>(&value, i32::MIN as i128, i32::MAX as i128)?;
        result.insert(key, value as i32);
    }
    Ok(Some(result))
}

pub fn deserialize_optional_string_list_map<'de, D>(
    deserializer: D,
) -> Result<Option<NullableStringListTable>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    let Value::Object(entries) = value else {
        return if value.is_null() {
            Ok(None)
        } else {
            Err(de::Error::custom("expected a JSON object or null"))
        };
    };
    let mut result = BTreeMap::new();
    for (key, value) in entries {
        let value = match value {
            Value::Null => None,
            Value::Array(values) => Some(
                values
                    .iter()
                    .map(|value| (!value.is_null()).then(|| java_string(value)))
                    .collect(),
            ),
            _ => return Err(de::Error::custom("expected a JSON string array or null")),
        };
        result.insert(key, value);
    }
    Ok(Some(result))
}

pub fn deserialize_optional_nullable_string_list<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<Option<String>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::Null => Ok(None),
        Value::String(value) if value.is_empty() => Ok(Some(Vec::new())),
        Value::String(value) => Ok(Some(vec![Some(value)])),
        Value::Array(values) => Ok(Some(
            values
                .iter()
                .map(|value| (!value.is_null()).then(|| java_string(value)))
                .collect(),
        )),
        Value::Object(_) | Value::Number(_) | Value::Bool(_) => Ok(None),
    }
}

pub fn deserialize_optional_nullable_string_map<'de, D>(
    deserializer: D,
) -> Result<Option<NullableStringMap>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    let Value::Object(entries) = value else {
        return if value.is_null() {
            Ok(None)
        } else {
            Err(de::Error::custom("expected a JSON object or null"))
        };
    };
    Ok(Some(
        entries
            .iter()
            .map(|(key, value)| (key.clone(), (!value.is_null()).then(|| java_string(value))))
            .collect(),
    ))
}

pub fn deserialize_optional_nullable_string_set<'de, D>(
    deserializer: D,
) -> Result<Option<HashSet<Option<String>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = FastjsonStringValue::deserialize(deserializer)?;
    match value {
        FastjsonStringValue::Null => Ok(None),
        FastjsonStringValue::String(value) => Ok(Some(HashSet::from([Some(value)]))),
        value @ FastjsonStringValue::Object(_) => value
            .java_string()
            .map(|value| Some(HashSet::from([Some(value)])))
            .map_err(de::Error::custom),
        FastjsonStringValue::Array(values) => values
            .into_iter()
            .map(|value| match value {
                FastjsonStringValue::Null => Ok(None),
                value => value.java_string().map(Some).map_err(de::Error::custom),
            })
            .collect::<Result<HashSet<_>, _>>()
            .map(Some),
        _ => Err(de::Error::custom(
            "expected a JSON array, string, object, or null",
        )),
    }
}

pub fn deserialize_optional_nullable_int_set<'de, D>(
    deserializer: D,
) -> Result<Option<HashSet<Option<i32>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    let values = match value {
        Value::Null => return Ok(None),
        Value::Array(values) => values,
        scalar @ (Value::Number(_) | Value::String(_) | Value::Bool(_) | Value::Object(_)) => {
            vec![scalar]
        }
    };
    values
        .iter()
        .map(|value| {
            if value.is_null() {
                Ok(None)
            } else if value.is_object() {
                Ok(Some(0))
            } else {
                fastjson_integer::<D::Error>(value, i32::MIN as i128, i32::MAX as i128)
                    .map(|value| Some(value as i32))
            }
        })
        .collect::<Result<HashSet<_>, _>>()
        .map(Some)
}

fn java_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::String(value) => value.clone(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

fn fastjson_integer<E>(value: &Value, minimum: i128, maximum: i128) -> Result<i128, E>
where
    E: de::Error,
{
    match value {
        Value::Null => Ok(0),
        Value::Bool(value) => Ok(i128::from(*value)),
        Value::Number(value) => {
            if let Some(integer) = value.as_i64() {
                return checked_integer::<E>(integer as i128, minimum, maximum);
            }
            if let Some(integer) = value.as_u64() {
                return checked_integer::<E>(integer as i128, minimum, maximum);
            }
            let number = value
                .as_f64()
                .ok_or_else(|| E::custom("Fastjson numeric value is not representable"))?;
            if !number.is_finite() {
                return Err(E::custom("Fastjson numeric value is not finite"));
            }
            // Fastjson's primitive numeric conversion truncates decimal
            // values; narrowing a parsed decimal follows Java Number.intValue
            // style low-bit conversion at the tested i32 boundary.
            let truncated = number.trunc() as i128;
            if minimum == i32::MIN as i128 && maximum == i32::MAX as i128 {
                Ok((truncated as i32) as i128)
            } else {
                Ok((truncated as i64) as i128)
            }
        }
        Value::String(value) => {
            if let Ok(integer) = value.parse::<i128>() {
                return checked_integer::<E>(integer, minimum, maximum);
            }
            let number = value
                .parse::<f64>()
                .map_err(|_| E::custom("Fastjson cannot coerce string to integer"))?;
            if !number.is_finite() {
                return Err(E::custom("Fastjson numeric string is not finite"));
            }
            let truncated = number.trunc();
            if truncated < minimum as f64 || truncated > maximum as f64 {
                return Err(E::custom("Fastjson integer is out of range"));
            }
            Ok(truncated as i128)
        }
        Value::Object(entries) if entries.is_empty() => Ok(0),
        _ => Err(E::custom("Fastjson cannot coerce JSON value to integer")),
    }
}

fn checked_integer<E>(value: i128, minimum: i128, maximum: i128) -> Result<i128, E>
where
    E: de::Error,
{
    if (minimum..=maximum).contains(&value) {
        Ok(value)
    } else {
        Err(E::custom("Fastjson integer is out of range"))
    }
}
