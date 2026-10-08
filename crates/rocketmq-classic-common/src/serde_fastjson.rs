//! Narrow JSON coercions observed while decoding RocketMQ DTOs with Java
//! Fastjson 2.0.63. These helpers preserve the Rust DTO types while matching
//! the pinned Java input conversions for scalar JSON values.

use std::collections::BTreeMap;
use std::collections::HashSet;

use serde::{Deserialize, Deserializer, de};
use serde_json::Value;

pub type NullableStringListTable = BTreeMap<String, Option<Vec<Option<String>>>>;
pub type NullableStringMap = BTreeMap<String, Option<String>>;

pub fn deserialize_nullable_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::Null => Ok(None),
        Value::String(value) => Ok(Some(value)),
        value => Ok(Some(java_string(&value))),
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
    let value = Value::deserialize(deserializer)?;
    let Value::Array(values) = value else {
        return if value.is_null() {
            Ok(None)
        } else {
            Err(de::Error::custom("expected a JSON array or null"))
        };
    };
    Ok(Some(
        values
            .iter()
            .map(|value| (!value.is_null()).then(|| java_string(value)))
            .collect(),
    ))
}

pub fn deserialize_optional_nullable_int_set<'de, D>(
    deserializer: D,
) -> Result<Option<HashSet<Option<i32>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    let Value::Array(values) = value else {
        return if value.is_null() {
            Ok(None)
        } else {
            Err(de::Error::custom("expected a JSON array or null"))
        };
    };
    values
        .iter()
        .map(|value| {
            if value.is_null() {
                Ok(None)
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
