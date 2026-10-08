use std::collections::BTreeMap;

use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_common::serde_fastjson;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

fn java_route_json<T: Serialize>(
    value: &T,
    pretty_format: bool,
) -> Result<String, serde_json::Error> {
    let value = serde_json::to_value(value)?;
    let mut output = String::new();
    write_java_route_json(&value, None, 0, pretty_format, &mut output)?;
    Ok(output)
}

fn write_java_route_json(
    value: &serde_json::Value,
    field_name: Option<&str>,
    depth: usize,
    pretty_format: bool,
    output: &mut String,
) -> Result<(), serde_json::Error> {
    match value {
        serde_json::Value::Object(fields)
            if fields.is_empty() && (!pretty_format || field_name.is_some()) =>
        {
            output.push_str("{}")
        }
        serde_json::Value::Object(fields) if fields.is_empty() => {
            output.push_str("{\n");
            write_java_indent(output, depth + 1);
            output.push('\n');
            write_java_indent(output, depth);
            output.push('}');
        }
        serde_json::Value::Object(fields) => {
            let numeric_keys = matches!(field_name, Some("brokerAddrs" | "currIdMap"));
            output.push('{');
            for (index, (key, value)) in fields.iter().enumerate() {
                if pretty_format {
                    output.push('\n');
                    write_java_indent(output, depth + 1);
                }
                if numeric_keys && key.parse::<i64>().is_ok() {
                    output.push_str(key);
                } else {
                    output.push_str(&serde_json::to_string(key)?);
                }
                output.push(':');
                write_java_route_json(value, Some(key), depth + 1, pretty_format, output)?;
                if index + 1 != fields.len() {
                    output.push(',');
                }
            }
            if pretty_format {
                output.push('\n');
                write_java_indent(output, depth);
            }
            output.push('}');
        }
        serde_json::Value::Array(values) if values.is_empty() && !pretty_format => {
            output.push_str("[]")
        }
        serde_json::Value::Array(values) if values.is_empty() => {
            output.push_str("[\n");
            write_java_indent(output, depth + 1);
            output.push('\n');
            write_java_indent(output, depth);
            output.push(']');
        }
        serde_json::Value::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if pretty_format {
                    output.push('\n');
                    write_java_indent(output, depth + 1);
                }
                write_java_route_json(value, None, depth + 1, pretty_format, output)?;
                if index + 1 != values.len() {
                    output.push(',');
                }
            }
            if pretty_format {
                output.push('\n');
                write_java_indent(output, depth);
            }
            output.push(']');
        }
        _ => output.push_str(&serde_json::to_string(value)?),
    }
    Ok(())
}

fn write_java_indent(output: &mut String, depth: usize) {
    for _ in 0..depth {
        output.push('\t');
    }
}

fn quote_unquoted_integer_object_keys(input: &str) -> String {
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
        if character == '{' || character == ',' {
            output.push(character);
            index += character.len_utf8();
            let whitespace_start = index;
            while bytes
                .get(index)
                .is_some_and(|byte| byte.is_ascii_whitespace())
            {
                index += 1;
            }
            let key_start = index;
            if bytes.get(index) == Some(&b'-') {
                index += 1;
            }
            let digits_start = index;
            while bytes.get(index).is_some_and(u8::is_ascii_digit) {
                index += 1;
            }
            let key_end = index;
            while bytes
                .get(index)
                .is_some_and(|byte| byte.is_ascii_whitespace())
            {
                index += 1;
            }
            if digits_start != key_end && bytes.get(index) == Some(&b':') {
                output.push_str(&input[whitespace_start..key_start]);
                output.push('"');
                output.push_str(&input[key_start..key_end]);
                output.push('"');
                output.push_str(&input[key_end..index]);
                continue;
            }
            output.push_str(&input[whitespace_start..index]);
            continue;
        }
        output.push(character);
        index += character.len_utf8();
    }
    output
}

/// Fastjson accepts leading `+` and zero-prefixed numeric value tokens that
/// `serde_json` rejects; normalize only those tokens outside quoted strings.
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

fn parse_java_route_json<T: DeserializeOwned>(input: &str) -> Result<T, serde_json::Error> {
    let quoted_keys = quote_unquoted_integer_object_keys(input);
    let normalized = normalize_fastjson_number_tokens(&quoted_keys);
    serde_json::from_str(&normalized)
}

fn parse_java_route_json_nullable<T: DeserializeOwned>(
    input: Option<&str>,
) -> Result<Option<T>, serde_json::Error> {
    let Some(input) = input else {
        return Ok(None);
    };
    let quoted_keys = quote_unquoted_integer_object_keys(input);
    let normalized = normalize_fastjson_number_tokens(&quoted_keys);
    serde_json::from_str(&normalized)
}

fn decode_java_route_json_nullable<T: DeserializeOwned>(
    data: Option<&[u8]>,
) -> Result<Option<T>, serde_json::Error> {
    let Some(data) = data else {
        return Ok(None);
    };
    let input = String::from_utf8_lossy(data);
    parse_java_route_json_nullable(Some(&input))
}

fn decode_java_route_json_list_nullable<T: DeserializeOwned>(
    data: Option<&[u8]>,
) -> Result<Option<Vec<Option<T>>>, serde_json::Error> {
    let Some(data) = data else {
        return Ok(None);
    };
    let input = String::from_utf8_lossy(data);
    let quoted_keys = quote_unquoted_integer_object_keys(&input);
    let normalized = normalize_fastjson_number_tokens(&quoted_keys);
    serde_json::from_str(&normalized)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueueData {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub broker_name: Option<String>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_i32")]
    pub read_queue_nums: i32,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_i32")]
    pub write_queue_nums: i32,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_i32")]
    pub perm: i32,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_i32")]
    pub topic_sys_flag: i32,
}

/// Failure cases from the pinned Java `QueueData.compareTo` method.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum QueueDataCompareError {
    #[error("QueueData broker name is null")]
    NullBrokerName,
}

impl QueueData {
    /// Returns the pinned Java 5.5.1 `QueueData.hashCode` result.
    pub fn java_hash_code(&self) -> i32 {
        let mut result = 1i32;
        result = result.wrapping_mul(31).wrapping_add(
            self.broker_name
                .as_deref()
                .map(java_string_hash_code)
                .unwrap_or(0),
        );
        result = result.wrapping_mul(31).wrapping_add(self.perm);
        result = result.wrapping_mul(31).wrapping_add(self.read_queue_nums);
        result = result.wrapping_mul(31).wrapping_add(self.write_queue_nums);
        result.wrapping_mul(31).wrapping_add(self.topic_sys_flag)
    }

    /// Returns the pinned Java 5.5.1 `QueueData.compareTo` result.
    ///
    /// Java compares broker names as UTF-16 strings and throws
    /// `NullPointerException` when either name is null.
    pub fn java_compare_to(&self, other: &Self) -> Result<i32, QueueDataCompareError> {
        let name = self
            .broker_name
            .as_deref()
            .ok_or(QueueDataCompareError::NullBrokerName)?;
        let other_name = other
            .broker_name
            .as_deref()
            .ok_or(QueueDataCompareError::NullBrokerName)?;
        Ok(java_string_compare_to(name, other_name))
    }

    /// Returns the pinned Java 5.5.1 `QueueData.toString` text.
    pub fn java_to_string(&self) -> String {
        format!(
            "QueueData [brokerName={}, readQueueNums={}, writeQueueNums={}, perm={}, topicSysFlag={}]",
            self.broker_name.as_deref().unwrap_or("null"),
            self.read_queue_nums,
            self.write_queue_nums,
            self.perm,
            self.topic_sys_flag
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BrokerData {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub cluster: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub broker_name: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_optional_long_string_map",
        skip_serializing_if = "Option::is_none"
    )]
    pub broker_addrs: Option<BTreeMap<i64, Option<String>>>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub zone_name: Option<String>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_bool")]
    pub enable_acting_master: bool,
}

impl PartialEq for BrokerData {
    fn eq(&self, other: &Self) -> bool {
        self.broker_name == other.broker_name && self.broker_addrs == other.broker_addrs
    }
}

impl Eq for BrokerData {}

/// Failure cases from the pinned Java `BrokerData.compareTo` method.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BrokerDataCompareError {
    #[error("BrokerData broker name is null")]
    NullBrokerName,
}

/// Java exception branches from `BrokerData.selectBrokerAddr`.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BrokerDataSelectAddrError {
    #[error("BrokerData broker address map is null (Java NullPointerException)")]
    NullAddressMap,
    #[error("BrokerData has no address candidates (Java IllegalArgumentException)")]
    NoAddressCandidates,
}

impl BrokerData {
    /// Selects the master address when present; otherwise randomly selects one
    /// value, matching pinned Java 5.5.1 `BrokerData.selectBrokerAddr`.
    ///
    /// A present null master falls through to random selection. Null selected
    /// values are represented as `Ok(None)`; a null map and an empty map retain
    /// Java's `NullPointerException` and `IllegalArgumentException` outcomes.
    pub fn java_select_broker_addr(&self) -> Result<Option<String>, BrokerDataSelectAddrError> {
        self.java_select_broker_addr_with(|length| rand::random_range(0..length))
    }

    fn java_select_broker_addr_with(
        &self,
        mut select: impl FnMut(usize) -> usize,
    ) -> Result<Option<String>, BrokerDataSelectAddrError> {
        let addresses = self
            .broker_addrs
            .as_ref()
            .ok_or(BrokerDataSelectAddrError::NullAddressMap)?;
        if let Some(Some(master)) = addresses.get(&0) {
            return Ok(Some(master.clone()));
        }
        if addresses.is_empty() {
            return Err(BrokerDataSelectAddrError::NoAddressCandidates);
        }
        let index = select(addresses.len());
        Ok(addresses.values().nth(index).cloned().flatten())
    }

    /// Returns the pinned Java 5.5.1 `BrokerData.hashCode` result.
    ///
    /// Java hashes only the broker address map and broker name, using the
    /// Java `Map`, `Long`, and UTF-16 `String` hash rules.
    pub fn java_hash_code(&self) -> i32 {
        let addresses_hash = self
            .broker_addrs
            .as_ref()
            .map(|addresses| {
                addresses.iter().fold(0i32, |hash, (broker_id, address)| {
                    let address_hash = address.as_deref().map(java_string_hash_code).unwrap_or(0);
                    hash.wrapping_add(java_long_hash_code(*broker_id) ^ address_hash)
                })
            })
            .unwrap_or(0);
        let broker_name_hash = self
            .broker_name
            .as_deref()
            .map(java_string_hash_code)
            .unwrap_or(0);
        31i32
            .wrapping_mul(31i32.wrapping_add(addresses_hash))
            .wrapping_add(broker_name_hash)
    }

    /// Returns the pinned Java 5.5.1 `BrokerData.compareTo` result.
    ///
    /// Java compares broker names as UTF-16 strings and returns the raw code
    /// unit difference. Its implementation throws `NullPointerException` if
    /// either broker name is null; this method represents that as an error.
    pub fn java_compare_to(&self, other: &Self) -> Result<i32, BrokerDataCompareError> {
        let name = self
            .broker_name
            .as_deref()
            .ok_or(BrokerDataCompareError::NullBrokerName)?;
        let other_name = other
            .broker_name
            .as_deref()
            .ok_or(BrokerDataCompareError::NullBrokerName)?;
        Ok(java_string_compare_to(name, other_name))
    }

    /// Returns the pinned Java 5.5.1 `BrokerData.toString` text.
    ///
    /// Address entries use deterministic Rust key order. Java's `HashMap`
    /// iteration order is unspecified.
    pub fn java_to_string(&self) -> String {
        let broker_addrs = self.broker_addrs.as_ref().map_or_else(
            || "null".to_owned(),
            |addresses| {
                let entries = addresses
                    .iter()
                    .map(|(broker_id, address)| {
                        format!("{broker_id}={}", address.as_deref().unwrap_or("null"))
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{{{entries}}}")
            },
        );
        format!(
            "BrokerData [brokerName={}, brokerAddrs={}, enableActingMaster={}]",
            self.broker_name.as_deref().unwrap_or("null"),
            broker_addrs,
            self.enable_acting_master
        )
    }
}

fn java_long_hash_code(value: i64) -> i32 {
    let bits = value as u64;
    (bits ^ (bits >> 32)) as u32 as i32
}

fn java_string_hash_code(value: &str) -> i32 {
    value.encode_utf16().fold(0i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

fn java_list_hash_code<T>(values: &[T], mut value_hash: impl FnMut(&T) -> i32) -> i32 {
    values.iter().fold(1i32, |hash, value| {
        hash.wrapping_mul(31).wrapping_add(value_hash(value))
    })
}

fn java_string_compare_to(left: &str, right: &str) -> i32 {
    let left_units: Vec<u16> = left.encode_utf16().collect();
    let right_units: Vec<u16> = right.encode_utf16().collect();
    for (&left, &right) in left_units.iter().zip(&right_units) {
        if left != right {
            return i32::from(left) - i32::from(right);
        }
    }
    left_units.len() as i32 - right_units.len() as i32
}

#[cfg(test)]
mod queue_data_method_tests {
    use super::{QueueData, QueueDataCompareError};

    #[test]
    fn queue_data_methods_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["queueDataMethods"].as_array().unwrap() {
            let left: QueueData = serde_json::from_value(case["left"].clone()).unwrap();
            let right: QueueData = serde_json::from_value(case["right"].clone()).unwrap();
            assert_eq!(left == right, case["equals"]);
            assert_eq!(
                left.java_hash_code(),
                case["leftHashCode"].as_i64().unwrap() as i32
            );
            assert_eq!(
                right.java_hash_code(),
                case["rightHashCode"].as_i64().unwrap() as i32
            );
            assert_eq!(left.java_to_string(), case["leftToString"]);
            assert_eq!(right.java_to_string(), case["rightToString"]);
            let result = left.java_compare_to(&right);
            if case["outcome"] == "OK" {
                assert_eq!(result.unwrap(), case["result"].as_i64().unwrap() as i32);
            } else {
                assert_eq!(result.unwrap_err(), QueueDataCompareError::NullBrokerName);
            }
        }
    }
}

#[cfg(test)]
mod broker_data_equality_tests {
    use super::BrokerData;

    #[test]
    fn broker_data_equality_matches_invoked_java_equals() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["brokerDataEquality"].as_array().unwrap() {
            let left: BrokerData = serde_json::from_value(case["left"].clone()).unwrap();
            let right: BrokerData = serde_json::from_value(case["right"].clone()).unwrap();
            assert_eq!(left == right, case["equals"], "{}", case["id"]);
        }
    }
}

#[cfg(test)]
mod broker_data_hash_code_tests {
    use super::BrokerData;

    #[test]
    fn broker_data_hash_code_matches_invoked_java_hash_code() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["brokerDataHashCode"].as_array().unwrap() {
            let broker: BrokerData = serde_json::from_value(case["input"].clone()).unwrap();
            assert_eq!(
                broker.java_hash_code(),
                case["hashCode"].as_i64().unwrap() as i32,
                "{}",
                case["id"]
            );
        }
    }
}

#[cfg(test)]
mod broker_data_to_string_tests {
    use super::BrokerData;

    #[test]
    fn broker_data_to_string_matches_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["brokerDataToString"].as_array().unwrap() {
            let broker: BrokerData = serde_json::from_value(case["input"].clone()).unwrap();
            assert_eq!(broker.java_to_string(), case["toString"]);
        }
    }
}

#[cfg(test)]
mod broker_data_select_addr_tests {
    use super::{BrokerData, BrokerDataSelectAddrError};

    #[test]
    fn broker_data_select_addr_matches_invoked_java_outcomes() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["brokerDataSelectAddr"].as_array().unwrap() {
            let broker: BrokerData = serde_json::from_value(case["input"].clone()).unwrap();
            if let Some(exception) = case["exception"].as_str() {
                let error = broker.java_select_broker_addr().unwrap_err();
                let expected = match exception {
                    "NullPointerException" => BrokerDataSelectAddrError::NullAddressMap,
                    "IllegalArgumentException" => BrokerDataSelectAddrError::NoAddressCandidates,
                    other => panic!("unexpected Java exception {other}"),
                };
                assert_eq!(error, expected, "{}", case["id"]);
                continue;
            }
            match broker.java_select_broker_addr().unwrap() {
                Some(address) => assert!(
                    case["addresses"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|allowed| allowed.as_str() == Some(address.as_str())),
                    "unexpected random address for {}: {address}",
                    case["id"]
                ),
                None => assert_eq!(case["nullAddress"], true, "{}", case["id"]),
            }
            for index in 0..broker.broker_addrs.as_ref().unwrap().len() {
                match broker.java_select_broker_addr_with(|_| index).unwrap() {
                    Some(address) => assert!(
                        case["addresses"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|allowed| allowed.as_str() == Some(address.as_str())),
                        "unexpected address for {}: {address}",
                        case["id"]
                    ),
                    None => assert_eq!(case["nullAddress"], true, "{}", case["id"]),
                }
            }
        }
    }
}

#[cfg(test)]
mod broker_data_compare_to_tests {
    use super::{BrokerData, BrokerDataCompareError};

    #[test]
    fn broker_data_compare_to_matches_invoked_java_comparable() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["brokerDataCompareTo"].as_array().unwrap() {
            let left: BrokerData = serde_json::from_value(case["left"].clone()).unwrap();
            let right: BrokerData = serde_json::from_value(case["right"].clone()).unwrap();
            let result = left.java_compare_to(&right);
            if case["outcome"] == "OK" {
                assert_eq!(result.unwrap(), case["result"].as_i64().unwrap() as i32);
            } else {
                assert_eq!(result.unwrap_err(), BrokerDataCompareError::NullBrokerName);
            }
        }
    }
}

#[cfg(test)]
mod nullable_broker_address_tests {
    use super::BrokerData;

    #[test]
    fn null_map_value_is_distinct_from_missing_key_and_null_map() {
        let value = serde_json::json!({"cluster":"c","brokerName":"b","brokerAddrs":{"0":null,"1":"slave:10911"}});
        let broker: BrokerData = serde_json::from_value(value.clone()).unwrap();
        let addresses = broker.broker_addrs.as_ref().unwrap();
        assert_eq!(addresses.get(&0), Some(&None));
        assert!(!addresses.contains_key(&2));
        assert_eq!(
            addresses.get(&1).and_then(Option::as_deref),
            Some("slave:10911")
        );
        assert_eq!(
            serde_json::to_value(&broker).unwrap()["brokerAddrs"],
            value["brokerAddrs"]
        );
        let null_map: BrokerData = serde_json::from_value(
            serde_json::json!({"cluster":"c","brokerName":"b","brokerAddrs":null}),
        )
        .unwrap();
        assert!(null_map.broker_addrs.is_none());
    }
}

#[cfg(test)]
mod nullable_nested_route_tests {
    use super::TopicRouteData;

    #[test]
    fn nested_broker_and_queue_names_follow_invoked_java_defaults_and_json() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["nullableNestedRouteFields"].as_array().unwrap() {
            let route: TopicRouteData = serde_json::from_value(case["input"].clone())
                .expect("pinned Java accepts this nested route input");
            let queue = route.queue_datas.as_ref().unwrap()[0].as_ref().unwrap();
            let encoded = serde_json::to_value(&route).unwrap();
            assert_eq!(
                encoded["queueDatas"][0].get("brokerName").is_none(),
                case["queueNameNull"]
            );
            assert_eq!(
                encoded["brokerDatas"][0].get("brokerName").is_none(),
                case["brokerNameNull"]
            );
            assert_eq!(
                encoded["brokerDatas"][0].get("cluster").is_none(),
                case["clusterNull"]
            );
            assert_eq!(
                encoded["queueDatas"][0].get("brokerName").is_some(),
                case["encodedHasQueueName"]
            );
            assert_eq!(
                encoded["brokerDatas"][0].get("brokerName").is_some(),
                case["encodedHasBrokerName"]
            );
            assert_eq!(
                encoded["brokerDatas"][0].get("cluster").is_some(),
                case["encodedHasCluster"]
            );
            assert_eq!(queue.read_queue_nums, case["queueRead"]);
            assert_eq!(queue.write_queue_nums, case["queueWrite"]);
            assert_eq!(queue.perm, case["queuePerm"]);
            assert_eq!(queue.topic_sys_flag, case["queueFlag"]);
            let subscriptions = route.subscribe_queues("topic");
            assert_eq!(
                subscriptions.len(),
                case["subscribeQueueCount"].as_u64().unwrap() as usize
            );
            assert_eq!(
                subscriptions
                    .iter()
                    .any(|queue| queue.broker_name().is_none()),
                case["subscribeQueueBrokerNull"]
            );
            assert_eq!(
                subscriptions.iter().any(|queue| serde_json::to_value(queue)
                    .unwrap()
                    .get("brokerName")
                    .is_none()),
                case["subscribeJsonBrokerOmitted"]
            );
        }
    }
}

#[cfg(test)]
mod nullable_message_queue_tests {
    use rocketmq_classic_common::message::MessageQueue;

    #[test]
    fn nullable_message_queue_fields_match_invoked_java_defaults_and_json() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["nullableMessageQueueFields"].as_array().unwrap() {
            let queue: MessageQueue = serde_json::from_value(case["input"].clone())
                .expect("pinned Java decodes this MessageQueue");
            let encoded = serde_json::to_value(&queue).unwrap();
            assert_eq!(encoded.get("topic").is_none(), case["topicNull"]);
            assert_eq!(encoded.get("brokerName").is_none(), case["brokerNull"]);
            assert_eq!(encoded.get("topic").is_some(), case["encodedHasTopic"]);
            assert_eq!(
                encoded.get("brokerName").is_some(),
                case["encodedHasBroker"]
            );
            assert_eq!(queue.queue_id(), case["queueId"].as_i64().unwrap() as i32);
        }
    }

    #[test]
    fn message_queue_compare_to_matches_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["messageQueueCompareTo"].as_array().unwrap() {
            let left: MessageQueue = serde_json::from_value(case["left"].clone()).unwrap();
            let right: MessageQueue = serde_json::from_value(case["right"].clone()).unwrap();
            let result = left.java_compare_to(&right);
            if case["outcome"] == "OK" {
                assert_eq!(result.unwrap(), case["result"].as_i64().unwrap() as i32);
            } else {
                assert!(result.is_err());
            }
        }
    }

    #[test]
    fn message_queue_hash_equals_and_text_match_invoked_java() {
        use rocketmq_classic_common::message::MessageQueue;

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["messageQueueMethods"].as_array().unwrap() {
            let queue: MessageQueue = serde_json::from_value(case["queue"].clone()).unwrap();
            let copy = queue.clone();
            assert_eq!(
                queue.java_hash_code(),
                case["hashCode"].as_i64().unwrap() as i32
            );
            assert_eq!(queue.to_string(), case["toString"].as_str().unwrap());
            assert_eq!(queue == queue, case["equalsSelf"].as_bool().unwrap());
            assert_eq!(queue == copy, case["equalsCopy"].as_bool().unwrap());
            assert!(!case["equalsNull"].as_bool().unwrap());
            assert!(!case["equalsWrongType"].as_bool().unwrap());
            if case["case"] == "defaults" {
                assert_eq!(MessageQueue::default(), queue);
            }

            let mut topic_changed = queue.clone();
            topic_changed.set_topic(Some(match queue.topic() {
                Some(topic) => format!("{topic}x"),
                None => "topic".to_owned(),
            }));
            assert_eq!(
                queue == topic_changed,
                case["equalsTopicChanged"].as_bool().unwrap()
            );

            let mut broker_changed = queue.clone();
            broker_changed.set_broker_name(Some(match queue.broker_name() {
                Some(broker) => format!("{broker}x"),
                None => "broker".to_owned(),
            }));
            assert_eq!(
                queue == broker_changed,
                case["equalsBrokerChanged"].as_bool().unwrap()
            );

            let mut queue_id_changed = queue.clone();
            queue_id_changed.set_queue_id(queue.queue_id().wrapping_add(1));
            assert_eq!(
                queue == queue_id_changed,
                case["equalsQueueIdChanged"].as_bool().unwrap()
            );
        }
    }
}

#[cfg(test)]
mod nullable_route_collection_tests {
    use super::TopicRouteData;

    #[test]
    fn source_explicit_null_route_collections_decode_without_becoming_empty() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["nullableRouteCollections"].as_array().unwrap() {
            let route: TopicRouteData = serde_json::from_value(case["input"].clone())
                .expect("Java decodes explicit null route collections");
            let encoded = serde_json::to_value(route).unwrap();
            for (field, decoded_key, encoded_key) in [
                ("queueDatas", "decodedQueueNull", "encodedHasQueue"),
                ("brokerDatas", "decodedBrokerNull", "encodedHasBroker"),
                ("filterServerTable", "decodedFilterNull", "encodedHasFilter"),
            ] {
                assert_eq!(encoded.get(field).is_none(), case[decoded_key]);
                assert_eq!(encoded.get(field).is_some(), case[encoded_key]);
            }
            assert_eq!(
                encoded.get("topicQueueMappingByBroker").is_some(),
                case["encodedHasMapping"]
            );
        }
    }

    #[test]
    fn java_copy_constructor_normalizes_nullable_route_collections() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["nullableRouteCollections"].as_array().unwrap() {
            let route: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
            let copy = route.java_copy_constructed();
            assert_eq!(
                copy.queue_datas.as_ref().unwrap().is_empty(),
                case["copyQueueEmpty"]
            );
            assert_eq!(
                copy.broker_datas.as_ref().unwrap().is_empty(),
                case["copyBrokerEmpty"]
            );
            assert_eq!(
                copy.filter_server_table.as_ref().unwrap().is_empty(),
                case["copyFilterEmpty"]
            );
            assert_eq!(
                copy.topic_queue_mapping_by_broker.is_none(),
                case["input"]["topicQueueMappingByBroker"].is_null()
            );
        }
    }
}

#[cfg(test)]
mod nullable_static_mapping_tests {
    use super::TopicQueueMappingInfo;

    #[test]
    fn static_mapping_optional_fields_match_invoked_java_decode_and_json() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["nullableStaticMappingFields"].as_array().unwrap() {
            let mapping: TopicQueueMappingInfo = serde_json::from_value(case["input"].clone())
                .expect("pinned Java accepts this mapping input");
            let encoded = serde_json::to_value(&mapping).unwrap();
            for (field, null_key, encoded_key) in [
                ("topic", "topicNull", "encodedHasTopic"),
                ("bname", "bnameNull", "encodedHasBname"),
                ("scope", "scopeNull", "encodedHasScope"),
                ("currIdMap", "currIdMapNull", "encodedHasCurrIdMap"),
            ] {
                assert_eq!(encoded.get(field).is_none(), case[null_key]);
                assert_eq!(encoded.get(field).is_some(), case[encoded_key]);
            }
            assert_eq!(
                mapping
                    .curr_id_map
                    .as_ref()
                    .is_some_and(|map| map.is_empty()),
                case["currIdMapEmpty"]
            );
        }
    }

    #[test]
    fn static_mapping_map_entry_inputs_match_invoked_java_acceptance() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["staticMappingMapEntryCases"].as_array().unwrap() {
            let mapping: Result<TopicQueueMappingInfo, _> =
                serde_json::from_str(case["rawInput"].as_str().unwrap());
            let java_accepts = case["outcome"] == "accepted";
            assert_eq!(
                mapping.is_ok(),
                java_accepts,
                "{} ({})",
                case["case"],
                case["error"]
            );
            if let Ok(mapping) = mapping {
                assert_eq!(
                    mapping.curr_id_map.as_ref().map(|values| values.len()),
                    case["mapSize"].as_u64().map(|size| size as usize)
                );
                assert_eq!(
                    mapping
                        .curr_id_map
                        .as_ref()
                        .is_some_and(|values| values.contains_key(&0)),
                    case["hasKeyZero"]
                );
                assert_eq!(
                    mapping
                        .curr_id_map
                        .as_ref()
                        .and_then(|values| values.get(&0))
                        .copied(),
                    case["keyZeroValue"].as_i64().map(|value| value as i32)
                );
            }
        }
    }
}

#[cfg(test)]
mod overlapping_static_mapping_tests {
    use super::TopicRouteData;

    #[test]
    fn overlapping_static_owners_follow_invoked_java_epochs_and_fallback() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["overlappingStaticMappingOwners"]
            .as_array()
            .unwrap()
        {
            let route: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
            let endpoints = route.static_queue_endpoints("topic").unwrap();
            let rows: Vec<_> = endpoints
                .iter()
                .map(|(queue, owner)| {
                    serde_json::json!({
                        "queueId": queue.queue_id(),
                        "brokerName": queue.broker_name(),
                        "owner": owner,
                    })
                })
                .collect();
            assert_eq!(serde_json::json!(rows), case["endpoints"]);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicQueueMappingInfo {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub topic: Option<String>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_i32")]
    pub total_queues: i32,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub bname: Option<String>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_i64")]
    pub epoch: i64,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_bool")]
    pub dirty: bool,
    #[serde(
        default = "default_mapping_scope",
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub scope: Option<String>,
    #[serde(
        default = "default_mapping_ids",
        deserialize_with = "serde_fastjson::deserialize_optional_int_map",
        skip_serializing_if = "Option::is_none"
    )]
    pub curr_id_map: Option<BTreeMap<i32, i32>>,
}

fn default_mapping_ids() -> Option<BTreeMap<i32, i32>> {
    Some(BTreeMap::new())
}

impl Default for TopicQueueMappingInfo {
    fn default() -> Self {
        Self {
            topic: None,
            total_queues: 0,
            bname: None,
            epoch: 0,
            dirty: false,
            scope: default_mapping_scope(),
            curr_id_map: default_mapping_ids(),
        }
    }
}

impl TopicQueueMappingInfo {
    /// Encodes the inherited Java `RemotingSerializable.toJson(boolean)` form.
    /// Bean fields are key-sorted and integer map keys remain unquoted as in Fastjson2.
    pub fn java_to_json(&self, pretty_format: bool) -> Result<String, serde_json::Error> {
        java_route_json(self, pretty_format)
    }

    /// Encodes the inherited Java `RemotingSerializable.encode()` UTF-8 bytes.
    pub fn java_encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        Ok(self.java_to_json(false)?.into_bytes())
    }

    /// Decodes the inherited Java `RemotingSerializable.decode` form, including
    /// unquoted integer keys emitted for `currIdMap` by Fastjson2.
    pub fn java_decode(data: &[u8]) -> Result<Self, serde_json::Error> {
        parse_java_route_json(&String::from_utf8_lossy(data))
    }

    /// Decodes the inherited nullable Java utility form, including null bytes
    /// and JSON `null` as a null result.
    pub fn java_decode_nullable(data: Option<&[u8]>) -> Result<Option<Self>, serde_json::Error> {
        decode_java_route_json_nullable(data)
    }

    /// Decodes the inherited Java `RemotingSerializable.decodeList` form.
    pub fn java_decode_list(data: &[u8]) -> Result<Vec<Self>, serde_json::Error> {
        parse_java_route_json(&String::from_utf8_lossy(data))
    }

    /// Decodes the inherited nullable Java list utility, preserving null list
    /// results and null members.
    pub fn java_decode_list_nullable(
        data: Option<&[u8]>,
    ) -> Result<Option<Vec<Option<Self>>>, serde_json::Error> {
        decode_java_route_json_list_nullable(data)
    }

    /// Decodes the inherited Java `RemotingSerializable.fromJson` form.
    pub fn java_from_json(json: &str) -> Result<Self, serde_json::Error> {
        parse_java_route_json(json)
    }

    /// Decodes the inherited nullable Java `fromJson` utility form.
    pub fn java_from_json_nullable(json: Option<&str>) -> Result<Option<Self>, serde_json::Error> {
        parse_java_route_json_nullable(json)
    }

    /// Returns the pinned Java 5.5.1 `TopicQueueMappingInfo.hashCode` result.
    pub fn java_hash_code(&self) -> i32 {
        let mut result = self
            .topic
            .as_deref()
            .map(java_string_hash_code)
            .unwrap_or(0);
        result = result.wrapping_mul(31).wrapping_add(
            self.scope
                .as_deref()
                .map(java_string_hash_code)
                .unwrap_or(0),
        );
        result = result.wrapping_mul(31).wrapping_add(self.total_queues);
        result = result.wrapping_mul(31).wrapping_add(
            self.bname
                .as_deref()
                .map(java_string_hash_code)
                .unwrap_or(0),
        );
        result = result
            .wrapping_mul(31)
            .wrapping_add(java_long_hash_code(self.epoch));
        result = result.wrapping_mul(31).wrapping_add(i32::from(self.dirty));
        result.wrapping_mul(31).wrapping_add(
            self.curr_id_map
                .as_ref()
                .map(|ids| {
                    ids.iter().fold(0i32, |hash, (logical, physical)| {
                        hash.wrapping_add(*logical ^ *physical)
                    })
                })
                .unwrap_or(0),
        )
    }

    /// Returns the pinned Java 5.5.1 `TopicQueueMappingInfo.toString` text.
    ///
    /// The ID map is emitted in its deterministic Rust key order. Java stores
    /// this map in a `ConcurrentHashMap`, whose iteration order is unspecified.
    pub fn java_to_string(&self) -> String {
        let curr_id_map = self.curr_id_map.as_ref().map_or_else(
            || "null".to_owned(),
            |ids| {
                let entries = ids
                    .iter()
                    .map(|(logical, physical)| format!("{logical}={physical}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{{{entries}}}")
            },
        );
        format!(
            "TopicQueueMappingInfo{{topic='{}', scope='{}', totalQueues={}, bname='{}', epoch={}, dirty={}, currIdMap={}}}",
            self.topic.as_deref().unwrap_or("null"),
            self.scope.as_deref().unwrap_or("null"),
            self.total_queues,
            self.bname.as_deref().unwrap_or("null"),
            self.epoch,
            self.dirty,
            curr_id_map
        )
    }
}

#[cfg(test)]
mod topic_queue_mapping_method_tests {
    use super::TopicQueueMappingInfo;

    #[test]
    fn topic_queue_mapping_methods_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["topicQueueMappingMethods"].as_array().unwrap() {
            let left: TopicQueueMappingInfo = serde_json::from_value(case["left"].clone()).unwrap();
            let right: TopicQueueMappingInfo =
                serde_json::from_value(case["right"].clone()).unwrap();
            assert_eq!(left == right, case["equals"]);
            assert_eq!(
                left.java_hash_code(),
                case["leftHashCode"].as_i64().unwrap() as i32
            );
            assert_eq!(
                right.java_hash_code(),
                case["rightHashCode"].as_i64().unwrap() as i32
            );
            assert_eq!(left.java_to_string(), case["leftToString"]);
            assert_eq!(right.java_to_string(), case["rightToString"]);
        }
    }
}

#[cfg(test)]
mod topic_route_data_method_tests {
    use super::TopicRouteData;

    #[test]
    fn topic_route_data_methods_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["topicRouteDataMethods"].as_array().unwrap() {
            let left: TopicRouteData = serde_json::from_value(case["left"].clone()).unwrap();
            let right: TopicRouteData = serde_json::from_value(case["right"].clone()).unwrap();
            assert_eq!(left == right, case["equals"]);
            assert_eq!(
                left.java_hash_code(),
                case["leftHashCode"].as_i64().unwrap() as i32
            );
            assert_eq!(
                right.java_hash_code(),
                case["rightHashCode"].as_i64().unwrap() as i32
            );
        }
    }

    #[test]
    fn topic_route_data_nullable_nested_values_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let method_case = &fixture["topicRouteDataMethods"][4];
        let left: TopicRouteData = serde_json::from_value(method_case["left"].clone())
            .expect("pinned Java accepts null nested route values");
        let right: TopicRouteData = serde_json::from_value(method_case["right"].clone())
            .expect("pinned Java accepts null nested route values");
        assert_eq!(left == right, method_case["equals"]);
        assert_eq!(
            left.java_hash_code(),
            method_case["leftHashCode"].as_i64().unwrap() as i32
        );
        assert_eq!(
            right.java_hash_code(),
            method_case["rightHashCode"].as_i64().unwrap() as i32
        );
        let string_case = fixture["topicRouteDataToString"]
            .as_array()
            .unwrap()
            .last()
            .unwrap();
        let route: TopicRouteData = serde_json::from_value(string_case["input"].clone())
            .expect("pinned Java accepts null nested route values");
        assert_eq!(route.java_to_string(), string_case["toString"]);
    }
}

#[cfg(test)]
mod topic_route_data_to_string_tests {
    use super::TopicRouteData;

    #[test]
    fn topic_route_data_to_string_matches_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["topicRouteDataToString"].as_array().unwrap() {
            let route: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
            assert_eq!(route.java_to_string(), case["toString"]);
        }
    }
}

#[cfg(test)]
mod topic_route_data_changed_method_tests {
    use super::TopicRouteData;

    #[test]
    fn topic_route_data_changed_matches_invoked_java_comparison_and_sort_errors() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["topicRouteDataChanged"].as_array().unwrap() {
            let current: TopicRouteData = serde_json::from_value(case["current"].clone()).unwrap();
            let old: Option<TopicRouteData> = if case["old"].is_null() {
                None
            } else {
                Some(serde_json::from_value(case["old"].clone()).unwrap())
            };
            let result = current.java_topic_route_data_changed(old.as_ref());
            if case["outcome"] == "OK" {
                assert_eq!(result.unwrap(), case["changed"]);
            } else {
                assert!(result.is_err());
            }
        }
    }
}

#[cfg(test)]
mod topic_route_data_clone_method_tests {
    use super::TopicRouteData;

    #[test]
    fn topic_route_data_copy_and_clone_methods_match_invoked_java_values_and_failures() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["topicRouteDataClone"].as_array().unwrap() {
            let source: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
            let expected_copy: TopicRouteData =
                serde_json::from_value(case["copyConstructor"].clone()).unwrap();
            assert_eq!(
                source.java_copy_constructed(),
                expected_copy,
                "{}",
                case["id"]
            );

            let clone = source.clone_topic_route_data();
            assert_eq!(
                clone.is_ok(),
                case["cloneOutcome"] == "OK",
                "{}",
                case["id"]
            );
            if let Ok(clone) = clone {
                let expected: TopicRouteData =
                    serde_json::from_value(case["clone"].clone()).unwrap();
                assert_eq!(clone, expected, "{}", case["id"]);
            }

            let deep_clone = source.deep_clone_topic_route_data();
            assert_eq!(
                deep_clone.is_ok(),
                case["deepCloneOutcome"] == "OK",
                "{}",
                case["id"]
            );
            if let Ok(deep_clone) = deep_clone {
                let expected: TopicRouteData =
                    serde_json::from_value(case["deepClone"].clone()).unwrap();
                assert_eq!(deep_clone, expected, "{}", case["id"]);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicRouteData {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub order_topic_conf: Option<String>,
    /// Preserves both a null Java list and null QueueData entries.
    #[serde(default = "default_route_vec", skip_serializing_if = "Option::is_none")]
    pub queue_datas: Option<Vec<Option<QueueData>>>,
    /// Preserves both a null Java list and null BrokerData entries.
    #[serde(default = "default_route_vec", skip_serializing_if = "Option::is_none")]
    pub broker_datas: Option<Vec<Option<BrokerData>>>,
    /// Preserves null filter lists and null String entries inside those lists.
    #[serde(
        default = "default_filter_table",
        deserialize_with = "serde_fastjson::deserialize_optional_string_list_map",
        skip_serializing_if = "Option::is_none"
    )]
    pub filter_server_table: Option<BTreeMap<String, Option<Vec<Option<String>>>>>,
    /// Preserves null mapping values in the Java route map.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic_queue_mapping_by_broker: Option<BTreeMap<String, Option<TopicQueueMappingInfo>>>,
}

#[cfg(test)]
mod route_dto_input_cases_tests {
    use super::{
        BrokerData, QueueData, TopicQueueMappingInfo, TopicRouteData, parse_java_route_json,
    };
    use rocketmq_classic_common::message::MessageQueue;

    #[test]
    fn route_dto_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let cases = fixture["routeDtoInputCases"].as_array().unwrap();
        for required in [
            "queue-unicode-decimal-string",
            "mapping-unicode-decimal-strings",
            "message-queue-unicode-decimal-id",
            "broker-unicode-decimal-address-key",
            "mapping-unicode-decimal-id-key",
            "queue-fastjson-leading-plus-number",
            "queue-fastjson-leading-zero-number",
            "queue-fastjson-leading-plus-fraction",
            "queue-fastjson-leading-zero-fraction",
            "mapping-fastjson-leading-plus-exponent",
            "mapping-fastjson-leading-zero-exponent",
            "queue-fastjson-vertical-tab-prefix",
            "queue-fastjson-form-feed-prefix",
            "queue-fastjson-form-feed-before-number",
            "queue-fastjson-vertical-tab-before-number",
            "queue-fastjson-nbsp-prefix",
            "queue-fastjson-bom-prefix",
            "queue-fastjson-bom-before-number",
        ] {
            assert!(
                cases.iter().any(|case| case["id"] == required),
                "missing {required}"
            );
        }
        for case in cases {
            let raw_input = case["rawInput"].as_str().unwrap();
            let decoded =
                match case["type"].as_str().unwrap() {
                    "queueData" => {
                        parse_java_route_json::<QueueData>(raw_input).and_then(serde_json::to_value)
                    }
                    "messageQueue" => parse_java_route_json::<MessageQueue>(raw_input)
                        .and_then(serde_json::to_value),
                    "brokerData" => parse_java_route_json::<BrokerData>(raw_input)
                        .and_then(serde_json::to_value),
                    "mapping" => parse_java_route_json::<TopicQueueMappingInfo>(raw_input)
                        .and_then(serde_json::to_value),
                    "topicRouteData" => parse_java_route_json::<TopicRouteData>(raw_input)
                        .and_then(serde_json::to_value),
                    other => panic!("unknown DTO case type {other}"),
                };
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(decoded.is_ok(), java_accepts, "{}: {decoded:?}", case["id"]);
            if let Ok(decoded) = decoded {
                for (field, expected) in case["fields"].as_object().unwrap() {
                    let actual = decoded.get(field);
                    if expected.is_null() {
                        assert!(
                            actual.is_none_or(serde_json::Value::is_null),
                            "{}.{field}: expected null/omitted, got {actual:?}",
                            case["id"]
                        );
                    } else {
                        assert_eq!(actual, Some(expected), "{}.{field}", case["id"]);
                    }
                }
            }
        }
    }
}

fn default_route_vec<T>() -> Option<Vec<T>> {
    Some(Vec::new())
}

fn default_filter_table() -> Option<BTreeMap<String, Option<Vec<Option<String>>>>> {
    Some(BTreeMap::new())
}

impl Default for TopicRouteData {
    fn default() -> Self {
        Self {
            order_topic_conf: None,
            queue_datas: default_route_vec(),
            broker_datas: default_route_vec(),
            filter_server_table: default_filter_table(),
            topic_queue_mapping_by_broker: None,
        }
    }
}

/// Null-value failures produced when Java `TopicRouteData.topicRouteDataChanged`
/// sorts its nested DTO lists.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TopicRouteDataChangedError {
    #[error("null QueueData entry during sort")]
    NullQueueData,
    #[error("null QueueData broker name during sort")]
    NullQueueBrokerName,
    #[error("null BrokerData entry during sort")]
    NullBrokerData,
    #[error("null BrokerData broker name during sort")]
    NullBrokerName,
}

/// Null-field failures from the pinned Java route copy/clone helpers.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TopicRouteDataCloneError {
    #[error("null queue data list (Java NullPointerException)")]
    NullQueueDataList,
    #[error("null broker data list (Java NullPointerException)")]
    NullBrokerDataList,
    #[error("null filter server table (Java NullPointerException)")]
    NullFilterServerTable,
    #[error("null QueueData entry during deep clone (Java NullPointerException)")]
    NullQueueData,
    #[error("null BrokerData entry during deep clone (Java NullPointerException)")]
    NullBrokerData,
    #[error("null filter server list during deep clone (Java NullPointerException)")]
    NullFilterServerList,
    #[error("null TopicQueueMappingInfo during deep clone (Java NullPointerException)")]
    NullTopicQueueMappingInfo,
    #[error("null mapping ID map during deep clone (Java NullPointerException)")]
    NullMappingIdMap,
}

fn default_mapping_scope() -> Option<String> {
    Some("__global__".to_owned())
}

impl TopicRouteData {
    /// Encodes the inherited Java `RemotingSerializable.toJson(boolean)` form.
    /// Bean fields are key-sorted and integer map keys remain unquoted as in Fastjson2.
    pub fn java_to_json(&self, pretty_format: bool) -> Result<String, serde_json::Error> {
        java_route_json(self, pretty_format)
    }

    /// Encodes the inherited Java `RemotingSerializable.encode()` UTF-8 bytes.
    pub fn java_encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        Ok(self.java_to_json(false)?.into_bytes())
    }

    /// Decodes the inherited Java `RemotingSerializable.decode` form, including
    /// unquoted integer keys emitted for nested broker and static-topic maps.
    pub fn java_decode(data: &[u8]) -> Result<Self, serde_json::Error> {
        parse_java_route_json(&String::from_utf8_lossy(data))
    }

    /// Decodes the inherited nullable Java utility form, including null bytes
    /// and JSON `null` as a null result.
    pub fn java_decode_nullable(data: Option<&[u8]>) -> Result<Option<Self>, serde_json::Error> {
        decode_java_route_json_nullable(data)
    }

    /// Decodes the inherited Java `RemotingSerializable.decodeList` form.
    pub fn java_decode_list(data: &[u8]) -> Result<Vec<Self>, serde_json::Error> {
        parse_java_route_json(&String::from_utf8_lossy(data))
    }

    /// Decodes the inherited nullable Java list utility, preserving null list
    /// results and null members.
    pub fn java_decode_list_nullable(
        data: Option<&[u8]>,
    ) -> Result<Option<Vec<Option<Self>>>, serde_json::Error> {
        decode_java_route_json_list_nullable(data)
    }

    /// Decodes the inherited Java `RemotingSerializable.fromJson` form.
    pub fn java_from_json(json: &str) -> Result<Self, serde_json::Error> {
        parse_java_route_json(json)
    }

    /// Decodes the inherited nullable Java `fromJson` utility form.
    pub fn java_from_json_nullable(json: Option<&str>) -> Result<Option<Self>, serde_json::Error> {
        parse_java_route_json_nullable(json)
    }

    /// Copies the DTO using the pinned Java `TopicRouteData` copy-constructor
    /// collection defaults. Explicit null queue, broker and filter collections
    /// become empty containers; the mapping table remains nullable.
    ///
    /// Rust owns nested values, so this copy is structurally independent even
    /// where Java's copied collection elements retain object identity.
    pub fn java_copy_constructed(&self) -> Self {
        let mut copy = self.clone();
        copy.queue_datas.get_or_insert_with(Vec::new);
        copy.broker_datas.get_or_insert_with(Vec::new);
        copy.filter_server_table.get_or_insert_with(BTreeMap::new);
        copy
    }

    /// Copies the route using Java's `cloneTopicRouteData` container checks.
    ///
    /// Java shallow-clones the outer containers and shares mutable nested DTOs.
    /// Rust owns those nested DTO values, so this returns a structural value
    /// copy. The runtime's raw table separately preserves aliases with
    /// `TopicRouteDataHandle`.
    pub fn clone_topic_route_data(&self) -> Result<Self, TopicRouteDataCloneError> {
        let queue_datas = self
            .queue_datas
            .as_ref()
            .ok_or(TopicRouteDataCloneError::NullQueueDataList)?;
        let broker_datas = self
            .broker_datas
            .as_ref()
            .ok_or(TopicRouteDataCloneError::NullBrokerDataList)?;
        let filter_server_table = self
            .filter_server_table
            .as_ref()
            .ok_or(TopicRouteDataCloneError::NullFilterServerTable)?;
        Ok(Self {
            order_topic_conf: self.order_topic_conf.clone(),
            queue_datas: Some(queue_datas.clone()),
            broker_datas: Some(broker_datas.clone()),
            filter_server_table: Some(filter_server_table.clone()),
            topic_queue_mapping_by_broker: self.topic_queue_mapping_by_broker.clone(),
        })
    }

    /// Performs Java `deepCloneTopicRouteData` validation and returns an
    /// owned structural copy of every route DTO and nested collection.
    pub fn deep_clone_topic_route_data(&self) -> Result<Self, TopicRouteDataCloneError> {
        let queue_datas = self
            .queue_datas
            .as_ref()
            .ok_or(TopicRouteDataCloneError::NullQueueDataList)?
            .iter()
            .map(|queue| {
                queue
                    .as_ref()
                    .cloned()
                    .ok_or(TopicRouteDataCloneError::NullQueueData)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let broker_datas = self
            .broker_datas
            .as_ref()
            .ok_or(TopicRouteDataCloneError::NullBrokerDataList)?
            .iter()
            .map(|broker| {
                broker
                    .as_ref()
                    .cloned()
                    .ok_or(TopicRouteDataCloneError::NullBrokerData)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let filter_server_table = self
            .filter_server_table
            .as_ref()
            .ok_or(TopicRouteDataCloneError::NullFilterServerTable)?
            .iter()
            .map(|(broker, servers)| {
                Ok((
                    broker.clone(),
                    Some(
                        servers
                            .as_ref()
                            .ok_or(TopicRouteDataCloneError::NullFilterServerList)?
                            .clone(),
                    ),
                ))
            })
            .collect::<Result<BTreeMap<_, _>, TopicRouteDataCloneError>>()?;
        let topic_queue_mapping_by_broker = self
            .topic_queue_mapping_by_broker
            .as_ref()
            .map(|mappings| {
                mappings
                    .iter()
                    .map(|(broker, mapping)| {
                        let mapping = mapping
                            .as_ref()
                            .ok_or(TopicRouteDataCloneError::NullTopicQueueMappingInfo)?;
                        let curr_id_map = mapping
                            .curr_id_map
                            .as_ref()
                            .ok_or(TopicRouteDataCloneError::NullMappingIdMap)?;
                        let mut copy = mapping.clone();
                        copy.curr_id_map = Some(curr_id_map.clone());
                        Ok((broker.clone(), Some(copy)))
                    })
                    .collect::<Result<BTreeMap<_, _>, TopicRouteDataCloneError>>()
            })
            .transpose()?;
        Ok(Self {
            order_topic_conf: self.order_topic_conf.clone(),
            queue_datas: Some(queue_datas.into_iter().map(Some).collect()),
            broker_datas: Some(broker_datas.into_iter().map(Some).collect()),
            filter_server_table: Some(filter_server_table),
            topic_queue_mapping_by_broker,
        })
    }

    /// Returns the pinned Java 5.5.1 `TopicRouteData.hashCode` result.
    pub fn java_hash_code(&self) -> i32 {
        let broker_hash = self
            .broker_datas
            .as_deref()
            .map(|brokers| {
                java_list_hash_code(brokers, |broker| {
                    broker.as_ref().map(BrokerData::java_hash_code).unwrap_or(0)
                })
            })
            .unwrap_or(0);
        let order_hash = self
            .order_topic_conf
            .as_deref()
            .map(java_string_hash_code)
            .unwrap_or(0);
        let queue_hash = self
            .queue_datas
            .as_deref()
            .map(|queues| {
                java_list_hash_code(queues, |queue| {
                    queue.as_ref().map(QueueData::java_hash_code).unwrap_or(0)
                })
            })
            .unwrap_or(0);
        let filter_hash = self
            .filter_server_table
            .as_ref()
            .map(|filters| {
                filters.iter().fold(0i32, |hash, (broker, servers)| {
                    let servers_hash = servers
                        .as_ref()
                        .map(|servers| {
                            java_list_hash_code(servers, |server| {
                                server.as_deref().map(java_string_hash_code).unwrap_or(0)
                            })
                        })
                        .unwrap_or(0);
                    hash.wrapping_add(java_string_hash_code(broker) ^ servers_hash)
                })
            })
            .unwrap_or(0);
        let mapping_hash = self
            .topic_queue_mapping_by_broker
            .as_ref()
            .map(|mappings| {
                mappings.iter().fold(0i32, |hash, (broker, mapping)| {
                    let mapping_hash = mapping
                        .as_ref()
                        .map(TopicQueueMappingInfo::java_hash_code)
                        .unwrap_or(0);
                    hash.wrapping_add(java_string_hash_code(broker) ^ mapping_hash)
                })
            })
            .unwrap_or(0);
        [
            broker_hash,
            order_hash,
            queue_hash,
            filter_hash,
            mapping_hash,
        ]
        .into_iter()
        .fold(1i32, |hash, value| {
            hash.wrapping_mul(31).wrapping_add(value)
        })
    }

    /// Returns the pinned Java 5.5.1 `TopicRouteData.toString` text.
    ///
    /// Map entries use deterministic Rust key order. Java's `HashMap`
    /// iteration order is unspecified.
    pub fn java_to_string(&self) -> String {
        let queue_datas = self.queue_datas.as_deref().map_or_else(
            || "null".to_owned(),
            |queues| {
                let values = queues
                    .iter()
                    .map(|queue| {
                        queue
                            .as_ref()
                            .map(QueueData::java_to_string)
                            .unwrap_or_else(|| "null".to_owned())
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("[{values}]")
            },
        );
        let broker_datas = self.broker_datas.as_deref().map_or_else(
            || "null".to_owned(),
            |brokers| {
                let values = brokers
                    .iter()
                    .map(|broker| {
                        broker
                            .as_ref()
                            .map(BrokerData::java_to_string)
                            .unwrap_or_else(|| "null".to_owned())
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("[{values}]")
            },
        );
        let filter_server_table = self.filter_server_table.as_ref().map_or_else(
            || "null".to_owned(),
            |filters| {
                let entries = filters
                    .iter()
                    .map(|(broker, servers)| {
                        let servers = servers.as_ref().map_or_else(
                            || "null".to_owned(),
                            |servers| {
                                let values = servers
                                    .iter()
                                    .map(|server| server.as_deref().unwrap_or("null"))
                                    .collect::<Vec<_>>()
                                    .join(", ");
                                format!("[{values}]")
                            },
                        );
                        format!("{broker}={servers}")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{{{entries}}}")
            },
        );
        let mapping_table = self.topic_queue_mapping_by_broker.as_ref().map_or_else(
            || "null".to_owned(),
            |mappings| {
                let entries = mappings
                    .iter()
                    .map(|(broker, mapping)| {
                        format!(
                            "{broker}={}",
                            mapping
                                .as_ref()
                                .map(TopicQueueMappingInfo::java_to_string)
                                .unwrap_or_else(|| "null".to_owned())
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{{{entries}}}")
            },
        );
        format!(
            "TopicRouteData [orderTopicConf={}, queueDatas={}, brokerDatas={}, filterServerTable={}, topicQueueMappingInfoTable={}]",
            self.order_topic_conf.as_deref().unwrap_or("null"),
            queue_datas,
            broker_datas,
            filter_server_table,
            mapping_table
        )
    }

    /// Matches pinned Java 5.5.1 `TopicRouteData.topicRouteDataChanged`.
    ///
    /// Java first copy-constructs both routes, normalizing null queue/broker/
    /// filter collections to empty, then sorts QueueData and BrokerData using
    /// their `compareTo` methods. A null list element or broker name fails
    /// only when sorting compares more than one element.
    pub fn java_topic_route_data_changed(
        &self,
        old: Option<&Self>,
    ) -> Result<bool, TopicRouteDataChangedError> {
        let Some(old) = old else {
            return Ok(true);
        };
        let (mut current, mut old) = (self.java_copy_constructed(), old.java_copy_constructed());
        for route in [&mut current, &mut old] {
            let queues = route.queue_datas.get_or_insert_with(Vec::new);
            if queues.len() > 1 {
                if queues.iter().any(Option::is_none) {
                    return Err(TopicRouteDataChangedError::NullQueueData);
                }
                if queues
                    .iter()
                    .flatten()
                    .any(|queue| queue.broker_name.is_none())
                {
                    return Err(TopicRouteDataChangedError::NullQueueBrokerName);
                }
            }
            queues.sort_by(|left, right| {
                left.as_ref()
                    .expect("null QueueData entries were rejected before sorting")
                    .java_compare_to(
                        right
                            .as_ref()
                            .expect("null QueueData entries were rejected before sorting"),
                    )
                    .expect("null QueueData names were rejected before sorting")
                    .cmp(&0)
            });

            let brokers = route.broker_datas.get_or_insert_with(Vec::new);
            if brokers.len() > 1 {
                if brokers.iter().any(Option::is_none) {
                    return Err(TopicRouteDataChangedError::NullBrokerData);
                }
                if brokers
                    .iter()
                    .flatten()
                    .any(|broker| broker.broker_name.is_none())
                {
                    return Err(TopicRouteDataChangedError::NullBrokerName);
                }
            }
            brokers.sort_by(|left, right| {
                left.as_ref()
                    .expect("null BrokerData entries were rejected before sorting")
                    .java_compare_to(
                        right
                            .as_ref()
                            .expect("null BrokerData entries were rejected before sorting"),
                    )
                    .expect("null BrokerData names were rejected before sorting")
                    .cmp(&0)
            });
            route.filter_server_table.get_or_insert_with(BTreeMap::new);
        }
        Ok(current != old)
    }

    pub fn publish_queues(&self, topic: &str) -> Vec<MessageQueue> {
        if let Some(order_conf) = self
            .order_topic_conf
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            return order_conf
                .split(';')
                .filter_map(|entry| {
                    let (broker, count) = entry.split_once(':')?;
                    let count = count.parse::<i32>().ok()?;
                    Some((0..count).map(move |queue_id| MessageQueue::new(topic, broker, queue_id)))
                })
                .flatten()
                .collect();
        }
        if self.order_topic_conf.is_none() {
            if let Some(endpoints) = self.static_queue_endpoints(topic) {
                let mut queues = endpoints.into_keys().collect::<Vec<_>>();
                queues.sort_by_key(MessageQueue::queue_id);
                return queues;
            }
        }

        let mut masters = BTreeMap::new();
        for broker in self.broker_datas.as_deref().unwrap_or(&[]).iter().flatten() {
            masters
                .entry(broker.broker_name.as_deref())
                .or_insert_with(|| {
                    broker
                        .broker_addrs
                        .as_ref()
                        .is_some_and(|addresses| addresses.contains_key(&0))
                });
        }
        let mut queue_data = self
            .queue_datas
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .flatten()
            .collect::<Vec<_>>();
        queue_data.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
        queue_data
            .into_iter()
            .filter(|data| {
                data.write_queue_nums > 0
                    && data.perm & 0x2 != 0
                    && masters
                        .get(&data.broker_name.as_deref())
                        .copied()
                        .unwrap_or(false)
            })
            .flat_map(|data| {
                (0..data.write_queue_nums).map(|queue_id| {
                    MessageQueue::new_nullable_broker(topic, data.broker_name.clone(), queue_id)
                })
            })
            .collect()
    }

    pub fn subscribe_queues(&self, topic: &str) -> Vec<MessageQueue> {
        if let Some(endpoints) = self.static_queue_endpoints(topic) {
            return endpoints.into_keys().collect();
        }
        let mut counts = BTreeMap::<Option<&str>, i32>::new();
        for data in self
            .queue_datas
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .flatten()
            .filter(|data| data.read_queue_nums > 0 && data.perm & 0x4 != 0)
        {
            let count = counts.entry(data.broker_name.as_deref()).or_default();
            *count = (*count).max(data.read_queue_nums);
        }
        counts
            .into_iter()
            .flat_map(|(broker, count)| {
                (0..count).map(move |queue_id| {
                    MessageQueue::new_nullable_broker(topic, broker.map(str::to_owned), queue_id)
                })
            })
            .collect()
    }

    pub fn static_queue_endpoints(&self, topic: &str) -> Option<BTreeMap<MessageQueue, String>> {
        if self
            .topic_queue_mapping_by_broker
            .as_ref()
            .is_none_or(BTreeMap::is_empty)
        {
            return None;
        }
        let mut owners = BTreeMap::<(String, i32), &TopicQueueMappingInfo>::new();
        for mapping in self
            .topic_queue_mapping_by_broker
            .as_ref()?
            .values()
            .flatten()
        {
            if let (Some(scope), Some(ids)) = (&mapping.scope, &mapping.curr_id_map) {
                for id in ids.keys() {
                    let key = (logical_broker(scope), *id);
                    if owners
                        .get(&key)
                        .is_none_or(|old| old.epoch <= mapping.epoch)
                    {
                        owners.insert(key, mapping);
                    }
                }
            }
        }
        let mut endpoints = BTreeMap::new();
        for queue in self.static_topic_queues(topic)? {
            let broker = owners
                .get(&(
                    queue.broker_name().unwrap_or_default().to_owned(),
                    queue.queue_id(),
                ))
                .and_then(|mapping| mapping.bname.clone())
                .unwrap_or_else(|| "__syslo__none__".to_owned());
            endpoints.insert(queue, broker);
        }
        Some(endpoints)
    }

    fn static_topic_queues(&self, topic: &str) -> Option<Vec<MessageQueue>> {
        if self
            .topic_queue_mapping_by_broker
            .as_ref()
            .is_none_or(BTreeMap::is_empty)
        {
            return None;
        }
        let mut totals = BTreeMap::<String, i32>::new();
        for mapping in self
            .topic_queue_mapping_by_broker
            .as_ref()?
            .values()
            .flatten()
        {
            if let Some(scope) = mapping.scope.as_deref().filter(|_| mapping.epoch >= -1) {
                // Java never updates its -1 threshold. Its endpoint map also
                // deduplicates logical names that different scope strings alias.
                let total = totals.entry(logical_broker(scope)).or_default();
                *total = (*total).max(mapping.total_queues);
            }
        }
        Some(
            totals
                .into_iter()
                .flat_map(|(broker, count)| {
                    (0..count).map(move |queue_id| MessageQueue::new(topic, &broker, queue_id))
                })
                .collect(),
        )
    }
}

fn logical_broker(scope: &str) -> String {
    // TopicQueueMappingUtils only strips the prefix for the exact global scope.
    format!(
        "__syslo__{}",
        if scope == "__global__" {
            &scope[2..]
        } else {
            scope
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_conversion_matches_pinned_java_epoch_scope_and_endpoint_oracle() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let mut checked = 0;
        for case in fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|case| case["input"]["topicQueueMappingByBroker"].is_object())
        {
            let route: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
            assert_eq!(
                serde_json::to_value(route.publish_queues("topic")).unwrap(),
                case["publish"]
            );
            assert_eq!(
                serde_json::to_value(route.subscribe_queues("topic")).unwrap(),
                case["subscribe"]
            );
            let endpoints = route.static_queue_endpoints("topic").unwrap();
            assert_eq!(
                serde_json::to_value(
                    endpoints
                        .iter()
                        .map(|(queue, broker)| serde_json::json!({"queue":queue,"broker":broker}))
                        .collect::<Vec<_>>()
                )
                .unwrap(),
                case["endpoints"]
            );
            checked += 1;
        }
        assert_eq!(checked, 4);
    }

    #[test]
    fn ordinary_publish_uses_first_matching_broker_and_static_publish_orders_ids_across_scopes() {
        let mut route = route();
        let mut duplicate = route.broker_datas.as_ref().unwrap()[0]
            .as_ref()
            .unwrap()
            .clone();
        duplicate.broker_addrs = Some(BTreeMap::from([(0, Some("other-master".into()))]));
        route.broker_datas.as_mut().unwrap()[0]
            .as_mut()
            .unwrap()
            .broker_addrs = Some(BTreeMap::from([(1, Some("slave".into()))]));
        route.broker_datas.as_mut().unwrap().push(Some(duplicate));
        assert!(route.publish_queues("topic").is_empty());
        route.topic_queue_mapping_by_broker = Some(BTreeMap::from([
            (
                "a".into(),
                Some(TopicQueueMappingInfo {
                    total_queues: 2,
                    epoch: 0,
                    scope: Some("a".into()),
                    curr_id_map: Some(BTreeMap::new()),
                    ..Default::default()
                }),
            ),
            (
                "b".into(),
                Some(TopicQueueMappingInfo {
                    total_queues: 2,
                    epoch: 0,
                    scope: Some("b".into()),
                    curr_id_map: Some(BTreeMap::new()),
                    ..Default::default()
                }),
            ),
        ]));
        assert_eq!(
            route
                .publish_queues("topic")
                .iter()
                .map(MessageQueue::queue_id)
                .collect::<Vec<_>>(),
            [0, 0, 1, 1]
        );
    }

    #[test]
    fn duplicate_queue_data_retains_java_stable_publish_order_and_set_subscription() {
        let mut route = route();
        route.broker_datas.as_mut().unwrap()[0]
            .as_mut()
            .unwrap()
            .broker_addrs = Some(BTreeMap::from([(0, Some("master".into()))]));
        route.queue_datas.as_mut().unwrap().retain(|queue| {
            queue
                .as_ref()
                .is_some_and(|queue| queue.broker_name.as_deref() == Some("b1"))
        });
        let mut second = route.queue_datas.as_ref().unwrap()[0]
            .as_ref()
            .unwrap()
            .clone();
        second.read_queue_nums = 1;
        second.write_queue_nums = 1;
        route.queue_datas.as_mut().unwrap().push(Some(second));
        assert_eq!(
            route
                .publish_queues("topic")
                .iter()
                .map(MessageQueue::queue_id)
                .collect::<Vec<_>>(),
            [0, 1, 2, 0]
        );
        assert_eq!(
            route
                .subscribe_queues("topic")
                .iter()
                .map(MessageQueue::queue_id)
                .collect::<Vec<_>>(),
            [0, 1]
        );
    }

    fn route() -> TopicRouteData {
        TopicRouteData {
            queue_datas: Some(vec![
                Some(QueueData {
                    broker_name: Some("b1".into()),
                    read_queue_nums: 2,
                    write_queue_nums: 3,
                    perm: 6,
                    topic_sys_flag: 0,
                }),
                Some(QueueData {
                    broker_name: Some("b2".into()),
                    read_queue_nums: 4,
                    write_queue_nums: 4,
                    perm: 4,
                    topic_sys_flag: 0,
                }),
                Some(QueueData {
                    broker_name: Some("missing".into()),
                    read_queue_nums: 9,
                    write_queue_nums: 9,
                    perm: 6,
                    topic_sys_flag: 0,
                }),
            ]),
            broker_datas: Some(vec![
                Some(BrokerData {
                    broker_name: Some("b1".into()),
                    ..BrokerData::default()
                }),
                Some(BrokerData {
                    broker_name: Some("b2".into()),
                    ..BrokerData::default()
                }),
            ]),
            ..TopicRouteData::default()
        }
    }

    #[test]
    fn converts_writable_and_readable_queues_without_absent_masters() {
        let mut route = route();
        route.broker_datas.as_mut().unwrap()[0]
            .as_mut()
            .unwrap()
            .broker_addrs = Some(BTreeMap::from([(0, Some("b1-master".into()))]));
        assert_eq!(route.publish_queues("t").len(), 3);
        assert_eq!(route.subscribe_queues("t").len(), 15);
    }

    #[test]
    fn order_topic_configuration_takes_precedence_over_route_data() {
        let mut route = route();
        route.order_topic_conf = Some("ordered:2;other:1".into());
        let queues = route.publish_queues("t");
        assert_eq!(queues.len(), 3);
        assert_eq!(queues[0].broker_name(), Some("ordered"));
        assert_eq!(queues[2].queue_id(), 0);
    }

    #[test]
    fn static_routes_allocate_logical_queue_space_per_scope() {
        let mut route = route();
        route.topic_queue_mapping_by_broker = Some(BTreeMap::from([(
            "b1".into(),
            Some(TopicQueueMappingInfo {
                topic: Some("t".into()),
                total_queues: 8,
                bname: Some("b1".into()),
                epoch: 0,
                dirty: false,
                scope: Some("__global__".into()),
                curr_id_map: Some(BTreeMap::from([(0, 0)])),
            }),
        )]));
        let queues = route.publish_queues("t");
        assert_eq!(queues.len(), 8);
        assert_eq!(queues[0].broker_name(), Some("__syslo__global__"));
        assert_eq!(queues[1].broker_name(), Some("__syslo__global__"));
        let endpoints = route.static_queue_endpoints("t").unwrap();
        assert_eq!(endpoints.get(&queues[0]).unwrap(), "b1");
        assert_eq!(endpoints.get(&queues[1]).unwrap(), "__syslo__none__");
    }

    #[test]
    fn preserves_static_topic_mapping_and_json_field_names() {
        let mut route = route();
        route.topic_queue_mapping_by_broker = Some(BTreeMap::from([(
            "b1".into(),
            Some(TopicQueueMappingInfo {
                topic: Some("t".into()),
                total_queues: 8,
                bname: Some("b1".into()),
                epoch: i64::MAX,
                dirty: true,
                scope: Some("global".into()),
                curr_id_map: Some(BTreeMap::from([(0, 2)])),
            }),
        )]));
        let json = serde_json::to_value(&route).unwrap();
        assert_eq!(json["topicQueueMappingByBroker"]["b1"]["totalQueues"], 8);
        assert_eq!(json["topicQueueMappingByBroker"]["b1"]["epoch"], i64::MAX);
    }

    #[test]
    fn inherited_remoting_serialization_matches_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();

        macro_rules! check_case {
            ($type:ty, $case:expr) => {{
                let case = $case;
                let raw_input = case["rawInput"].as_str().unwrap();
                let value: $type = serde_json::from_str(raw_input).unwrap();
                let native_json = value.java_to_json(false).unwrap();
                assert_eq!(native_json, case["instanceJson"], "{} toJson", case["id"]);
                assert_eq!(
                    native_json, case["staticJson"],
                    "{} static toJson",
                    case["id"]
                );
                assert_eq!(
                    native_json.as_bytes(),
                    case["instanceEncodedJson"].as_str().unwrap().as_bytes(),
                    "{} encode",
                    case["id"]
                );
                assert_eq!(
                    native_json.as_bytes(),
                    case["staticEncodedJson"].as_str().unwrap().as_bytes(),
                    "{} static encode",
                    case["id"]
                );
                assert_eq!(
                    native_json.as_bytes(),
                    case["featureEncodedJson"].as_str().unwrap().as_bytes(),
                    "{} feature encode",
                    case["id"]
                );
                assert_eq!(
                    value.java_to_json(true).unwrap(),
                    case["instancePrettyJson"],
                    "{} pretty toJson",
                    case["id"]
                );
                assert_eq!(
                    value.java_to_json(true).unwrap(),
                    case["staticPrettyJson"],
                    "{} static pretty toJson",
                    case["id"]
                );

                let decoded_expected: $type =
                    <$type>::java_from_json(case["decodedJson"].as_str().unwrap()).unwrap();
                let expected = serde_json::to_value(decoded_expected).unwrap();
                let decoded = <$type>::java_decode(native_json.as_bytes()).unwrap();
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    expected,
                    "{} decode",
                    case["id"]
                );
                let from_json = <$type>::java_from_json(&native_json).unwrap();
                assert_eq!(
                    serde_json::to_value(from_json).unwrap(),
                    expected,
                    "{} fromJson",
                    case["id"]
                );
                let list_json = format!("[{native_json}]");
                let decoded_list = <$type>::java_decode_list(list_json.as_bytes()).unwrap();
                assert_eq!(decoded_list.len(), case["decodedListCount"]);
                assert_eq!(
                    serde_json::to_value(&decoded_list[0]).unwrap(),
                    expected,
                    "{} decodeList",
                    case["id"]
                );
            }};
        }

        for case in fixture["inheritedSerializationCases"].as_array().unwrap() {
            if case["id"].as_str().unwrap().starts_with("route-") {
                check_case!(TopicRouteData, case);
            } else {
                check_case!(TopicQueueMappingInfo, case);
            }
        }
    }

    #[test]
    fn inherited_remoting_serialization_null_utilities_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let java = &fixture["inheritedSerializationNullUtilities"];
        assert_eq!(java["decodeNullBytesReturnsNull"], true);
        assert_eq!(java["decodeJsonNullReturnsNull"], true);
        assert_eq!(java["fromJsonNullInputReturnsNull"], true);
        assert_eq!(java["fromJsonJsonNullReturnsNull"], true);
        assert_eq!(java["decodeListNullBytesReturnsNull"], true);
        assert_eq!(java["decodeListJsonNullReturnsNull"], true);
        assert_eq!(java["decodeListNullMemberCount"], 1);
        assert_eq!(java["decodeListHasNullMember"], true);
        assert_eq!(java["mappingDecodeNullBytesReturnsNull"], true);
        assert_eq!(java["mappingDecodeJsonNullReturnsNull"], true);
        assert_eq!(java["mappingFromJsonNullInputReturnsNull"], true);
        assert_eq!(java["mappingFromJsonJsonNullReturnsNull"], true);
        assert_eq!(java["mappingDecodeListNullBytesReturnsNull"], true);
        assert_eq!(java["mappingDecodeListJsonNullReturnsNull"], true);
        assert_eq!(java["mappingDecodeListNullMemberCount"], 1);
        assert_eq!(java["mappingDecodeListHasNullMember"], true);

        assert!(
            TopicRouteData::java_decode_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            TopicRouteData::java_decode_nullable(Some(b"null"))
                .unwrap()
                .is_none()
        );
        assert!(
            TopicRouteData::java_from_json_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            TopicRouteData::java_from_json_nullable(Some("null"))
                .unwrap()
                .is_none()
        );
        assert!(
            TopicRouteData::java_decode_list_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            TopicRouteData::java_decode_list_nullable(Some(b"null"))
                .unwrap()
                .is_none()
        );
        let decoded = TopicRouteData::java_decode_list_nullable(Some(b"[null]"))
            .unwrap()
            .unwrap();
        assert_eq!(decoded.len(), 1);
        assert!(decoded[0].is_none());

        assert!(
            TopicQueueMappingInfo::java_decode_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            TopicQueueMappingInfo::java_decode_nullable(Some(b"null"))
                .unwrap()
                .is_none()
        );
        assert!(
            TopicQueueMappingInfo::java_from_json_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            TopicQueueMappingInfo::java_from_json_nullable(Some("null"))
                .unwrap()
                .is_none()
        );
        assert!(
            TopicQueueMappingInfo::java_decode_list_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            TopicQueueMappingInfo::java_decode_list_nullable(Some(b"null"))
                .unwrap()
                .is_none()
        );
        let decoded = TopicQueueMappingInfo::java_decode_list_nullable(Some(b"[null]"))
            .unwrap()
            .unwrap();
        assert_eq!(decoded.len(), 1);
        assert!(decoded[0].is_none());
    }
}
