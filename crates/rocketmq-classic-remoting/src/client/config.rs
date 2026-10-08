use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};

use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

use super::TransportMode;
use crate::error::TransportError;

/// SOCKS5 proxy settings corresponding to Java's `SocksProxyConfig`.
#[derive(Clone, Debug, Default, Deserialize, serde::Serialize, PartialEq, Eq)]
pub struct SocksProxyConfig {
    #[serde(rename = "addr")]
    pub address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
}

// Java keeps malformed proxy entries until a matching route is connected.
// Keep those cases private so the public, address-bearing config stays honest.
#[derive(Clone, Debug)]
pub(super) enum ParsedProxyRoute {
    Direct,
    Proxy(SocksProxyConfig),
    MissingAddress,
}

impl From<SocksProxyConfig> for ParsedProxyRoute {
    fn from(value: SocksProxyConfig) -> Self {
        Self::Proxy(value)
    }
}

#[derive(Clone, Debug)]
enum FastjsonValue {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Array(Vec<Self>),
    Object(Vec<(String, Self)>),
}

impl FastjsonValue {
    fn java_string(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::Null => "null".into(),
            Self::Bool(value) => value.to_string(),
            Self::Number(value) => java_number_string(value),
            Self::Array(_) | Self::Object(_) => {
                let mut output = String::new();
                self.write_compact_json(&mut output);
                output
            }
        }
    }

    fn write_compact_json(&self, output: &mut String) {
        match self {
            Self::Null => output.push_str("null"),
            Self::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
            Self::Number(value) => output.push_str(&java_number_string(value)),
            Self::String(value) => write_json_string(value, output),
            Self::Array(values) => {
                output.push('[');
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    value.write_compact_json(output);
                }
                output.push(']');
            }
            Self::Object(fields) => {
                output.push('{');
                for (index, (key, value)) in fields.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    write_json_string(key, output);
                    output.push(':');
                    value.write_compact_json(output);
                }
                output.push('}');
            }
        }
    }
}

thread_local! {
    static FASTJSON_NUMBER_TOKENS: RefCell<VecDeque<String>> = const { RefCell::new(VecDeque::new()) };
}

struct FastjsonNumberTokenScope(VecDeque<String>);

impl FastjsonNumberTokenScope {
    fn install(tokens: VecDeque<String>) -> Self {
        let previous = FASTJSON_NUMBER_TOKENS
            .with(|current| std::mem::replace(&mut *current.borrow_mut(), tokens));
        Self(previous)
    }
}

impl Drop for FastjsonNumberTokenScope {
    fn drop(&mut self) {
        FASTJSON_NUMBER_TOKENS.with(|current| {
            *current.borrow_mut() = std::mem::take(&mut self.0);
        });
    }
}

fn take_fastjson_number_token<E: serde::de::Error>() -> Result<String, E> {
    FASTJSON_NUMBER_TOKENS.with(|tokens| {
        tokens
            .borrow_mut()
            .pop_front()
            .ok_or_else(|| E::custom("missing Fastjson numeric token"))
    })
}

fn java_number_string(token: &str) -> String {
    if let Some((number, suffix)) = token.split_at_checked(token.len().saturating_sub(1)) {
        match suffix.as_bytes().first().copied() {
            Some(b'd' | b'D') => {
                if let Some(value) = parse_fastjson_float(number) {
                    return if value.is_nan() {
                        "NaN".into()
                    } else if value.is_infinite() {
                        if value.is_sign_negative() {
                            "-Infinity".into()
                        } else {
                            "Infinity".into()
                        }
                    } else if value == 0.0 {
                        "0.0".into()
                    } else {
                        java_double_string(value)
                    };
                }
            }
            Some(b'f' | b'F') => {
                if let Some(value) = parse_fastjson_float(number).map(|value| value as f32) {
                    return java_float_string(value);
                }
            }
            Some(b'l' | b'L') => {
                return canonical_integer_string(number);
            }
            _ => {}
        }
    }

    if token.contains(['e', 'E']) {
        let value = parse_fastjson_float(token).unwrap_or(f64::NAN);
        if value.is_nan() {
            return token.to_owned();
        }
        if value.is_infinite() {
            return if value.is_sign_negative() {
                "-Infinity".into()
            } else {
                "Infinity".into()
            };
        }
        if value == 0.0 {
            return "0.0".into();
        }
        return java_double_string(value);
    }

    let (negative, unsigned) = match token.as_bytes().first() {
        Some(b'-') => (true, &token[1..]),
        Some(b'+') => (false, &token[1..]),
        _ => (false, token),
    };
    let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let integer = integer.trim_start_matches('0');
    let integer = if integer.is_empty() { "0" } else { integer };
    let is_zero = integer == "0" && fraction.bytes().all(|digit| digit == b'0');
    let mut output = String::new();
    if negative && !is_zero {
        output.push('-');
    }
    output.push_str(integer);
    if token.contains('.') {
        output.push('.');
        if fraction.is_empty() {
            output.push('0');
        } else {
            output.push_str(fraction);
        }
    }
    output
}

fn canonical_integer_string(token: &str) -> String {
    let (negative, digits) = match token.as_bytes().first() {
        Some(b'-') => (true, &token[1..]),
        Some(b'+') => (false, &token[1..]),
        _ => (false, token),
    };
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        "0".into()
    } else if negative {
        format!("-{digits}")
    } else {
        digits.to_owned()
    }
}

fn java_float_string(value: f32) -> String {
    if value.is_nan() {
        return "NaN".into();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-Infinity".into()
        } else {
            "Infinity".into()
        };
    }
    if value == 0.0 {
        return "0.0".into();
    }

    let rendered = value.abs().to_string();
    let (mantissa, exponent) = rendered
        .split_once('e')
        .or_else(|| rendered.split_once('E'))
        .map_or((rendered.as_str(), 0), |(mantissa, exponent)| {
            (mantissa, exponent.parse::<i32>().unwrap_or(0))
        });
    let decimal_position = mantissa.find('.').unwrap_or(mantissa.len()) as i32;
    let mut digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let first_significant = digits
        .bytes()
        .position(|digit| digit != b'0')
        .expect("nonzero float has a significant digit");
    let decimal_exponent = decimal_position + exponent - first_significant as i32 - 1;
    digits.drain(..first_significant);
    while digits.ends_with('0') {
        digits.pop();
    }

    let mut output = String::new();
    if value.is_sign_negative() {
        output.push('-');
    }
    if !(-3..7).contains(&decimal_exponent) {
        output.push(digits.as_bytes()[0] as char);
        output.push('.');
        if digits.len() == 1 {
            output.push('0');
        } else {
            output.push_str(&digits[1..]);
        }
        output.push('E');
        output.push_str(&decimal_exponent.to_string());
        return output;
    }

    let decimal_position = decimal_exponent + 1;
    if decimal_position <= 0 {
        output.push_str("0.");
        output.extend(std::iter::repeat_n('0', (-decimal_position) as usize));
        output.push_str(&digits);
    } else if decimal_position as usize >= digits.len() {
        output.push_str(&digits);
        output.extend(std::iter::repeat_n(
            '0',
            decimal_position as usize - digits.len(),
        ));
        output.push_str(".0");
    } else {
        let decimal_position = decimal_position as usize;
        output.push_str(&digits[..decimal_position]);
        output.push('.');
        output.push_str(&digits[decimal_position..]);
    }
    output
}

fn parse_fastjson_float(token: &str) -> Option<f64> {
    let token = token.strip_prefix('+').unwrap_or(token);
    let exponent_position = token.find(['e', 'E']).unwrap_or(token.len());
    let (mantissa, exponent) = token.split_at(exponent_position);
    let (sign, unsigned) = match mantissa.as_bytes().first() {
        Some(b'-') => ("-", &mantissa[1..]),
        _ => ("", mantissa),
    };
    let normalized_mantissa = if let Some(fraction) = unsigned.strip_prefix('.') {
        format!("{sign}0.{fraction}")
    } else if unsigned.ends_with('.') {
        format!("{sign}{unsigned}0")
    } else {
        mantissa.to_owned()
    };
    format!("{normalized_mantissa}{exponent}").parse().ok()
}

fn java_double_string(value: f64) -> String {
    let rendered = serde_json::Number::from_f64(value.abs())
        .expect("finite float is a JSON number")
        .to_string();
    let (mantissa, exponent) = rendered
        .split_once('e')
        .or_else(|| rendered.split_once('E'))
        .map_or((rendered.as_str(), 0), |(mantissa, exponent)| {
            (mantissa, exponent.parse::<i32>().unwrap_or(0))
        });
    let decimal_position = mantissa.find('.').unwrap_or(mantissa.len()) as i32;
    let mut digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let first_significant = digits
        .bytes()
        .position(|digit| digit != b'0')
        .expect("nonzero float has a significant digit");
    let decimal_exponent = decimal_position + exponent - first_significant as i32 - 1;
    digits.drain(..first_significant);
    while digits.ends_with('0') {
        digits.pop();
    }

    let mut output = String::new();
    if value.is_sign_negative() {
        output.push('-');
    }
    if !(-3..7).contains(&decimal_exponent) {
        output.push(digits.as_bytes()[0] as char);
        output.push('.');
        if digits.len() == 1 {
            output.push('0');
        } else {
            output.push_str(&digits[1..]);
        }
        output.push('E');
        output.push_str(&decimal_exponent.to_string());
        return output;
    }

    let decimal_position = decimal_exponent + 1;
    if decimal_position <= 0 {
        output.push_str("0.");
        output.extend(std::iter::repeat_n('0', (-decimal_position) as usize));
        output.push_str(&digits);
    } else if decimal_position as usize >= digits.len() {
        output.push_str(&digits);
        output.extend(std::iter::repeat_n(
            '0',
            decimal_position as usize - digits.len(),
        ));
        output.push_str(".0");
    } else {
        let decimal_position = decimal_position as usize;
        output.push_str(&digits[..decimal_position]);
        output.push('.');
        output.push_str(&digits[decimal_position..]);
    }
    output
}

fn write_json_string(value: &str, output: &mut String) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\u{0008}' => output.push_str("\\b"),
            '\u{000c}' => output.push_str("\\f"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '\u{0000}'..='\u{001f}' => {
                let byte = character as u8;
                output.push_str("\\u00");
                output.push(HEX[(byte >> 4) as usize] as char);
                output.push(HEX[(byte & 0x0f) as usize] as char);
            }
            other => output.push(other),
        }
    }
    output.push('"');
}

impl<'de> Deserialize<'de> for FastjsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FastjsonValueVisitor;

        impl<'de> Visitor<'de> for FastjsonValueVisitor {
            type Value = FastjsonValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON value")
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(FastjsonValue::Null)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(FastjsonValue::Null)
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
                Ok(FastjsonValue::Bool(value))
            }

            fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(FastjsonValue::Number(take_fastjson_number_token()?))
            }

            fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(FastjsonValue::Number(take_fastjson_number_token()?))
            }

            fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(FastjsonValue::Number(take_fastjson_number_token()?))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
                Ok(FastjsonValue::String(value.to_owned()))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
                Ok(FastjsonValue::String(value))
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element()? {
                    values.push(value);
                }
                Ok(FastjsonValue::Array(values))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut fields: Vec<(String, FastjsonValue)> = Vec::new();
                while let Some((key, value)) = map.next_entry::<String, FastjsonValue>()? {
                    if let Some((_, existing)) = fields.iter_mut().find(|(name, _)| *name == key) {
                        *existing = value;
                    } else {
                        fields.push((key, value));
                    }
                }
                Ok(FastjsonValue::Object(fields))
            }
        }

        deserializer.deserialize_any(FastjsonValueVisitor)
    }
}

#[derive(Deserialize)]
#[serde(transparent)]
struct RawProxyRoute(HashMap<String, Option<FastjsonValue>>);

impl RawProxyRoute {
    fn java_string_field(&self, name: &str) -> Option<String> {
        self.0
            .get(name)
            .and_then(Option::as_ref)
            .map(FastjsonValue::java_string)
    }
}

fn contains_fastjson_rejected_literal(input: &str) -> bool {
    let bytes = input.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        // Fastjson accepts JSON's ASCII whitespace set but rejects the
        // Unicode line separator that JSON5 treats as whitespace.
        if bytes[index..].starts_with(&[0xe2, 0x80, 0xa8]) {
            return true;
        }
        match bytes[index] {
            b'"' | b'\'' => {
                let quote = bytes[index];
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == b'\\' {
                        let Some(&escaped) = bytes.get(index + 1) else {
                            return true;
                        };
                        match escaped {
                            b'"' | b'\'' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't'
                            | b'v' | b'0' => index += 2,
                            b'x' if fastjson_escape_digits(bytes, index, quote, 2).is_some() => {
                                index += 4;
                            }
                            b'u' if fastjson_escape_digits(bytes, index, quote, 4).is_some() => {
                                index += 6;
                            }
                            escaped if fastjson_additional_escape_code_unit(escaped).is_some() => {
                                index += 2;
                            }
                            _ => return true,
                        }
                    } else if bytes[index] == quote {
                        index += 1;
                        break;
                    } else {
                        index += 1;
                    }
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                let mut previous = index;
                while previous > 0 && bytes[previous - 1].is_ascii_whitespace() {
                    previous -= 1;
                }
                if previous > 0 && bytes[previous - 1] == b':' {
                    return true;
                }
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                let mut previous = index;
                while previous > 0 && bytes[previous - 1].is_ascii_whitespace() {
                    previous -= 1;
                }
                if previous > 0 && bytes[previous - 1] == b':' {
                    return true;
                }
                index += 2;
                while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    index += 1;
                }
                index = (index + 2).min(bytes.len());
            }
            b'\x0b' => return true,
            b'.' if bytes.get(index + 1).is_some_and(u8::is_ascii_digit) => {
                let signed = index > 0
                    && matches!(bytes[index - 1], b'+' | b'-')
                    && (index < 2 || !is_number_token_byte(bytes[index - 2]));
                if !signed && (index == 0 || !is_number_token_byte(bytes[index - 1])) {
                    return true;
                }
                index += 1;
            }
            b'0' if (index == 0 || !is_number_token_byte(bytes[index - 1]))
                && matches!(bytes.get(index + 1), Some(b'x' | b'X'))
                && bytes.get(index + 2).is_some_and(u8::is_ascii_hexdigit) =>
            {
                let mut end = index + 2;
                while bytes.get(end).is_some_and(u8::is_ascii_hexdigit) {
                    end += 1;
                }
                if bytes
                    .get(end)
                    .is_none_or(|byte| !is_number_token_byte(*byte))
                {
                    let next = skip_json5_trivia(bytes, end);
                    if bytes.get(next) != Some(&b':') {
                        return true;
                    }
                }
                index = end;
            }
            b'a'..=b'z' | b'A'..=b'Z' | b'_' | b'$'
                if index == 0 || !is_identifier_token_byte(bytes[index - 1]) =>
            {
                let start = index;
                index += 1;
                while bytes
                    .get(index)
                    .is_some_and(|byte| is_identifier_token_byte(*byte))
                {
                    index += 1;
                }
                let token = &bytes[start..index];
                let next = skip_json5_trivia(bytes, index);
                if bytes.get(next) == Some(&b':')
                    || matches!(token, b"NaN" | b"Infinity" | b"undefined")
                {
                    return true;
                }
            }
            _ => index += 1,
        }
    }
    false
}

fn is_number_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$' | b'.')
}

fn is_identifier_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$')
}

fn skip_json5_trivia(bytes: &[u8], mut index: usize) -> usize {
    loop {
        while bytes.get(index).is_some_and(u8::is_ascii_whitespace) {
            index += 1;
        }
        if bytes.get(index..index + 2) == Some(b"//") {
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
        } else if bytes.get(index..index + 2) == Some(b"/*") {
            index += 2;
            while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/') {
                index += 1;
            }
            index = (index + 2).min(bytes.len());
        } else {
            return index;
        }
    }
}

fn normalize_fastjson_source(input: &str) -> String {
    // Fastjson drops line separators from line comments and permits them in strings,
    // while Rust Strings cannot hold isolated UTF-16 surrogate code units.
    let bytes = input.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes.get(index..index + 2) == Some(b"//") {
            output.extend_from_slice(b"//");
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                if bytes[index] == b'\r'
                    || bytes[index..].starts_with(&[0xe2, 0x80, 0xa8])
                    || bytes[index..].starts_with(&[0xe2, 0x80, 0xa9])
                {
                    index += if bytes[index] == b'\r' { 1 } else { 3 };
                } else {
                    output.push(bytes[index]);
                    index += 1;
                }
            }
            continue;
        }
        if bytes.get(index..index + 2) == Some(b"/*") {
            let start = index;
            index += 2;
            while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/') {
                index += 1;
            }
            if index + 1 < bytes.len() {
                index += 2;
            } else {
                index = bytes.len();
            }
            output.extend_from_slice(&bytes[start..index]);
            continue;
        }
        if !matches!(bytes[index], b'"' | b'\'') {
            output.push(bytes[index]);
            index += 1;
            continue;
        }
        let quote = bytes[index];
        output.push(quote);
        index += 1;
        while index < bytes.len() {
            if bytes[index] == b'\\' {
                if let Some((code_unit, end)) = fastjson_escape_code_unit(bytes, index, quote, b'u')
                {
                    if (0xd800..=0xdbff).contains(&code_unit) {
                        match fastjson_escape_code_unit(bytes, end, quote, b'u') {
                            Some((low, paired_end)) if (0xdc00..=0xdfff).contains(&low) => {
                                push_json_unicode_escape(&mut output, code_unit);
                                push_json_unicode_escape(&mut output, low);
                                index = paired_end;
                            }
                            _ => {
                                output.extend_from_slice(b"\\uFFFD");
                                index = end;
                            }
                        }
                    } else if (0xdc00..=0xdfff).contains(&code_unit) {
                        output.extend_from_slice(b"\\uFFFD");
                        index = end;
                    } else {
                        push_json_unicode_escape(&mut output, code_unit);
                        index = end;
                    }
                } else if let Some((code_unit, end)) =
                    fastjson_escape_code_unit(bytes, index, quote, b'x')
                {
                    push_json_unicode_escape(&mut output, code_unit);
                    index = end;
                } else if let Some((escaped, code_unit)) =
                    bytes.get(index + 1).copied().and_then(|escaped| {
                        fastjson_additional_escape_code_unit(escaped)
                            .map(|code_unit| (escaped, code_unit))
                    })
                {
                    if fastjson_identity_escape(escaped) {
                        output.push(escaped);
                    } else {
                        push_json_unicode_escape(&mut output, code_unit);
                    }
                    index += 2;
                } else {
                    let end = (index + 2).min(bytes.len());
                    output.extend_from_slice(&bytes[index..end]);
                    index = end;
                }
            } else if bytes[index..].starts_with(&[0xe2, 0x80, 0xa8]) {
                output.extend_from_slice(b"\\u2028");
                index += 3;
            } else {
                let byte = bytes[index];
                output.push(byte);
                index += 1;
                if byte == quote {
                    break;
                }
            }
        }
    }
    String::from_utf8(output).expect("source normalization preserves valid UTF-8")
}

fn fastjson_escape_digits(bytes: &[u8], start: usize, quote: u8, count: usize) -> Option<usize> {
    let digits_start = start.checked_add(2)?;
    let end = digits_start.checked_add(count)?;
    let digits = bytes.get(digits_start..end)?;
    (!digits
        .iter()
        .any(|byte| matches!(*byte, b'\n' | b'\r') || *byte == quote))
    .then_some(end)
}

fn fastjson_escape_code_unit(
    bytes: &[u8],
    start: usize,
    quote: u8,
    marker: u8,
) -> Option<(u16, usize)> {
    if bytes.get(start) != Some(&b'\\') || bytes.get(start + 1) != Some(&marker) {
        return None;
    }
    let count = if marker == b'u' { 4 } else { 2 };
    let end = fastjson_escape_digits(bytes, start, quote, count)?;
    let digits = &bytes[start + 2..end];
    let mut value = 0u16;
    for digit in digits {
        value = value
            .wrapping_shl(4)
            .wrapping_add(u16::from(fastjson_hex_digit_or_zero(*digit)));
    }
    Some((value, end))
}

fn fastjson_hex_digit_or_zero(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => 0,
    }
}

fn fastjson_additional_escape_code_unit(escaped: u8) -> Option<u16> {
    match escaped {
        b'1'..=b'7' => Some(u16::from(escaped - b'0')),
        b'F' => Some(0x000c),
        byte if fastjson_identity_escape(byte) => Some(u16::from(byte)),
        _ => None,
    }
}

fn fastjson_identity_escape(escaped: u8) -> bool {
    matches!(
        escaped,
        b' ' | b'#' | b'&' | b'(' | b')' | b',' | b'.' | b'@' | b'[' | b']' | b'_' | b'~'
    )
}

fn push_json_unicode_escape(output: &mut Vec<u8>, value: u16) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    output.extend_from_slice(b"\\u");
    output.push(HEX[((value >> 12) & 0x0f) as usize]);
    output.push(HEX[((value >> 8) & 0x0f) as usize]);
    output.push(HEX[((value >> 4) & 0x0f) as usize]);
    output.push(HEX[(value & 0x0f) as usize]);
}

fn recover_fastjson_eof_line_comment(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut index = 0;
    let mut closing = Vec::new();
    while index < bytes.len() {
        match bytes[index] {
            b'"' | b'\'' => {
                let quote = bytes[index];
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == b'\\' {
                        index = (index + 2).min(bytes.len());
                    } else if bytes[index] == quote {
                        index += 1;
                        break;
                    } else {
                        index += 1;
                    }
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                let comment_start = index;
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
                if index == bytes.len() {
                    if closing.len() > 1 {
                        // Fastjson tolerates an unfinished root object here, but not a nested proxy object.
                        return input.to_owned();
                    }
                    let mut output =
                        String::from_utf8(bytes[..comment_start].to_vec()).expect("valid UTF-8");
                    for delimiter in closing.iter().rev() {
                        output.push(*delimiter as char);
                    }
                    return output;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    index += 1;
                }
                index = (index + 2).min(bytes.len());
            }
            b'{' => {
                closing.push(b'}');
                index += 1;
            }
            b'[' => {
                closing.push(b']');
                index += 1;
            }
            b'}' | b']' => {
                if closing.pop() != Some(bytes[index]) {
                    return input.to_owned();
                }
                index += 1;
            }
            _ => index += 1,
        }
    }
    input.to_owned()
}

fn normalize_fastjson_numbers(input: &str) -> (String, VecDeque<String>) {
    let bytes = input.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut tokens = VecDeque::new();
    let mut copied_through = 0;
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'"' | b'\'' => {
                let quote = bytes[index];
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == b'\\' {
                        index = (index + 2).min(bytes.len());
                    } else if bytes[index] == quote {
                        index += 1;
                        break;
                    } else {
                        index += 1;
                    }
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    index += 1;
                }
                index = (index + 2).min(bytes.len());
            }
            _ => {
                let Some(end) = decimal_number_end(bytes, index) else {
                    index += 1;
                    continue;
                };
                if index > 0 && is_number_token_byte(bytes[index - 1]) {
                    index = end;
                    continue;
                }
                let next = skip_json5_trivia(bytes, end);
                if bytes.get(next) == Some(&b':') {
                    index = end;
                    continue;
                }
                tokens.push_back(String::from_utf8_lossy(&bytes[index..end]).into_owned());
                output.extend_from_slice(&bytes[copied_through..index]);
                output.push(b'0');
                copied_through = end;
                index = end;
            }
        }
    }
    output.extend_from_slice(&bytes[copied_through..]);
    (
        String::from_utf8(output).expect("normalization preserves valid UTF-8"),
        tokens,
    )
}

fn decimal_number_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut index = start;
    if matches!(bytes.get(index), Some(b'+' | b'-')) {
        if index > 0 && is_number_token_byte(bytes[index - 1]) {
            return None;
        }
        index += 1;
    } else if index > 0 && is_number_token_byte(bytes[index - 1]) {
        return None;
    }

    if !matches!(bytes.get(index), Some(b'0'..=b'9' | b'.')) {
        return None;
    }
    if bytes.get(index) == Some(&b'0')
        && matches!(
            bytes.get(index + 1),
            Some(b'x' | b'X' | b'b' | b'B' | b'o' | b'O')
        )
    {
        return None;
    }

    let mut digits = 0;
    while bytes.get(index).is_some_and(u8::is_ascii_digit) {
        digits += 1;
        index += 1;
    }
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            digits += 1;
            index += 1;
        }
    }
    if digits == 0 {
        return None;
    }
    if matches!(bytes.get(index), Some(b'e' | b'E')) {
        let exponent = index;
        index += 1;
        if matches!(bytes.get(index), Some(b'+' | b'-')) {
            index += 1;
        }
        let exponent_start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if exponent_start == index {
            return Some(exponent);
        }
    }
    if matches!(
        bytes.get(index),
        Some(b'd' | b'D' | b'f' | b'F' | b'l' | b'L')
    ) && !bytes
        .get(index + 1)
        .is_some_and(|byte| is_number_token_byte(*byte))
    {
        if matches!(bytes[index], b'l' | b'L')
            && bytes[start..index]
                .iter()
                .any(|byte| matches!(byte, b'.' | b'e' | b'E'))
        {
            return None;
        }
        return Some(index + 1);
    }
    if bytes
        .get(index)
        .is_some_and(|byte| is_number_token_byte(*byte))
    {
        return None;
    }
    Some(index)
}

impl SocksProxyConfig {
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            username: None,
            password: None,
        }
    }

    pub fn with_credentials(
        address: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            address: address.into(),
            username: Some(username.into()),
            password: Some(password.into()),
        }
    }
}

/// Client controls mapped from Java `NettyClientConfig` settings used by this transport.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub transport: TransportMode,
    /// Java `connectTimeoutMillis`; negative values leave Netty's 30-second default in effect.
    pub connect_timeout: i32,
    /// Java `channelNotActiveInterval` in milliseconds; inert in the pinned client's normal flow.
    pub channel_not_active_interval: i64,
    pub async_semaphore_value: usize,
    pub oneway_semaphore_value: usize,
    pub worker_threads: usize,
    pub callback_executor_threads: usize,
    pub disable_callback_executor: bool,
    pub client_socket_send_buffer_size: usize,
    pub client_socket_receive_buffer_size: usize,
    pub write_buffer_high_water_mark: usize,
    pub write_buffer_low_water_mark: usize,
    pub pooled_byte_buffer_allocator_enabled: bool,
    pub disable_netty_worker_group: bool,
    /// Java `clientChannelMaxIdleTimeSeconds`; non-positive values disable idle detection.
    pub max_idle_time: i32,
    pub close_socket_if_timeout: bool,
    pub scan_available_nameserver: bool,
    pub enable_reconnect_for_go_away: bool,
    /// Java `long maxReconnectIntervalTimeSeconds`; retained but unused by the pinned client.
    pub max_reconnect_interval_time_seconds: i64,
    /// Java `NettyClientConfig.socksProxyConfig` JSON text, keyed by IPv4 CIDR.
    pub socks_proxy_config: String,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            transport: TransportMode::Plain,
            connect_timeout: 3000,
            channel_not_active_interval: 60_000,
            async_semaphore_value: 65535,
            oneway_semaphore_value: 65535,
            worker_threads: 4,
            callback_executor_threads: std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1),
            disable_callback_executor: false,
            client_socket_send_buffer_size: 0,
            client_socket_receive_buffer_size: 0,
            write_buffer_high_water_mark: 0,
            write_buffer_low_water_mark: 0,
            pooled_byte_buffer_allocator_enabled: false,
            disable_netty_worker_group: false,
            max_idle_time: 120,
            close_socket_if_timeout: true,
            scan_available_nameserver: true,
            enable_reconnect_for_go_away: true,
            max_reconnect_interval_time_seconds: 60,
            socks_proxy_config: "{}".into(),
        }
    }
}

impl ClientConfig {
    pub(super) fn connect_timeout_duration(&self) -> std::time::Duration {
        if self.connect_timeout < 0 {
            std::time::Duration::from_secs(30)
        } else {
            std::time::Duration::from_millis(self.connect_timeout as u64)
        }
    }

    pub(super) fn scan_interval(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.connect_timeout.max(100) as u64)
    }

    pub(super) fn max_idle_time_duration(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.max_idle_time.max(0) as u64)
    }

    pub fn validate(&self) -> Result<(), TransportError> {
        if self.worker_threads == 0
            || i32::try_from(self.worker_threads).is_err()
            || i32::try_from(self.callback_executor_threads).is_err()
            || i32::try_from(self.async_semaphore_value).is_err()
            || i32::try_from(self.oneway_semaphore_value).is_err()
            || self.async_semaphore_value > tokio::sync::Semaphore::MAX_PERMITS
            || self.oneway_semaphore_value > tokio::sync::Semaphore::MAX_PERMITS
            || i32::try_from(self.client_socket_send_buffer_size).is_err()
            || i32::try_from(self.client_socket_receive_buffer_size).is_err()
            || i32::try_from(self.write_buffer_high_water_mark).is_err()
            || i32::try_from(self.write_buffer_low_water_mark).is_err()
        {
            return Err(TransportError::InvalidConfiguration(
                "integer client settings must fit their Java signed integer ranges; worker count must be nonzero"
                    .into(),
            ));
        }
        if self.write_buffer_low_water_mark > 0
            && self.write_buffer_high_water_mark > 0
            && self.write_buffer_low_water_mark > self.write_buffer_high_water_mark
        {
            return Err(TransportError::InvalidConfiguration(
                "write buffer low water mark must not exceed high water mark when both are enabled"
                    .into(),
            ));
        }
        Ok(())
    }

    pub(super) fn write_buffer_watermarks(&self) -> (usize, usize) {
        if self.write_buffer_low_water_mark > 0 && self.write_buffer_high_water_mark > 0 {
            (
                self.write_buffer_high_water_mark,
                self.write_buffer_low_water_mark,
            )
        } else {
            (64 * 1024, 32 * 1024)
        }
    }

    /// Java-compatible getter/setter behavior: retain JSON text until client construction.
    pub fn get_socks_proxy_config(&self) -> &str {
        &self.socks_proxy_config
    }

    pub fn set_socks_proxy_config(&mut self, json: impl Into<String>) {
        self.socks_proxy_config = json.into();
    }

    pub(super) fn parse_socks_proxy_config(
        &self,
    ) -> Result<HashMap<String, ParsedProxyRoute>, TransportError> {
        if self.socks_proxy_config.is_empty() || self.socks_proxy_config.trim() == "null" {
            return Ok(HashMap::new());
        }
        if contains_fastjson_rejected_literal(&self.socks_proxy_config) {
            return Err(TransportError::InvalidConfiguration(
                "Fastjson SOCKS parser rejects this JSON5 literal".into(),
            ));
        }
        let string_normalized = normalize_fastjson_source(&self.socks_proxy_config);
        let recovered = recover_fastjson_eof_line_comment(&string_normalized);
        let (normalized, number_tokens) = normalize_fastjson_numbers(&recovered);
        let _number_scope = FastjsonNumberTokenScope::install(number_tokens);
        let parsed: Option<HashMap<String, Option<RawProxyRoute>>> =
            json5::from_str(&normalized)
                .map_err(|error| TransportError::InvalidConfiguration(error.to_string()))?;
        Ok(parsed
            .unwrap_or_default()
            .into_iter()
            .map(|(cidr, entry)| {
                let route = match entry {
                    None => ParsedProxyRoute::Direct,
                    Some(raw) => match raw.java_string_field("addr") {
                        Some(address) => ParsedProxyRoute::Proxy(SocksProxyConfig {
                            address,
                            username: raw.java_string_field("username"),
                            password: raw.java_string_field("password"),
                        }),
                        None => ParsedProxyRoute::MissingAddress,
                    },
                };
                (cidr, route)
            })
            .collect())
    }

    pub(super) fn proxy_for<T: Clone + Into<ParsedProxyRoute>>(
        routes: &HashMap<String, T>,
        host: &str,
    ) -> Result<Option<SocksProxyConfig>, TransportError> {
        for (cidr, route) in routes {
            if cidr == "0.0.0.0/0" || ipv4_in_cidr(host, cidr)? {
                return match route.clone().into() {
                    ParsedProxyRoute::Direct => Ok(None),
                    ParsedProxyRoute::Proxy(proxy) => Ok(Some(proxy)),
                    ParsedProxyRoute::MissingAddress => Err(TransportError::InvalidConfiguration(
                        "matched SOCKS proxy has no address".into(),
                    )),
                };
            }
        }
        Ok(None)
    }
}

fn ipv4_in_cidr(address: &str, cidr: &str) -> Result<bool, TransportError> {
    let address = java_ip_to_int(address)?;
    let mut parts = cidr.split('/');
    let network = parts.next().unwrap_or_default();
    let prefix = parts.next().ok_or_else(|| {
        TransportError::InvalidConfiguration(format!("invalid SOCKS proxy CIDR: {cidr}"))
    })?;
    let network = java_ip_to_int(network)?;
    let prefix = prefix.parse::<i32>().map_err(|error| {
        TransportError::InvalidConfiguration(format!("invalid SOCKS proxy CIDR {cidr}: {error}"))
    })?;
    // Java int shifts mask the shift distance to five bits, including /0 and out-of-range prefixes.
    let mask = (-1_i32).wrapping_shl(32_i32.wrapping_sub(prefix) as u32);
    Ok((address & mask) == (network & mask))
}

fn java_ip_to_int(ip: &str) -> Result<i32, TransportError> {
    // Java's ipToInt reads the first four split fields and ignores any extras.
    let mut address = 0_i32;
    let mut octets = ip.split('.');
    for index in 0..4 {
        let octet = octets.next().ok_or_else(|| {
            TransportError::InvalidConfiguration(format!(
                "invalid IPv4 address in SOCKS proxy CIDR: {ip}"
            ))
        })?;
        let value = octet.parse::<i32>().map_err(|error| {
            TransportError::InvalidConfiguration(format!(
                "invalid IPv4 address in SOCKS proxy CIDR {ip}: {error}"
            ))
        })?;
        address |= value.wrapping_shl((24 - index * 8) as u32);
    }
    Ok(address)
}

#[cfg(test)]
mod tests {
    use super::{ClientConfig, SocksProxyConfig};

    #[test]
    fn signed_time_settings_match_java_normalization() {
        let defaults = ClientConfig::default();
        assert_eq!(
            defaults.connect_timeout_duration(),
            std::time::Duration::from_secs(3)
        );
        assert_eq!(defaults.scan_interval(), std::time::Duration::from_secs(3));

        let negative = ClientConfig {
            connect_timeout: -1,
            max_idle_time: -1,
            channel_not_active_interval: -1,
            ..defaults
        };
        assert!(negative.validate().is_ok());
        assert_eq!(
            negative.connect_timeout_duration(),
            std::time::Duration::from_secs(30)
        );
        assert_eq!(
            negative.scan_interval(),
            std::time::Duration::from_millis(100)
        );
        assert_eq!(negative.max_idle_time_duration(), std::time::Duration::ZERO);
    }

    #[test]
    fn write_buffer_watermarks_match_java_enablement() {
        let defaults = ClientConfig::default();
        assert_eq!(defaults.write_buffer_watermarks(), (64 * 1024, 32 * 1024));

        let low_only = ClientConfig {
            write_buffer_low_water_mark: 10,
            ..defaults.clone()
        };
        assert!(low_only.validate().is_ok());
        assert_eq!(low_only.write_buffer_watermarks(), (64 * 1024, 32 * 1024));

        let high_only = ClientConfig {
            write_buffer_high_water_mark: 128,
            ..defaults.clone()
        };
        assert!(high_only.validate().is_ok());
        assert_eq!(high_only.write_buffer_watermarks(), (64 * 1024, 32 * 1024));

        let enabled = ClientConfig {
            write_buffer_high_water_mark: 128,
            write_buffer_low_water_mark: 32,
            ..defaults.clone()
        };
        assert_eq!(enabled.write_buffer_watermarks(), (128, 32));

        let disabled = ClientConfig {
            write_buffer_high_water_mark: 128,
            write_buffer_low_water_mark: 0,
            ..defaults.clone()
        };
        assert_eq!(disabled.write_buffer_watermarks(), (64 * 1024, 32 * 1024));

        let invalid = ClientConfig {
            write_buffer_high_water_mark: 32,
            write_buffer_low_water_mark: 128,
            ..defaults
        };
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn java_socks_proxy_json_round_trips_java_field_names() {
        let mut config = ClientConfig::default();
        let json = r#"{"10.1.0.0/16":{"addr":"proxy.internal:1080","username":"user","password":"secret"}}"#;
        config.set_socks_proxy_config(json);
        assert_eq!(config.get_socks_proxy_config(), json);
        assert!(matches!(
            &config.parse_socks_proxy_config().unwrap()["10.1.0.0/16"],
            super::ParsedProxyRoute::Proxy(proxy)
                if proxy == &SocksProxyConfig::with_credentials("proxy.internal:1080", "user", "secret")
        ));
        let malformed = ClientConfig {
            socks_proxy_config: r#"{"broken":{]"#.into(),
            ..ClientConfig::default()
        };
        assert!(malformed.validate().is_ok());
        assert!(malformed.parse_socks_proxy_config().is_err());
        let null_map = ClientConfig {
            socks_proxy_config: "null".into(),
            ..ClientConfig::default()
        };
        assert!(null_map.parse_socks_proxy_config().unwrap().is_empty());
    }

    #[test]
    fn fastjson_literal_guard_ignores_quoted_and_comment_text() {
        assert!(super::contains_fastjson_rejected_literal(
            r#"{"10.0.0.0/8":{"addr":0x2a}}"#
        ));
        assert!(super::contains_fastjson_rejected_literal(
            r#"{"10.0.0.0/8":{"addr":NaN}}"#
        ));
        assert!(super::contains_fastjson_rejected_literal(
            r#"{"10.0.0.0/8":{"addr":Infinity}}"#
        ));
        assert!(super::contains_fastjson_rejected_literal(
            r#"{"10.0.0.0/8":{addr: "proxy:1080"}}"#
        ));
        assert!(!super::contains_fastjson_rejected_literal(
            r#"{"10.0.0.0/8":{"addr":"0x2a"}}"#
        ));
        assert!(!super::contains_fastjson_rejected_literal(
            r#"{"10.0.0.0/8":{'addr':"proxy:1080"}}"#
        ));
        assert!(!super::contains_fastjson_rejected_literal(
            r#"{/* NaN and 0x2a are comment text */"10.0.0.0/8":{"addr":"proxy"}}"#
        ));
        assert!(super::contains_fastjson_rejected_literal(
            "{\u{2028}\"10.0.0.0/8\":{\"addr\":\"proxy\"}}"
        ));
        assert!(!super::contains_fastjson_rejected_literal(
            "{\"10.0.0.0/8\":{\"addr\":\"line\u{2028}separator\"}}"
        ));
        assert!(super::contains_fastjson_rejected_literal(
            "{\u{000b}\"10.0.0.0/8\":{\"addr\":\"proxy\"}}"
        ));
        assert!(!super::contains_fastjson_rejected_literal(
            "{\"10.0.0.0/8\":{\"addr\":\"line\u{000b}separator\"}}"
        ));
        assert!(!super::contains_fastjson_rejected_literal(
            r#"{0x2a:{"addr":"proxy"}}"#
        ));
    }

    #[test]
    fn fastjson_constructor_inputs_preserve_matched_route_behavior() {
        let mut config = ClientConfig::default();
        config.set_socks_proxy_config(
            "{'10.1.0.0/16':{'addr':'proxy:1080'},'10.2.0.0/16':null,'10.3.0.0/16':{}}",
        );
        let routes = config.parse_socks_proxy_config().unwrap();
        assert_eq!(
            ClientConfig::proxy_for(&routes, "10.1.2.3")
                .unwrap()
                .unwrap()
                .address,
            "proxy:1080"
        );
        assert!(
            ClientConfig::proxy_for(&routes, "10.2.2.3")
                .unwrap()
                .is_none()
        );
        assert!(ClientConfig::proxy_for(&routes, "10.3.2.3").is_err());
    }

    #[test]
    fn scalar_socks_fields_match_invoked_java_parsed_proxy_values() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        let cases = fixture["socksConstruction"].as_array().unwrap();
        assert_eq!(
            cases.len(),
            263,
            "Java oracle includes 126 prior cases, 128 ASCII followers, and 9 Unicode escape cases"
        );
        for escaped in 0..=0x7f {
            let mut expected = String::from("{\"10.0.0.0/8\":{\"addr\":\"A\\");
            expected.push(char::from_u32(escaped).unwrap());
            expected.push_str("Z\"}}");
            assert_eq!(
                cases[126 + escaped as usize]["input"],
                expected,
                "ASCII escape follower U+{escaped:04X}"
            );
        }
        let unicode_escapes = [
            "u1λ34", "u12λ4", "u12١4", "u12１4", "u1𝟚34", "xλ0", "x0١", "u12\t4", "u12\n4",
        ];
        for (offset, escaped) in unicode_escapes.iter().enumerate() {
            let expected = format!("{{\"10.0.0.0/8\":{{\"addr\":\"A\\{escaped}Z\"}}}}");
            assert_eq!(
                cases[254 + offset]["input"],
                expected,
                "Unicode escape probe {offset}"
            );
        }

        let mut outcome_mismatches = Vec::new();
        for case in cases.iter().skip(19) {
            let mut config = ClientConfig::default();
            config.set_socks_proxy_config(case["input"].as_str().unwrap());
            let parsed = config.parse_socks_proxy_config();
            let java_accepted = case["outcome"] == "OK";
            if parsed.is_ok() != java_accepted {
                outcome_mismatches.push(format!(
                    "input={:?}, Java={}, Rust={}",
                    case["input"],
                    case["outcome"],
                    if parsed.is_ok() { "OK" } else { "error" }
                ));
                continue;
            }
            if !java_accepted {
                continue;
            }
            let routes = parsed.unwrap();
            let Some(route) = routes.get("10.0.0.0/8") else {
                assert_eq!(case["routePresent"], false, "{}", case["input"]);
                continue;
            };
            assert_eq!(case["routePresent"], true, "{}", case["input"]);
            match route {
                super::ParsedProxyRoute::Proxy(proxy) => {
                    if let Some(code_units) = case["parsedAddrUtf16"].as_array() {
                        let code_units = code_units
                            .iter()
                            .map(|unit| unit.as_u64().unwrap() as u16)
                            .collect::<Vec<_>>();
                        assert_eq!(code_units.len(), 1);
                        assert!((0xd800..=0xdfff).contains(&code_units[0]));
                        // Java String can retain lone surrogates; native String uses U+FFFD.
                        assert_eq!(proxy.address, "\u{fffd}");
                    } else {
                        assert_eq!(Some(proxy.address.as_str()), case["parsedAddr"].as_str());
                    }
                    assert_eq!(proxy.username.as_deref(), case["parsedUsername"].as_str());
                    assert_eq!(proxy.password.as_deref(), case["parsedPassword"].as_str());
                }
                super::ParsedProxyRoute::MissingAddress => {
                    assert!(case["parsedAddr"].is_null());
                }
                super::ParsedProxyRoute::Direct => panic!("Java decoded a nonnull route object"),
            }
        }
        assert!(
            outcome_mismatches.is_empty(),
            "SOCKS parser outcomes differ:\n{}",
            outcome_mismatches.join("\n")
        );
    }

    #[test]
    fn socks_cidr_matching_matches_invoked_java_helper() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        for case in fixture["socksCidr"].as_array().unwrap() {
            let result = super::ipv4_in_cidr(
                case["address"].as_str().unwrap(),
                case["cidr"].as_str().unwrap(),
            );
            assert_eq!(result.is_ok(), case["outcome"] == "OK", "{case}");
            if let Ok(matches) = result {
                assert_eq!(matches, case["match"]);
            }
        }
    }

    #[test]
    fn socks_proxy_selection_matches_invoked_java_remoting_client() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        for case in fixture["socksProxySelection"].as_array().unwrap() {
            let mut config = ClientConfig::default();
            config.set_socks_proxy_config(case["config"].as_str().unwrap());
            let routes = config.parse_socks_proxy_config().unwrap();
            let selected = ClientConfig::proxy_for(&routes, case["host"].as_str().unwrap());
            assert_eq!(selected.is_ok(), case["outcome"] == "OK", "{case}");
            if case["outcome"] != "OK" {
                continue;
            }
            assert_eq!(
                selected.unwrap().map(|proxy| proxy.address).as_deref(),
                case["proxyAddress"].as_str(),
                "{case}"
            );
        }
    }

    #[test]
    fn socks_proxy_routes_match_java_ipv4_cidr_and_error_behavior() {
        let private_route = std::collections::HashMap::from([(
            "10.20.0.0/16".into(),
            SocksProxyConfig::new("private-proxy:1080"),
        )]);
        assert_eq!(
            ClientConfig::proxy_for(&private_route, "10.20.4.5")
                .unwrap()
                .unwrap()
                .address,
            "private-proxy:1080"
        );
        let default_route = std::collections::HashMap::from([(
            "0.0.0.0/0".into(),
            SocksProxyConfig::new("default-proxy:1080"),
        )]);
        assert_eq!(
            ClientConfig::proxy_for(&default_route, "192.0.2.5")
                .unwrap()
                .unwrap()
                .address,
            "default-proxy:1080"
        );
        assert!(
            ClientConfig::proxy_for(&default_route, "broker.example")
                .unwrap()
                .is_some()
        );
        assert!(
            ClientConfig::proxy_for(&default_route, "2001:db8::1")
                .unwrap()
                .is_some()
        );

        let zero_prefix = std::collections::HashMap::from([(
            "10.20.0.0/0".into(),
            SocksProxyConfig::new("zero-prefix-proxy:1080"),
        )]);
        assert!(
            ClientConfig::proxy_for(&zero_prefix, "10.20.4.5")
                .unwrap()
                .is_none()
        );
        assert!(ClientConfig::proxy_for(&private_route, "broker.example").is_err());
        let invalid_cidr = std::collections::HashMap::from([(
            "10.20.0.0/not-a-prefix".into(),
            SocksProxyConfig::new("proxy:1080"),
        )]);
        assert!(ClientConfig::proxy_for(&invalid_cidr, "10.20.4.5").is_err());

        // Java ipToInt parses signed integers and Java int shifts mask shift counts to 0..31.
        let java_shifted_prefix = std::collections::HashMap::from([(
            "10.20.0.0/33".into(),
            SocksProxyConfig::new("shifted-prefix-proxy:1080"),
        )]);
        assert!(
            ClientConfig::proxy_for(&java_shifted_prefix, "10.20.4.5")
                .unwrap()
                .is_some()
        );
        let java_integer_octets = std::collections::HashMap::from([(
            "266.20.0.0/16".into(),
            SocksProxyConfig::new("integer-octet-proxy:1080"),
        )]);
        assert!(
            ClientConfig::proxy_for(&java_integer_octets, "10.20.4.5")
                .unwrap()
                .is_some()
        );
    }
}
