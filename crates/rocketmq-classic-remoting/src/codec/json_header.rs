use std::{
    collections::BTreeMap,
    io::{self, Write},
};

use rocketmq_classic_protocol::codes::{LanguageCode, SerializeType};
use serde_json::{Map, Value};

use crate::{command::RemotingCommand, error::CodecError};

pub fn encode(command: &RemotingCommand, max_length: usize) -> Result<Vec<u8>, CodecError> {
    let mut fields = Map::new();
    fields.insert("code".into(), command.code.into());
    if let Some(language) = command.language {
        let language = match language {
            LanguageCode::Unknown(code) => return Err(CodecError::UnsupportedLanguage(code)),
            language => language_name(language),
        };
        fields.insert("language".into(), language.into());
    }
    fields.insert("version".into(), command.version.into());
    fields.insert("opaque".into(), command.opaque.into());
    fields.insert("flag".into(), command.flag.into());
    if let Some(remark) = &command.remark {
        fields.insert("remark".into(), remark.clone().into());
    }
    if let Some(ext_fields) = &command.ext_fields {
        fields.insert(
            "extFields".into(),
            serde_json::to_value(ext_fields).unwrap(),
        );
    }
    fields.insert(
        "serializeTypeCurrentRPC".into(),
        match command.serialization {
            SerializeType::Json => "JSON",
            SerializeType::RocketMq => "ROCKETMQ",
            SerializeType::Unknown(code) => return Err(CodecError::UnsupportedSerialization(code)),
        }
        .into(),
    );
    let value = Value::Object(fields);
    let mut counter = CountingWriter {
        length: 0,
        limit: max_length,
    };
    serde_json::to_writer(&mut counter, &value)
        .map_err(|_| CodecError::FieldTooWide("JSON header"))?;
    let mut output = Vec::with_capacity(counter.length);
    serde_json::to_writer(&mut output, &value)
        .map_err(|error| CodecError::Json(error.to_string()))?;
    Ok(output)
}

pub fn decode(bytes: &[u8]) -> Result<RemotingCommand, CodecError> {
    let value: Value =
        serde_json::from_slice(bytes).map_err(|error| CodecError::Json(error.to_string()))?;
    let object = value
        .as_object()
        .ok_or_else(|| CodecError::Json("header must be a JSON object".into()))?;
    let code = integer(object.get("code"), "code")?.unwrap_or_default();
    let language = match object.get("language") {
        None => Some(LanguageCode::Java),
        Some(Value::Null) => None,
        Some(Value::String(name)) => Some(
            language_code(name)
                .ok_or_else(|| CodecError::Json(format!("unknown LanguageCode `{name}`")))?,
        ),
        _ => return Err(CodecError::Json("language must be a string".into())),
    };
    let version = integer(object.get("version"), "version")?.unwrap_or_default();
    let opaque = integer(object.get("opaque"), "opaque")?.unwrap_or_default();
    let flag = integer(object.get("flag"), "flag")?.unwrap_or_default();
    let remark = match object.get("remark") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => Some(value.clone()),
        _ => return Err(CodecError::Json("remark must be a string or null".into())),
    };
    let ext_fields = match object.get("extFields") {
        None | Some(Value::Null) => None,
        Some(Value::Object(values)) => {
            let mut fields = BTreeMap::new();
            for (key, value) in values {
                let value = value
                    .as_str()
                    .ok_or_else(|| CodecError::Json(format!("extFields.{key} must be a string")))?;
                fields.insert(key.clone(), value.to_owned());
            }
            Some(fields)
        }
        _ => {
            return Err(CodecError::Json(
                "extFields must be an object or null".into(),
            ));
        }
    };
    let serialization = match object.get("serializeTypeCurrentRPC") {
        None | Some(Value::Null) => SerializeType::Json,
        Some(Value::String(name)) if name == "JSON" => SerializeType::Json,
        Some(Value::String(name)) if name == "ROCKETMQ" => SerializeType::RocketMq,
        Some(Value::String(name)) => {
            return Err(CodecError::Json(format!("unknown serialization `{name}`")));
        }
        Some(_) => {
            return Err(CodecError::Json(
                "serializeTypeCurrentRPC must be a string".into(),
            ));
        }
    };
    Ok(RemotingCommand {
        code,
        language,
        version,
        opaque,
        flag,
        remark,
        ext_fields,
        serialization,
        ..RemotingCommand::default()
    })
}

struct CountingWriter {
    length: usize,
    limit: usize,
}

impl Write for CountingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let length = self
            .length
            .checked_add(bytes.len())
            .filter(|length| *length <= self.limit)
            .ok_or_else(|| io::Error::other("JSON header exceeds configured bound"))?;
        self.length = length;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn integer(value: Option<&Value>, field: &'static str) -> Result<Option<i32>, CodecError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_i64()
            .and_then(|number| i32::try_from(number).ok())
            .map(Some)
            .ok_or_else(|| CodecError::Json(format!("{field} must be a 32-bit integer"))),
    }
}

fn language_name(language: LanguageCode) -> &'static str {
    match language {
        LanguageCode::Java => "JAVA",
        LanguageCode::Cpp => "CPP",
        LanguageCode::Dotnet => "DOTNET",
        LanguageCode::Python => "PYTHON",
        LanguageCode::Delphi => "DELPHI",
        LanguageCode::Erlang => "ERLANG",
        LanguageCode::Ruby => "RUBY",
        LanguageCode::Other => "OTHER",
        LanguageCode::Http => "HTTP",
        LanguageCode::Go => "GO",
        LanguageCode::Php => "PHP",
        LanguageCode::Oms => "OMS",
        LanguageCode::Rust => "RUST",
        LanguageCode::NodeJs => "NODE_JS",
        LanguageCode::Unknown(_) => unreachable!(),
    }
}

fn language_code(name: &str) -> Option<LanguageCode> {
    Some(match name {
        "JAVA" => LanguageCode::Java,
        "CPP" => LanguageCode::Cpp,
        "DOTNET" => LanguageCode::Dotnet,
        "PYTHON" => LanguageCode::Python,
        "DELPHI" => LanguageCode::Delphi,
        "ERLANG" => LanguageCode::Erlang,
        "RUBY" => LanguageCode::Ruby,
        "OTHER" => LanguageCode::Other,
        "HTTP" => LanguageCode::Http,
        "GO" => LanguageCode::Go,
        "PHP" => LanguageCode::Php,
        "OMS" => LanguageCode::Oms,
        "RUST" => LanguageCode::Rust,
        "NODE_JS" => LanguageCode::NodeJs,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_uses_java_field_names_and_language_enum_string() {
        let command = RemotingCommand {
            code: 105,
            ext_fields: Some(BTreeMap::from([("unknown".into(), "kept".into())])),
            ..RemotingCommand::default()
        };
        let encoded = encode(&command, 4096).unwrap();
        let value: Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(value["language"], "RUST");
        assert_eq!(value["extFields"]["unknown"], "kept");
        assert!(value.get("body").is_none());
        assert_eq!(decode(&encoded).unwrap(), command);
    }
}
