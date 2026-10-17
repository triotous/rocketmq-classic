use std::collections::BTreeMap;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI32, Ordering};

use rocketmq_classic_protocol::codes::{JavaEnumLookupError, LanguageCode, SerializeType};

static REQUEST_ID: AtomicI32 = AtomicI32::new(0);
static SERIALIZE_TYPE_CONFIG: OnceLock<Result<SerializeType, String>> = OnceLock::new();

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RemotingCommandType {
    RequestCommand,
    ResponseCommand,
}

impl RemotingCommandType {
    pub const fn values() -> [Self; 2] {
        [Self::RequestCommand, Self::ResponseCommand]
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, JavaEnumLookupError> {
        match name {
            None => Err(JavaEnumLookupError::NullName),
            Some("REQUEST_COMMAND") => Ok(Self::RequestCommand),
            Some("RESPONSE_COMMAND") => Ok(Self::ResponseCommand),
            Some(other) => Err(JavaEnumLookupError::UnknownName {
                enum_class: "org.apache.rocketmq.remoting.protocol.RemotingCommandType",
                name: other.to_owned(),
            }),
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::RequestCommand => "REQUEST_COMMAND",
            Self::ResponseCommand => "RESPONSE_COMMAND",
        }
    }

    pub const fn ordinal(self) -> i32 {
        match self {
            Self::RequestCommand => 0,
            Self::ResponseCommand => 1,
        }
    }

    pub const fn java_compare_to(self, other: Self) -> i32 {
        self.ordinal() - other.ordinal()
    }
}

impl std::fmt::Display for RemotingCommandType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RequestType {
    Stream,
}

impl RequestType {
    pub const fn values() -> [Self; 1] {
        [Self::Stream]
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, JavaEnumLookupError> {
        match name {
            None => Err(JavaEnumLookupError::NullName),
            Some("STREAM") => Ok(Self::Stream),
            Some(other) => Err(JavaEnumLookupError::UnknownName {
                enum_class: "org.apache.rocketmq.remoting.protocol.RequestType",
                name: other.to_owned(),
            }),
        }
    }

    pub const fn from_java_byte(code: i8) -> Option<Self> {
        match code {
            0 => Some(Self::Stream),
            _ => None,
        }
    }

    pub const fn name(self) -> &'static str {
        "STREAM"
    }

    pub const fn ordinal(self) -> i32 {
        0
    }

    pub const fn get_code(self) -> i8 {
        0
    }

    pub const fn java_compare_to(self, _other: Self) -> i32 {
        0
    }
}

impl std::fmt::Display for RequestType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RequestSource {
    Sdk,
    ProxyForOrder,
    ProxyForBroadcast,
    ProxyForStream,
}

impl RequestSource {
    pub const SYSTEM_PROPERTY_KEY: &'static str = "rocketmq.requestSource";

    pub const fn values() -> [Self; 4] {
        [
            Self::Sdk,
            Self::ProxyForOrder,
            Self::ProxyForBroadcast,
            Self::ProxyForStream,
        ]
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, JavaEnumLookupError> {
        match name {
            None => Err(JavaEnumLookupError::NullName),
            Some("SDK") => Ok(Self::Sdk),
            Some("PROXY_FOR_ORDER") => Ok(Self::ProxyForOrder),
            Some("PROXY_FOR_BROADCAST") => Ok(Self::ProxyForBroadcast),
            Some("PROXY_FOR_STREAM") => Ok(Self::ProxyForStream),
            Some(other) => Err(JavaEnumLookupError::UnknownName {
                enum_class: "org.apache.rocketmq.remoting.protocol.RequestSource",
                name: other.to_owned(),
            }),
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Sdk => "SDK",
            Self::ProxyForOrder => "PROXY_FOR_ORDER",
            Self::ProxyForBroadcast => "PROXY_FOR_BROADCAST",
            Self::ProxyForStream => "PROXY_FOR_STREAM",
        }
    }

    pub const fn ordinal(self) -> i32 {
        match self {
            Self::Sdk => 0,
            Self::ProxyForOrder => 1,
            Self::ProxyForBroadcast => 2,
            Self::ProxyForStream => 3,
        }
    }

    pub const fn get_value(self) -> i32 {
        self.ordinal() - 1
    }

    pub const fn is_valid(value: Option<i32>) -> bool {
        matches!(value, Some(-1..=2))
    }

    pub const fn parse_integer(value: Option<i32>) -> Self {
        match value {
            Some(-1) => Self::Sdk,
            Some(0) => Self::ProxyForOrder,
            Some(1) => Self::ProxyForBroadcast,
            Some(2) => Self::ProxyForStream,
            _ => Self::Sdk,
        }
    }

    pub const fn java_compare_to(self, other: Self) -> i32 {
        self.ordinal() - other.ordinal()
    }
}

impl std::fmt::Display for RequestSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemotingCommand {
    pub code: i32,
    pub language: Option<LanguageCode>,
    pub version: i32,
    pub opaque: i32,
    pub flag: i32,
    pub remark: Option<String>,
    pub ext_fields: Option<BTreeMap<String, String>>,
    pub body: Option<Vec<u8>>,
    pub serialization: SerializeType,
    pub suspended: bool,
}

impl Default for RemotingCommand {
    fn default() -> Self {
        Self {
            code: 0,
            language: Some(LanguageCode::Rust),
            version: 0,
            opaque: 0,
            flag: 0,
            remark: None,
            ext_fields: None,
            body: None,
            serialization: Self::get_serialize_type_config_in_this_server()
                .unwrap_or_else(|error| panic!("{error}")),
            suspended: false,
        }
    }
}

impl RemotingCommand {
    pub const SERIALIZE_TYPE_PROPERTY: &'static str = "rocketmq.serialize.type";
    pub const SERIALIZE_TYPE_ENV: &'static str = "ROCKETMQ_SERIALIZE_TYPE";
    pub const REMOTING_VERSION_KEY: &'static str = "rocketmq.remoting.version";

    const RESPONSE_BIT: i32 = 1;
    const ONEWAY_BIT: i32 = 1 << 1;

    pub fn create_new_request_id() -> i32 {
        REQUEST_ID.fetch_add(1, Ordering::Relaxed)
    }

    pub fn get_serialize_type_config_in_this_server() -> Result<SerializeType, String> {
        SERIALIZE_TYPE_CONFIG
            .get_or_init(|| {
                let configured = std::env::var(Self::SERIALIZE_TYPE_PROPERTY)
                    .ok()
                    .or_else(|| std::env::var(Self::SERIALIZE_TYPE_ENV).ok());
                match configured {
                    Some(value) if !value.trim().is_empty() => {
                        SerializeType::value_of_name(Some(&value)).map_err(|error| {
                            format!("parser specified protocol error. protocol={value}: {error}")
                        })
                    }
                    _ => Ok(SerializeType::Json),
                }
            })
            .clone()
    }

    pub const fn get_header_length(length: i32) -> i32 {
        length & 0x00ff_ffff
    }

    pub const fn get_protocol_type(source: i32) -> Option<SerializeType> {
        let code = ((source as u32 >> 24) as u8) as i8;
        SerializeType::from_java_byte(code)
    }

    pub const fn mark_protocol_type(source: i32, serialization: SerializeType) -> i32 {
        (((serialization.code() as u32) << 24) | (source as u32 & 0x00ff_ffff)) as i32
    }

    pub fn is_response(&self) -> bool {
        self.flag & Self::RESPONSE_BIT != 0
    }

    pub fn get_type(&self) -> RemotingCommandType {
        if self.is_response() {
            RemotingCommandType::ResponseCommand
        } else {
            RemotingCommandType::RequestCommand
        }
    }

    pub fn mark_response(&mut self) {
        self.flag |= Self::RESPONSE_BIT;
    }

    pub fn is_oneway(&self) -> bool {
        self.flag & Self::ONEWAY_BIT != 0
    }

    pub fn mark_oneway(&mut self) {
        self.flag |= Self::ONEWAY_BIT;
    }

    pub fn is_suspended(&self) -> bool {
        self.suspended
    }

    pub fn set_suspended(&mut self, suspended: bool) {
        self.suspended = suspended;
    }
}

impl std::fmt::Display for RemotingCommand {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let language = self
            .language
            .map(|language| language.to_string())
            .unwrap_or_else(|| "null".to_owned());
        let remark = self.remark.as_deref().unwrap_or("null");
        let ext_fields = self.ext_fields.as_ref().map_or_else(
            || "null".to_owned(),
            |fields| {
                let entries = fields
                    .iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{{{entries}}}")
            },
        );
        let flag_binary = format!("{:b}", self.flag as u32);
        write!(
            formatter,
            "RemotingCommand [code={}, language={}, version={}, opaque={}, flag(B)={}, remark={}, extFields={}, serializeTypeCurrentRPC={}]",
            self.code,
            language,
            self.version,
            self.opaque,
            flag_binary,
            remark,
            ext_fields,
            self.serialization
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remoting_enum_methods_and_command_type_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json"))
                .unwrap();
        let cases = &fixture["remotingEnumMethodCases"];

        macro_rules! check_enum_cases {
            ($enum_type:ty, $cases:expr) => {{
                let enum_cases = $cases.as_array().unwrap();
                let values = <$enum_type>::values();
                let expected_names = enum_cases
                    .iter()
                    .find(|case| case["case"] == "constant")
                    .unwrap()["values"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|name| name.as_str().unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(
                    values.iter().map(|value| value.name()).collect::<Vec<_>>(),
                    expected_names
                );
                let mut mutated_values = <$enum_type>::values();
                mutated_values[0] = values[values.len() - 1];
                assert_eq!(<$enum_type>::values()[0], values[0]);

                for case in enum_cases {
                    match case["case"].as_str().unwrap() {
                        "constant" => {
                            let name = case["name"].as_str().unwrap();
                            let value = <$enum_type>::value_of_name(Some(name)).unwrap();
                            let ordinal = case["ordinal"].as_i64().unwrap() as i32;
                            assert_eq!(value.name(), name, "{case}");
                            assert_eq!(value.to_string(), case["text"].as_str().unwrap(), "{case}");
                            assert_eq!(value.ordinal(), ordinal, "{case}");
                            assert_eq!(value, values[ordinal as usize], "{case}");
                            assert_eq!(case["valueOfName"], name, "{case}");
                            assert_eq!(case["valuesDefensiveCopy"], true, "{case}");
                            let next = values[(ordinal as usize + 1) % values.len()];
                            assert_eq!(
                                value.java_compare_to(next),
                                case["compareNext"].as_i64().unwrap() as i32,
                                "{case}"
                            );
                        }
                        "lookup" => {
                            let result = <$enum_type>::value_of_name(case["input"].as_str());
                            match result {
                                Ok(value) => {
                                    assert_eq!(case["outcome"], "OK", "{case}");
                                    assert_eq!(case["decoded"], value.name(), "{case}");
                                }
                                Err(error) => {
                                    let expected = if case["input"].is_null() {
                                        "NullPointerException"
                                    } else {
                                        "IllegalArgumentException"
                                    };
                                    assert_eq!(case["outcome"], expected, "{case}");
                                    assert_eq!(case["message"], error.to_string(), "{case}");
                                }
                            }
                        }
                        other => panic!("unknown Java remoting enum case: {other}"),
                    }
                }
            }};
        }

        check_enum_cases!(RemotingCommandType, &cases["RemotingCommandType"]);
        check_enum_cases!(RequestType, &cases["RequestType"]);
        let request_source = &cases["RequestSource"];
        check_enum_cases!(RequestSource, &request_source["enum"]);
        assert_eq!(
            RequestSource::SYSTEM_PROPERTY_KEY,
            request_source["systemPropertyKey"].as_str().unwrap()
        );
        for case in request_source["values"].as_array().unwrap() {
            let source = RequestSource::value_of_name(case["name"].as_str()).unwrap();
            assert_eq!(source.get_value(), case["value"].as_i64().unwrap() as i32);
        }
        for case in request_source["integers"].as_array().unwrap() {
            let value = case["input"].as_i64().map(|input| input as i32);
            assert_eq!(
                RequestSource::is_valid(value),
                case["valid"].as_bool().unwrap()
            );
            assert_eq!(
                RequestSource::parse_integer(value).name(),
                case["parsed"].as_str().unwrap()
            );
        }

        for case in cases["requestTypeBytes"].as_array().unwrap() {
            let input = case["input"].as_i64().unwrap() as i8;
            let decoded = RequestType::from_java_byte(input);
            assert_eq!(case["outcome"], "OK", "{case}");
            assert_eq!(
                case["decoded"].as_str(),
                decoded.map(RequestType::name),
                "{case}"
            );
            assert_eq!(
                case["getCode"].as_i64().map(|code| code as i8),
                decoded.map(RequestType::get_code),
                "{case}"
            );
        }

        let request = RemotingCommand::default();
        let mut response = RemotingCommand::default();
        response.mark_response();
        let mut marked_request = RemotingCommand::default();
        marked_request.mark_response();
        let commands = [request, response, marked_request];
        for (case, command) in cases["commandGetType"]
            .as_array()
            .unwrap()
            .iter()
            .zip(commands)
        {
            assert_eq!(command.get_type().name(), case["type"].as_str().unwrap());
            assert_eq!(command.is_response(), case["isResponse"].as_bool().unwrap());
        }

        let default = RemotingCommand::default();
        let mut oneway = RemotingCommand::default();
        oneway.mark_oneway();
        let mut oneway_response = RemotingCommand::default();
        oneway_response.mark_oneway();
        oneway_response.mark_response();
        let mut repeated_marks = oneway_response.clone();
        repeated_marks.mark_oneway();
        repeated_marks.mark_response();
        let mut preserves_unknown_bits = RemotingCommand {
            flag: 1 << 8,
            ..RemotingCommand::default()
        };
        preserves_unknown_bits.mark_oneway();
        preserves_unknown_bits.mark_response();
        let commands = [
            default,
            oneway,
            oneway_response,
            repeated_marks,
            preserves_unknown_bits,
        ];
        let flag_cases = cases["commandFlags"].as_array().unwrap();
        assert_eq!(flag_cases.len(), commands.len());
        for (case, command) in flag_cases.iter().zip(commands) {
            assert_eq!(
                command.flag,
                case["flag"].as_i64().unwrap() as i32,
                "{case}"
            );
            assert_eq!(
                command.get_type().name(),
                case["type"].as_str().unwrap(),
                "{case}"
            );
            assert_eq!(
                command.is_oneway(),
                case["isOneway"].as_bool().unwrap(),
                "{case}"
            );
            assert_eq!(
                command.is_response(),
                case["isResponse"].as_bool().unwrap(),
                "{case}"
            );
        }

        let suspended_cases = cases["commandSuspended"].as_array().unwrap();
        let mut command = RemotingCommand::default();
        assert_eq!(suspended_cases.len(), 3);
        for (case, next) in suspended_cases.iter().zip([None, Some(true), Some(false)]) {
            if let Some(value) = next {
                command.set_suspended(value);
            }
            assert_eq!(
                command.is_suspended(),
                case["suspended"].as_bool().unwrap(),
                "{case}"
            );
        }

        command.set_suspended(true);
        let encoded = crate::codec::json_header::encode(&command).unwrap();
        assert!(
            !String::from_utf8(encoded.clone())
                .unwrap()
                .contains("suspended")
        );
        assert!(
            !crate::codec::json_header::decode(&encoded)
                .unwrap()
                .is_suspended()
        );

        let helpers = &cases["commandProtocolHelpers"];
        assert_eq!(
            RemotingCommand::SERIALIZE_TYPE_PROPERTY,
            helpers["serializeTypeProperty"].as_str().unwrap()
        );
        assert_eq!(
            RemotingCommand::SERIALIZE_TYPE_ENV,
            helpers["serializeTypeEnv"].as_str().unwrap()
        );
        assert_eq!(
            RemotingCommand::REMOTING_VERSION_KEY,
            helpers["remotingVersionKey"].as_str().unwrap()
        );
        assert_eq!(
            RemotingCommand::get_serialize_type_config_in_this_server()
                .unwrap()
                .name(),
            helpers["serializeTypeConfig"].as_str().unwrap()
        );
        for case in helpers["headerLengths"].as_array().unwrap() {
            assert_eq!(
                RemotingCommand::get_header_length(case["input"].as_i64().unwrap() as i32),
                case["result"].as_i64().unwrap() as i32,
                "{case}"
            );
        }
        for case in helpers["protocolTypes"].as_array().unwrap() {
            let source = case["source"].as_i64().unwrap() as i32;
            let serialization = match case["type"].as_str().unwrap() {
                "JSON" => SerializeType::Json,
                "ROCKETMQ" => SerializeType::RocketMq,
                other => panic!("unexpected Java serialization type {other}"),
            };
            let marked = RemotingCommand::mark_protocol_type(source, serialization);
            assert_eq!(marked, case["marked"].as_i64().unwrap() as i32, "{case}");
            assert_eq!(
                RemotingCommand::get_protocol_type(marked).map(SerializeType::name),
                case["decoded"].as_str(),
                "{case}"
            );
        }
        for case in helpers["protocolTypeLookups"].as_array().unwrap() {
            assert_eq!(
                RemotingCommand::get_protocol_type(case["source"].as_i64().unwrap() as i32)
                    .map(SerializeType::name),
                case["decoded"].as_str(),
                "{case}"
            );
        }
        let first_id = RemotingCommand::create_new_request_id();
        let second_id = RemotingCommand::create_new_request_id();
        assert_eq!(
            second_id.wrapping_sub(first_id),
            helpers["requestIdDelta"].as_i64().unwrap() as i32
        );

        for case in cases["commandToString"].as_array().unwrap() {
            let fields = &case["fields"];
            let language = fields["language"]
                .as_str()
                .map(|name| LanguageCode::value_of_name(Some(name)).unwrap());
            let serialization =
                SerializeType::value_of_name(fields["serialization"].as_str()).unwrap();
            let ext_fields = fields["extFields"].as_object().map(|object| {
                object
                    .iter()
                    .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
                    .collect()
            });
            let command = RemotingCommand {
                code: fields["code"].as_i64().unwrap() as i32,
                language,
                version: fields["version"].as_i64().unwrap() as i32,
                opaque: fields["opaque"].as_i64().unwrap() as i32,
                flag: fields["flag"].as_i64().unwrap() as i32,
                remark: fields["remark"].as_str().map(str::to_owned),
                ext_fields,
                serialization,
                ..RemotingCommand::default()
            };
            assert_eq!(
                command.to_string(),
                case["result"].as_str().unwrap(),
                "{case}"
            );
        }
    }

    #[test]
    fn pinned_java_remoting_command_public_surface_is_present_in_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/remoting/response-future-api.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["sourceCommit"],
            "efa1be59d6fc747ad220f036c86030f05f0419f9"
        );
        assert_eq!(
            fixture["remotingCommandPublicConstructors"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        let methods = fixture["remotingCommandPublicMethods"].as_array().unwrap();
        assert_eq!(methods.len(), 57);
        let method_names = methods
            .iter()
            .map(|signature| signature.as_str().unwrap())
            .collect::<Vec<_>>();
        assert!(method_names.len() >= 40);
        for signature in [
            "void markResponseType()",
            "boolean isResponseType()",
            "void markOnewayRPC()",
            "boolean isOnewayRPC()",
            "boolean isSuspended()",
            "void setSuspended(boolean)",
            "java.lang.String toString()",
        ] {
            assert!(method_names.contains(&signature), "missing {signature}");
        }
    }

    #[test]
    fn serialize_type_config_matches_java_property_and_environment_rules() {
        const CHILD_MARKER: &str = "ROCKETMQ_CLASSIC_SERIALIZE_TYPE_CASE";
        if let Ok(case) = std::env::var(CHILD_MARKER) {
            match case.as_str() {
                "property-precedence" => assert_eq!(
                    RemotingCommand::default().serialization,
                    SerializeType::RocketMq
                ),
                "environment-alias" => assert_eq!(
                    RemotingCommand::default().serialization,
                    SerializeType::RocketMq
                ),
                "blank-property" => assert_eq!(
                    RemotingCommand::default().serialization,
                    SerializeType::Json
                ),
                "invalid-property" => {
                    let error =
                        RemotingCommand::get_serialize_type_config_in_this_server().unwrap_err();
                    assert!(error.contains("parser specified protocol error"));
                    assert!(error.contains("protocol=rocketmq"));
                    let panic = std::panic::catch_unwind(RemotingCommand::default).unwrap_err();
                    assert!(
                        panic
                            .downcast_ref::<String>()
                            .unwrap()
                            .contains("protocol=rocketmq")
                    );
                }
                other => panic!("unknown child case {other}"),
            }
            return;
        }

        let executable = std::env::current_exe().unwrap();
        for (case, property, environment) in [
            ("property-precedence", Some("ROCKETMQ"), Some("JSON")),
            ("environment-alias", None, Some("ROCKETMQ")),
            ("blank-property", Some(" \t"), Some("ROCKETMQ")),
            ("invalid-property", Some("rocketmq"), None),
        ] {
            let mut child = std::process::Command::new(&executable);
            child
                .arg("--exact")
                .arg("command::tests::serialize_type_config_matches_java_property_and_environment_rules")
                .arg("--nocapture")
                .env(CHILD_MARKER, case)
                .env_remove(RemotingCommand::SERIALIZE_TYPE_PROPERTY)
                .env_remove(RemotingCommand::SERIALIZE_TYPE_ENV);
            if let Some(value) = property {
                child.env(RemotingCommand::SERIALIZE_TYPE_PROPERTY, value);
            }
            if let Some(value) = environment {
                child.env(RemotingCommand::SERIALIZE_TYPE_ENV, value);
            }
            let output = child.output().unwrap();
            assert!(
                output.status.success(),
                "child case {case} failed: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    #[test]
    fn defaults_and_flag_helpers_match_java_bits() {
        let mut command = RemotingCommand::default();
        assert_eq!(command.language, Some(LanguageCode::Rust));
        assert!(!command.is_response());
        assert!(!command.is_oneway());
        command.flag = 1 << 8;
        command.mark_response();
        assert_eq!(command.flag, (1 << 8) | 1);
        command.mark_oneway();
        assert_eq!(command.flag, (1 << 8) | 3);
    }
}
