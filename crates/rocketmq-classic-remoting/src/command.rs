use std::collections::BTreeMap;

use rocketmq_classic_protocol::codes::{JavaEnumLookupError, LanguageCode, SerializeType};

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
            serialization: SerializeType::Json,
        }
    }
}

impl RemotingCommand {
    const RESPONSE_BIT: i32 = 1;
    const ONEWAY_BIT: i32 = 1 << 1;

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
