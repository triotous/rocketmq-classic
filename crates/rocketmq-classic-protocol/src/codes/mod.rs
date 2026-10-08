pub mod request;
pub mod response;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LanguageCodeLookupError {
    #[error("Name is null")]
    NullName,
    #[error("No enum constant org.apache.rocketmq.remoting.protocol.LanguageCode.{0}")]
    UnknownName(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LanguageCode {
    Java,
    Cpp,
    Dotnet,
    Python,
    Delphi,
    Erlang,
    Ruby,
    Other,
    Http,
    Go,
    Php,
    Oms,
    Rust,
    NodeJs,
}

impl LanguageCode {
    pub const fn values() -> [Self; 14] {
        [
            Self::Java,
            Self::Cpp,
            Self::Dotnet,
            Self::Python,
            Self::Delphi,
            Self::Erlang,
            Self::Ruby,
            Self::Other,
            Self::Http,
            Self::Go,
            Self::Php,
            Self::Oms,
            Self::Rust,
            Self::NodeJs,
        ]
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, LanguageCodeLookupError> {
        match name {
            None => Err(LanguageCodeLookupError::NullName),
            Some("JAVA") => Ok(Self::Java),
            Some("CPP") => Ok(Self::Cpp),
            Some("DOTNET") => Ok(Self::Dotnet),
            Some("PYTHON") => Ok(Self::Python),
            Some("DELPHI") => Ok(Self::Delphi),
            Some("ERLANG") => Ok(Self::Erlang),
            Some("RUBY") => Ok(Self::Ruby),
            Some("OTHER") => Ok(Self::Other),
            Some("HTTP") => Ok(Self::Http),
            Some("GO") => Ok(Self::Go),
            Some("PHP") => Ok(Self::Php),
            Some("OMS") => Ok(Self::Oms),
            Some("RUST") => Ok(Self::Rust),
            Some("NODE_JS") => Ok(Self::NodeJs),
            Some(other) => Err(LanguageCodeLookupError::UnknownName(other.to_owned())),
        }
    }

    pub const fn from_java_byte(code: i8) -> Option<Self> {
        match code {
            0 => Some(Self::Java),
            1 => Some(Self::Cpp),
            2 => Some(Self::Dotnet),
            3 => Some(Self::Python),
            4 => Some(Self::Delphi),
            5 => Some(Self::Erlang),
            6 => Some(Self::Ruby),
            7 => Some(Self::Other),
            8 => Some(Self::Http),
            9 => Some(Self::Go),
            10 => Some(Self::Php),
            11 => Some(Self::Oms),
            12 => Some(Self::Rust),
            13 => Some(Self::NodeJs),
            _ => None,
        }
    }
    pub const fn code(self) -> u8 {
        match self {
            Self::Java => 0,
            Self::Cpp => 1,
            Self::Dotnet => 2,
            Self::Python => 3,
            Self::Delphi => 4,
            Self::Erlang => 5,
            Self::Ruby => 6,
            Self::Other => 7,
            Self::Http => 8,
            Self::Go => 9,
            Self::Php => 10,
            Self::Oms => 11,
            Self::Rust => 12,
            Self::NodeJs => 13,
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Java => "JAVA",
            Self::Cpp => "CPP",
            Self::Dotnet => "DOTNET",
            Self::Python => "PYTHON",
            Self::Delphi => "DELPHI",
            Self::Erlang => "ERLANG",
            Self::Ruby => "RUBY",
            Self::Other => "OTHER",
            Self::Http => "HTTP",
            Self::Go => "GO",
            Self::Php => "PHP",
            Self::Oms => "OMS",
            Self::Rust => "RUST",
            Self::NodeJs => "NODE_JS",
        }
    }

    pub const fn ordinal(self) -> i32 {
        self.code() as i32
    }

    pub const fn java_compare_to(self, other: Self) -> i32 {
        self.ordinal() - other.ordinal()
    }

    pub fn get_code_by_name(name: Option<&str>) -> Option<Self> {
        match name {
            Some("JAVA") => Some(Self::Java),
            Some("CPP") => Some(Self::Cpp),
            Some("DOTNET") => Some(Self::Dotnet),
            Some("PYTHON") => Some(Self::Python),
            Some("DELPHI") => Some(Self::Delphi),
            Some("ERLANG") => Some(Self::Erlang),
            Some("RUBY") => Some(Self::Ruby),
            Some("OTHER") => Some(Self::Other),
            Some("HTTP") => Some(Self::Http),
            Some("GO") => Some(Self::Go),
            Some("PHP") => Some(Self::Php),
            Some("OMS") => Some(Self::Oms),
            Some("RUST") => Some(Self::Rust),
            Some("NODE_JS") => Some(Self::NodeJs),
            _ => None,
        }
    }
}

impl std::fmt::Display for LanguageCode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializeType {
    Json,
    RocketMq,
    Unknown(u8),
}

impl SerializeType {
    pub const fn from_code(code: u8) -> Self {
        match code {
            0 => Self::Json,
            1 => Self::RocketMq,
            value => Self::Unknown(value),
        }
    }
    pub const fn from_java_byte(code: i8) -> Self {
        Self::from_code(code as u8)
    }
    pub const fn java_byte(self) -> i8 {
        self.code() as i8
    }
    pub const fn code(self) -> u8 {
        match self {
            Self::Json => 0,
            Self::RocketMq => 1,
            Self::Unknown(value) => value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn java_language_lookup_matches_known_and_unknown_byte_codes() {
        for code in 0..=13 {
            let language = LanguageCode::from_java_byte(code as i8).unwrap();
            assert_eq!(language.code(), code);
        }
        for code in [14, 127, -1, i8::MIN] {
            assert_eq!(LanguageCode::from_java_byte(code), None);
        }
        assert_eq!(LanguageCode::Rust.code(), 12);
    }

    #[test]
    fn language_code_methods_match_invoked_java_values() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        let mut mutated_values = LanguageCode::values();
        mutated_values[0] = LanguageCode::NodeJs;
        assert_eq!(LanguageCode::values()[0], LanguageCode::Java);
        for case in fixture["languageCodeMethodCases"].as_array().unwrap() {
            match case["case"].as_str().unwrap() {
                "constant" => {
                    let name = case["name"].as_str().unwrap();
                    let code = case["code"].as_i64().unwrap() as u8;
                    let value = LanguageCode::get_code_by_name(Some(name)).unwrap();
                    let values = LanguageCode::values();
                    let expected_names = case["values"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|name| name.as_str().unwrap())
                        .collect::<Vec<_>>();
                    assert_eq!(value.code(), code, "{case}");
                    assert_eq!(value.name(), name, "{case}");
                    assert_eq!(
                        value.ordinal(),
                        case["ordinal"].as_i64().unwrap() as i32,
                        "{case}"
                    );
                    assert_eq!(value.to_string(), case["text"].as_str().unwrap(), "{case}");
                    assert_eq!(
                        values.iter().map(|value| value.name()).collect::<Vec<_>>(),
                        expected_names,
                        "{case}"
                    );
                    let next = values[(value.ordinal() as usize + 1) % values.len()];
                    assert_eq!(
                        value.java_compare_to(next),
                        case["compareNext"].as_i64().unwrap() as i32,
                        "{case}"
                    );
                    assert_eq!(case["valueOfName"], name, "{case}");
                    assert_eq!(case["valuesDefensiveCopy"], true, "{case}");
                    assert_eq!(
                        LanguageCode::from_java_byte(code as i8),
                        Some(value),
                        "{case}"
                    );
                    assert_eq!(case["valueOfByte"], name, "{case}");
                    assert_eq!(case["getCodeByName"], name, "{case}");
                }
                "byte" => {
                    let input = case["input"].as_i64().unwrap() as i8;
                    let decoded = LanguageCode::from_java_byte(input);
                    assert_eq!(
                        decoded.map(LanguageCode::name),
                        case["decoded"].as_str(),
                        "{case}"
                    );
                }
                "name" => {
                    let input = case["input"].as_str();
                    let decoded = LanguageCode::get_code_by_name(input);
                    assert_eq!(
                        decoded.map(LanguageCode::name),
                        case["decoded"].as_str(),
                        "{case}"
                    );
                }
                "enumNameLookup" => {
                    let result = LanguageCode::value_of_name(case["input"].as_str());
                    match result {
                        Ok(value) => {
                            assert_eq!(case["outcome"], "OK", "{case}");
                            assert_eq!(case["decoded"], value.name(), "{case}");
                        }
                        Err(error) => {
                            let outcome = match &error {
                                LanguageCodeLookupError::NullName => "NullPointerException",
                                LanguageCodeLookupError::UnknownName(_) => {
                                    "IllegalArgumentException"
                                }
                            };
                            assert_eq!(case["outcome"], outcome, "{case}");
                            assert_eq!(case["message"], error.to_string(), "{case}");
                        }
                    }
                }
                kind => panic!("unknown Java LanguageCode case: {kind}"),
            }
        }
    }

    #[test]
    fn serialization_codes_preserve_unknown_tags() {
        assert_eq!(SerializeType::from_code(0), SerializeType::Json);
        assert_eq!(SerializeType::from_code(1), SerializeType::RocketMq);
        assert_eq!(SerializeType::from_code(200).code(), 200);
    }

    #[test]
    fn request_and_response_codes_are_signed_raw_i32_constants() {
        assert_eq!(request::SEND_MESSAGE, 10);
        assert_eq!(request::POP_MESSAGE, 200050);
        assert_eq!(response::RPC_UNKNOWN, -1000);
    }
}
