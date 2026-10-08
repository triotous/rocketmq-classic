pub mod request;
pub mod response;

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

    pub fn from_java_name(name: Option<&str>) -> Option<Self> {
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
        for case in fixture["languageCodeMethodCases"].as_array().unwrap() {
            match case["case"].as_str().unwrap() {
                "constant" => {
                    let name = case["name"].as_str().unwrap();
                    let code = case["code"].as_i64().unwrap() as u8;
                    let value = LanguageCode::from_java_name(Some(name)).unwrap();
                    assert_eq!(value.code(), code, "{case}");
                    assert_eq!(value.name(), name, "{case}");
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
                    let decoded = LanguageCode::from_java_name(input);
                    assert_eq!(
                        decoded.map(LanguageCode::name),
                        case["decoded"].as_str(),
                        "{case}"
                    );
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
