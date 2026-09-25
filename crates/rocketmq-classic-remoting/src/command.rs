use std::collections::BTreeMap;

use rocketmq_classic_protocol::codes::{LanguageCode, SerializeType};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemotingCommand {
    pub code: i32,
    pub language: LanguageCode,
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
            language: LanguageCode::Rust,
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
    fn defaults_and_flag_helpers_match_java_bits() {
        let mut command = RemotingCommand::default();
        assert_eq!(command.language, LanguageCode::Rust);
        assert!(!command.is_response());
        assert!(!command.is_oneway());
        command.flag = 1 << 8;
        command.mark_response();
        assert_eq!(command.flag, (1 << 8) | 1);
        command.mark_oneway();
        assert_eq!(command.flag, (1 << 8) | 3);
    }
}
