pub mod admin;
pub mod consumer;
pub mod extensions;
pub mod producer;
pub mod route;

use crate::config::{SharedClientConfig, read_shared_client_config};
use crate::error::RpcError;
use rocketmq_classic_protocol::codes::response;
use rocketmq_classic_protocol::header::{CommandHeader, HeaderMap};
use rocketmq_classic_remoting::RemotingCommand;
use rocketmq_classic_remoting::client::RemotingClient;
use serde::de::DeserializeOwned;
use tokio::time::Instant;

#[derive(Clone)]
pub struct MQClientApi {
    remoting: RemotingClient,
    language: Option<rocketmq_classic_protocol::codes::LanguageCode>,
    config: Option<SharedClientConfig>,
}

impl MQClientApi {
    pub fn new(remoting: RemotingClient) -> Self {
        Self::with_language(
            remoting,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
        )
    }

    pub(crate) fn with_language(
        remoting: RemotingClient,
        language: Option<rocketmq_classic_protocol::codes::LanguageCode>,
    ) -> Self {
        Self {
            remoting,
            language,
            config: None,
        }
    }

    pub(crate) fn with_config(remoting: RemotingClient, config: SharedClientConfig) -> Self {
        Self {
            remoting,
            language: None,
            config: Some(config),
        }
    }

    async fn request<H: CommandHeader>(
        &self,
        addr: &str,
        code: i32,
        header: &H,
        body: Option<Vec<u8>>,
        deadline: Instant,
    ) -> Result<RemotingCommand, RpcError> {
        self.request_with_mode(addr, code, header, body, deadline, true)
            .await
    }

    async fn request_future<H: CommandHeader>(
        &self,
        addr: &str,
        code: i32,
        header: &H,
        body: Option<Vec<u8>>,
        deadline: Instant,
    ) -> Result<RemotingCommand, RpcError> {
        self.request_with_mode(addr, code, header, body, deadline, false)
            .await
    }

    async fn request_future_without_header(
        &self,
        addr: &str,
        code: i32,
        body: Option<Vec<u8>>,
        deadline: Instant,
    ) -> Result<RemotingCommand, RpcError> {
        let command = RemotingCommand {
            code,
            language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
            body,
            ..Default::default()
        };
        Ok(self.remoting.invoke(addr, command, deadline).await?)
    }

    #[allow(clippy::too_many_arguments)]
    async fn request_with_mode<H: CommandHeader>(
        &self,
        addr: &str,
        code: i32,
        header: &H,
        body: Option<Vec<u8>>,
        deadline: Instant,
        sync: bool,
    ) -> Result<RemotingCommand, RpcError> {
        let mut fields = HeaderMap::new();
        header.encode_fields(&mut fields);
        self.request_fields_with_mode(addr, code, fields, body, deadline, sync)
            .await
    }

    async fn request_fields_with_mode(
        &self,
        addr: &str,
        code: i32,
        fields: HeaderMap,
        body: Option<Vec<u8>>,
        deadline: Instant,
        sync: bool,
    ) -> Result<RemotingCommand, RpcError> {
        let heartbeat_language = self
            .config
            .as_ref()
            .map(|config| read_shared_client_config(config).language)
            .unwrap_or(self.language);
        let command = RemotingCommand {
            code,
            // Pinned Java sets clientConfig.language only on heartbeat V1/V2.
            language: if code == rocketmq_classic_protocol::codes::request::HEART_BEAT {
                heartbeat_language
            } else {
                Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
            },
            ext_fields: Some(fields.into()),
            body,
            ..Default::default()
        };
        Ok(if sync {
            self.remoting
                .invoke_sync_with_deadline(addr, command, deadline)
                .await?
        } else {
            self.remoting.invoke(addr, command, deadline).await?
        })
    }

    async fn oneway<H: CommandHeader>(
        &self,
        addr: &str,
        code: i32,
        header: &H,
        body: Option<Vec<u8>>,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let mut fields = HeaderMap::new();
        header.encode_fields(&mut fields);
        self.remoting.invoke_oneway(
            addr,
            RemotingCommand {
                code,
                language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                ext_fields: Some(fields.into()),
                body,
                ..RemotingCommand::default()
            },
            deadline,
        )?;
        Ok(())
    }
}

fn classify(reply: RemotingCommand, code: i32, peer: &str) -> Result<RemotingCommand, RpcError> {
    if reply.code != response::SUCCESS {
        return Err(RpcError::Response {
            request_code: code,
            peer: peer.to_owned(),
            code: reply.code,
            remark: reply.remark,
        });
    }
    Ok(reply)
}

fn decode_header<T: CommandHeader>(
    reply: &RemotingCommand,
    code: i32,
    peer: &str,
) -> Result<T, RpcError> {
    T::decode_fields(
        reply
            .ext_fields
            .as_ref()
            .ok_or_else(|| RpcError::MalformedResponse {
                request_code: code,
                peer: peer.to_owned(),
                reason: "missing response header".into(),
            })?,
    )
    .map_err(|error| RpcError::MalformedResponse {
        request_code: code,
        peer: peer.to_owned(),
        reason: error.to_string(),
    })
}

fn decode_body<T: DeserializeOwned>(
    reply: &RemotingCommand,
    code: i32,
    peer: &str,
) -> Result<T, RpcError> {
    let body = reply
        .body
        .as_ref()
        .ok_or_else(|| RpcError::MalformedResponse {
            request_code: code,
            peer: peer.to_owned(),
            reason: "missing response body".into(),
        })?;
    serde_json::from_slice(body).map_err(|error| RpcError::MalformedResponse {
        request_code: code,
        peer: peer.to_owned(),
        reason: error.to_string(),
    })
}

#[cfg(test)]
mod enum_tests {
    use super::{
        consumer::PullStatus,
        extensions::{AckStatus, PopStatus},
        producer::SendStatus,
    };

    #[test]
    fn client_status_enum_methods_match_invoked_java_values() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        let groups = &fixture["clientStatusEnumMethodCases"];

        macro_rules! assert_status_cases {
            ($enum_type:ty, $cases:expr) => {{
                let cases = $cases.as_array().unwrap();
                let values = <$enum_type>::values();
                let expected_names = cases
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
                mutated_values[0] = values[values.len() - 1].clone();
                assert_eq!(<$enum_type>::values()[0], values[0]);

                for case in cases {
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
                            let next = values[((ordinal as usize) + 1) % values.len()].clone();
                            assert_eq!(
                                value.java_compare_to(&next),
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
                        other => panic!("unknown Java status enum case: {other}"),
                    }
                }
            }};
        }

        assert_status_cases!(PullStatus, &groups["PullStatus"]);
        assert_status_cases!(PopStatus, &groups["PopStatus"]);
        assert_status_cases!(AckStatus, &groups["AckStatus"]);
        assert_status_cases!(SendStatus, &groups["SendStatus"]);
    }
}
