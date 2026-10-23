use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use rocketmq_classic_protocol::{
    codes::{LanguageCode, request, response},
    header::{CommandHeader, HeaderMap, ProtocolError},
};
use rocketmq_classic_remoting::RemotingCommand;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::{MQClientApi, classify, java_client_request_command};
use crate::error::RpcError;

/// Java `GetMetaDataResponseHeader` returned by a controller.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GetMetaDataResponseHeader {
    pub group: Option<String>,
    pub controller_leader_id: Option<String>,
    pub controller_leader_address: Option<String>,
    pub is_leader: bool,
    pub peers: Option<String>,
}

impl CommandHeader for GetMetaDataResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        insert_optional(fields, "group", self.group.as_deref());
        insert_optional(
            fields,
            "controllerLeaderId",
            self.controller_leader_id.as_deref(),
        );
        insert_optional(
            fields,
            "controllerLeaderAddress",
            self.controller_leader_address.as_deref(),
        );
        fields.insert("isLeader".into(), self.is_leader.to_string());
        insert_optional(fields, "peers", self.peers.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            group: fields.get("group").cloned(),
            controller_leader_id: fields.get("controllerLeaderId").cloned(),
            controller_leader_address: fields.get("controllerLeaderAddress").cloned(),
            // Java's primitive boolean defaults to false when the ext field is absent
            // or is not equal to "true", ignoring case.
            is_leader: fields
                .get("isLeader")
                .is_some_and(|value| value.eq_ignore_ascii_case("true")),
            peers: fields.get("peers").cloned(),
        })
    }
}

/// Java `ElectMasterResponseHeader` returned by a successful election request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ElectMasterResponseHeader {
    pub master_broker_id: Option<i64>,
    pub master_address: Option<String>,
    pub master_epoch: Option<i32>,
    pub sync_state_set_epoch: Option<i32>,
}

impl CommandHeader for ElectMasterResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        insert_optional_number(fields, "masterBrokerId", self.master_broker_id);
        insert_optional(fields, "masterAddress", self.master_address.as_deref());
        insert_optional_number(fields, "masterEpoch", self.master_epoch);
        insert_optional_number(fields, "syncStateSetEpoch", self.sync_state_set_epoch);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            master_broker_id: parse_optional(fields, "masterBrokerId")?,
            master_address: fields.get("masterAddress").cloned(),
            master_epoch: parse_optional(fields, "masterEpoch")?,
            sync_state_set_epoch: parse_optional(fields, "syncStateSetEpoch")?,
        })
    }
}

/// Java `BrokerReplicasInfo` returned by `getInSyncStateData`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerReplicasInfo {
    #[serde(default = "empty_optional_map")]
    pub replicas_info_table: Option<HashMap<String, ReplicasInfo>>,
}

impl Default for BrokerReplicasInfo {
    fn default() -> Self {
        Self {
            replicas_info_table: Some(HashMap::new()),
        }
    }
}

/// Replica state for one broker, corresponding to Java's nested
/// `BrokerReplicasInfo.ReplicasInfo`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicasInfo {
    pub master_broker_id: Option<i64>,
    pub master_address: Option<String>,
    pub master_epoch: Option<i32>,
    pub sync_state_set_epoch: Option<i32>,
    pub in_sync_replicas: Option<Vec<ReplicaIdentity>>,
    pub not_in_sync_replicas: Option<Vec<ReplicaIdentity>>,
}

/// One replica identity in a Java `BrokerReplicasInfo` response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaIdentity {
    pub broker_name: Option<String>,
    pub broker_id: Option<i64>,
    pub broker_address: Option<String>,
    pub alive: Option<bool>,
}

/// Java `EpochEntryCache` returned by `getBrokerEpochCache`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EpochEntryCache {
    pub cluster_name: Option<String>,
    pub broker_name: Option<String>,
    #[serde(default)]
    pub broker_id: i64,
    pub epoch_list: Option<Vec<EpochEntry>>,
    #[serde(default)]
    pub max_offset: i64,
}

/// One commit-log epoch range returned in an `EpochEntryCache`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EpochEntry {
    #[serde(default)]
    pub epoch: i32,
    #[serde(default)]
    pub start_offset: i64,
    #[serde(default = "last_epoch_end_offset")]
    pub end_offset: i64,
}

fn last_epoch_end_offset() -> i64 {
    i64::MAX
}

/// Java `BrokerMemberGroup` returned by a successful master election.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerMemberGroup {
    pub cluster: Option<String>,
    pub broker_name: Option<String>,
    #[serde(default = "empty_optional_map")]
    pub broker_addrs: Option<HashMap<i64, String>>,
}

impl Default for BrokerMemberGroup {
    fn default() -> Self {
        Self {
            cluster: None,
            broker_name: None,
            broker_addrs: Some(HashMap::new()),
        }
    }
}

impl BrokerMemberGroup {
    /// Returns the smallest broker ID, matching Java `minimumBrokerId()`.
    pub fn minimum_broker_id(&self) -> Option<i64> {
        let broker_addrs = self.broker_addrs.as_ref()?;
        Some(broker_addrs.keys().copied().min().unwrap_or_default())
    }
}

fn empty_optional_map<K, V>() -> Option<HashMap<K, V>> {
    Some(HashMap::new())
}

fn insert_optional(fields: &mut HeaderMap, name: &str, value: Option<&str>) {
    if let Some(value) = value {
        fields.insert(name.to_owned(), value.to_owned());
    }
}

fn insert_optional_number<T: ToString>(fields: &mut HeaderMap, name: &str, value: Option<T>) {
    if let Some(value) = value {
        fields.insert(name.to_owned(), value.to_string());
    }
}

fn parse_optional<T: std::str::FromStr>(
    fields: &HeaderMap,
    name: &'static str,
) -> Result<Option<T>, ProtocolError>
where
    T::Err: std::fmt::Debug,
{
    fields
        .get(name)
        .map(|value| {
            value
                .parse()
                .map_err(|error: T::Err| ProtocolError::InvalidField {
                    field: name,
                    reason: format!("{error:?}"),
                })
        })
        .transpose()
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn malformed_response(code: i32, peer: &str, reason: impl Into<String>) -> RpcError {
    RpcError::MalformedResponse {
        request_code: code,
        peer: peer.to_owned(),
        reason: reason.into(),
    }
}

fn decode_nullable_body<T: DeserializeOwned>(
    body: Option<&[u8]>,
    code: i32,
    peer: &str,
) -> Result<Option<T>, RpcError> {
    let Some(body) = body.filter(|body| !body.is_empty()) else {
        return Ok(None);
    };
    serde_json::from_slice(body).map_err(|error| malformed_response(code, peer, error.to_string()))
}

impl MQClientApi {
    /// Reads controller metadata through Java `MQClientAPIImpl.getControllerMetaData`.
    /// The request uses the Java method's fixed 3000 millisecond timeout.
    pub async fn get_controller_meta_data(
        &self,
        controller_address: &str,
    ) -> Result<GetMetaDataResponseHeader, RpcError> {
        let code = request::CONTROLLER_GET_METADATA_INFO;
        let reply = self
            .invoke_controller(controller_address, code, None, None, 3000)
            .await?;
        let reply = classify(reply, code, controller_address)?;
        let fields = reply.ext_fields.as_ref().cloned().unwrap_or_default();
        GetMetaDataResponseHeader::decode_fields(&fields)
            .map_err(|error| malformed_response(code, controller_address, error.to_string()))
    }

    /// Reads replication state through Java `MQClientAPIImpl.getInSyncStateData`.
    /// It first asks the supplied controller for metadata, then sends the broker
    /// list to the returned leader. A null broker list is serialized as JSON null.
    pub async fn get_in_sync_state_data(
        &self,
        controller_address: &str,
        brokers: Option<&[String]>,
    ) -> Result<Option<BrokerReplicasInfo>, RpcError> {
        let metadata = self.get_controller_meta_data(controller_address).await?;
        let leader = controller_leader_address(
            metadata.controller_leader_address.as_deref(),
            request::CONTROLLER_GET_SYNC_STATE_DATA,
        )?;
        let body =
            serde_json::to_vec(&brokers).map_err(|source| RpcError::RequestSerialization {
                request_code: request::CONTROLLER_GET_SYNC_STATE_DATA,
                source,
            })?;
        let reply = self
            .invoke_controller(
                leader,
                request::CONTROLLER_GET_SYNC_STATE_DATA,
                None,
                Some(body),
                3000,
            )
            .await?;
        let reply = classify(reply, request::CONTROLLER_GET_SYNC_STATE_DATA, leader)?;
        decode_nullable_body(
            reply.body.as_deref(),
            request::CONTROLLER_GET_SYNC_STATE_DATA,
            leader,
        )
    }

    /// Reads the Broker's epoch cache through Java
    /// `MQClientAPIImpl.getBrokerEpochCache`, using its fixed 3000 millisecond
    /// timeout and nullable response body.
    pub async fn get_broker_epoch_cache(
        &self,
        broker_address: &str,
    ) -> Result<Option<EpochEntryCache>, RpcError> {
        let code = request::GET_BROKER_EPOCH_CACHE;
        let reply = self
            .invoke_controller(broker_address, code, None, None, 3000)
            .await?;
        let reply = classify(reply, code, broker_address)?;
        decode_nullable_body(reply.body.as_deref(), code, broker_address)
    }

    /// Reads Java `MQClientAPIImpl.getControllerConfig` from the supplied
    /// controllers, or from the remoting client's NameServer list when the
    /// controller list is null or empty. A missing list after fallback returns
    /// `None`; malformed Properties values are represented as `None` per entry.
    pub async fn get_controller_config(
        &self,
        controllers: Option<&[String]>,
        timeout_millis: i64,
    ) -> Result<Option<HashMap<String, Option<HashMap<String, String>>>>, RpcError> {
        let addresses = controllers
            .filter(|addresses| !addresses.is_empty())
            .map(<[_]>::to_vec)
            .or_else(|| self.remoting.get_name_server_address_list())
            .unwrap_or_default();
        if addresses.is_empty() {
            return Ok(None);
        }

        let code = request::GET_CONTROLLER_CONFIG;
        let mut configurations = HashMap::with_capacity(addresses.len());
        for address in addresses {
            let reply = self
                .invoke_controller(&address, code, None, None, timeout_millis)
                .await?;
            let reply = classify(reply, code, &address)?;
            let body = reply.body.as_deref().ok_or_else(|| {
                malformed_response(code, &address, "successful reply has no body")
            })?;
            let properties =
                crate::auth::parse_java_properties(String::from_utf8_lossy(body).as_bytes());
            configurations.insert(address, properties);
        }
        Ok(Some(configurations))
    }

    /// Updates Java controller configuration. Empty Properties or an absent
    /// controller list is a no-op; non-success replies are accumulated and the
    /// last error is returned after all controllers have been called.
    pub async fn update_controller_config(
        &self,
        properties: Option<&HashMap<String, String>>,
        controllers: Option<&[String]>,
        timeout_millis: i64,
    ) -> Result<(), RpcError> {
        let properties =
            properties.ok_or(RpcError::Header(ProtocolError::MissingField("properties")))?;
        if properties.is_empty() || controllers.is_none_or(<[_]>::is_empty) {
            return Ok(());
        }

        // Java Properties iteration order is unspecified. Keep one body for
        // every controller, while matching its key=value newline encoding.
        let mut body = String::new();
        for (key, value) in properties {
            body.push_str(key);
            body.push('=');
            body.push_str(value);
            body.push('\n');
        }
        let code = request::UPDATE_CONTROLLER_CONFIG;
        let mut last_error = None;
        for address in controllers.unwrap_or_default() {
            let reply = self
                .invoke_controller(
                    address,
                    code,
                    None,
                    Some(body.as_bytes().to_vec()),
                    timeout_millis,
                )
                .await?;
            if reply.code != response::SUCCESS {
                last_error = Some(RpcError::Response {
                    request_code: code,
                    peer: address.clone(),
                    code: reply.code,
                    remark: reply.remark,
                });
            }
        }
        last_error.map_or(Ok(()), Err)
    }

    /// Elects a Broker master through Java `MQClientAPIImpl.electMaster`.
    /// Controller metadata is read first and the request is sent to its leader.
    pub async fn elect_master(
        &self,
        controller_address: &str,
        cluster_name: &str,
        broker_name: &str,
        broker_id: Option<i64>,
    ) -> Result<(ElectMasterResponseHeader, Option<BrokerMemberGroup>), RpcError> {
        let metadata = self.get_controller_meta_data(controller_address).await?;
        let leader = controller_leader_address(
            metadata.controller_leader_address.as_deref(),
            request::CONTROLLER_ELECT_MASTER,
        )?;
        let mut fields = HeaderMap::new();
        fields.insert("clusterName".into(), cluster_name.to_owned());
        fields.insert("brokerName".into(), broker_name.to_owned());
        insert_optional_number(&mut fields, "brokerId", broker_id);
        fields.insert("designateElect".into(), "true".into());
        fields.insert("invokeTime".into(), now_millis().to_string());

        let reply = self
            .invoke_controller(
                leader,
                request::CONTROLLER_ELECT_MASTER,
                Some(fields),
                None,
                3000,
            )
            .await?;
        let reply = classify(reply, request::CONTROLLER_ELECT_MASTER, leader)?;
        let fields = reply.ext_fields.as_ref().cloned().unwrap_or_default();
        let header = ElectMasterResponseHeader::decode_fields(&fields).map_err(|error| {
            malformed_response(request::CONTROLLER_ELECT_MASTER, leader, error.to_string())
        })?;
        let broker_member_group = decode_nullable_body(
            reply.body.as_deref(),
            request::CONTROLLER_ELECT_MASTER,
            leader,
        )?;
        Ok((header, broker_member_group))
    }

    /// Cleans controller Broker metadata through Java
    /// `MQClientAPIImpl.cleanControllerBrokerData`, after resolving the current
    /// controller leader.
    pub async fn clean_controller_broker_data(
        &self,
        controller_address: &str,
        cluster_name: Option<&str>,
        broker_name: &str,
        broker_controller_ids_to_clean: Option<&str>,
        clean_living_broker: bool,
    ) -> Result<(), RpcError> {
        let metadata = self.get_controller_meta_data(controller_address).await?;
        let leader = controller_leader_address(
            metadata.controller_leader_address.as_deref(),
            request::CLEAN_BROKER_DATA,
        )?;
        let mut fields = HeaderMap::new();
        insert_optional(&mut fields, "clusterName", cluster_name);
        fields.insert("brokerName".into(), broker_name.to_owned());
        insert_optional(
            &mut fields,
            "brokerControllerIdsToClean",
            broker_controller_ids_to_clean,
        );
        fields.insert(
            "isCleanLivingBroker".into(),
            clean_living_broker.to_string(),
        );
        fields.insert("invokeTime".into(), now_millis().to_string());

        let reply = self
            .invoke_controller(leader, request::CLEAN_BROKER_DATA, Some(fields), None, 3000)
            .await?;
        classify(reply, request::CLEAN_BROKER_DATA, leader)?;
        Ok(())
    }

    async fn invoke_controller(
        &self,
        address: &str,
        code: i32,
        ext_fields: Option<HeaderMap>,
        body: Option<Vec<u8>>,
        timeout_millis: i64,
    ) -> Result<RemotingCommand, RpcError> {
        let reply = self
            .remoting
            .invoke_sync_with_timeout_millis(
                address,
                RemotingCommand {
                    code,
                    language: Some(LanguageCode::Java),
                    ext_fields: ext_fields.map(Into::into),
                    body,
                    ..java_client_request_command(code)?
                },
                timeout_millis,
            )
            .await?;
        Ok(reply)
    }
}

fn controller_leader_address(address: Option<&str>, request_code: i32) -> Result<&str, RpcError> {
    address.ok_or_else(|| {
        RpcError::Transport(
            rocketmq_classic_remoting::TransportError::InvalidConfiguration(format!(
                "controller metadata for request {request_code} has no leader address"
            )),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controller_dtos_decode_java_camel_case_and_nullable_fields() {
        let state: BrokerReplicasInfo = serde_json::from_str(
            r#"{"replicasInfoTable":{"broker-a":{"masterBrokerId":null,"masterAddress":"broker-a:10911","masterEpoch":7,"syncStateSetEpoch":8,"inSyncReplicas":[{"brokerName":"broker-a","brokerId":0,"brokerAddress":"broker-a:10911","alive":true}],"notInSyncReplicas":null}}}"#,
        )
        .unwrap();
        let replica = &state.replicas_info_table.unwrap()["broker-a"];
        assert_eq!(replica.master_broker_id, None);
        assert_eq!(replica.master_epoch, Some(7));
        assert_eq!(
            replica.in_sync_replicas.as_ref().unwrap()[0].alive,
            Some(true)
        );
        assert_eq!(replica.not_in_sync_replicas, None);

        let group: BrokerMemberGroup = serde_json::from_str(
            r#"{"cluster":"cluster-a","brokerName":"broker-a","brokerAddrs":{"5":"slave","0":"master"}}"#,
        )
        .unwrap();
        assert_eq!(group.minimum_broker_id(), Some(0));
        assert_eq!(BrokerMemberGroup::default().minimum_broker_id(), Some(0));

        let epoch: EpochEntry = serde_json::from_str(r#"{"epoch":4,"startOffset":10}"#).unwrap();
        assert_eq!(epoch.end_offset, i64::MAX);
    }

    #[test]
    fn response_headers_match_java_optional_and_primitive_defaults() {
        let metadata = GetMetaDataResponseHeader::decode_fields(&HeaderMap::new()).unwrap();
        assert!(!metadata.is_leader);
        assert_eq!(metadata.controller_leader_address, None);

        let election = ElectMasterResponseHeader::decode_fields(&HeaderMap::from([
            ("masterBrokerId".into(), "17".into()),
            ("masterEpoch".into(), "not-an-int".into()),
        ]));
        assert!(matches!(
            election,
            Err(ProtocolError::InvalidField {
                field: "masterEpoch",
                ..
            })
        ));
    }
}
