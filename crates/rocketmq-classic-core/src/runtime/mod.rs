mod client_check;
mod control;
mod hooks;
mod inbound;
#[path = "runtime.rs"]
mod instance;
mod manager;
mod pull;
mod rebalance;
mod registry;
mod tables;
mod tasks;

pub use control::RuntimeState;
pub use instance::ClientRuntime;
pub use manager::{ClientRuntimeManager, FactoryTable};
pub use pull::MessageRequest;
pub use registry::{ConsumerOperation, ConsumerRuntime, GroupLease, ProducerRuntime};
pub use tables::{
    ConsumerEntrySetView, ConsumerKeySetView, ConsumerTable, ConsumerValuesView,
    ProducerEntrySetView, ProducerKeySetView, ProducerTable, ProducerValuesView,
};

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error(
        "Check client in broker error, maybe because you use {expression_type} to filter message, but server has not been upgraded to support!This error would not affect the launch of consumer, but may has impact on message receiving if you have use the new features which are not supported by server, please check the log!"
    )]
    ClientCheck {
        expression_type: String,
        #[source]
        source: Box<RuntimeError>,
    },
    #[error("NameServer addressing callback registration panicked")]
    AddressingCallbackFailed,
    #[error("heartbeat encoding failed: {0}")]
    HeartbeatEncoding(serde_json::Error),
    #[error("invalid heartbeat consumer data: {0}")]
    InvalidHeartbeatData(&'static str),
    #[error("periodic interval must be positive")]
    InvalidSchedule,
    #[error("periodic callback panicked")]
    TaskCallbackFailed,
    #[error("runtime operation is invalid in state {0:?}")]
    InvalidState(RuntimeState),
    #[error("owned runtime control task failed")]
    ControlTaskFailed,
    #[error("runtime startup failed")]
    #[cfg(test)]
    StartupFailure,
    #[error("client group already registered: {group}")]
    DuplicateGroup { group: String },
    #[error("invalid topic route: {0}")]
    InvalidRoute(&'static str),
    #[error("invalid broker socket address")]
    InvalidBrokerAddress,
    #[error("runtime RPC failed: {0}")]
    Rpc(#[from] crate::error::RpcError),
    #[error("shared runtime configuration conflicts for client {client_id}")]
    ConfigurationConflict { client_id: String },
    #[error("runtime registry lock was poisoned")]
    RegistryPoisoned,
    #[error("runtime construction failed: {0}")]
    Transport(#[from] rocketmq_classic_remoting::TransportError),
}
