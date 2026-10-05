mod control;
mod hooks;
mod inbound;
#[path = "runtime.rs"]
mod instance;
mod manager;
mod pull;
mod rebalance;
mod registry;
mod tasks;

pub use control::RuntimeState;
pub use instance::ClientRuntime;
pub use manager::ClientRuntimeManager;
pub use pull::MessageRequest;
pub use registry::{ConsumerOperation, ConsumerRuntime, GroupLease, ProducerRuntime};

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("NameServer addressing callback registration panicked")]
    AddressingCallbackFailed,
    #[error("heartbeat encoding failed: {0}")]
    HeartbeatEncoding(serde_json::Error),
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
