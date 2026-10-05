#[path = "runtime.rs"]
mod instance;
mod manager;
mod registry;

pub use instance::ClientRuntime;
pub use manager::ClientRuntimeManager;
pub use registry::{ConsumerRuntime, GroupLease, ProducerRuntime};

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
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
