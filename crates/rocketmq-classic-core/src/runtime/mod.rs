#[path = "runtime.rs"]
mod instance;
mod manager;

pub use instance::ClientRuntime;
pub use manager::ClientRuntimeManager;

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("shared runtime configuration conflicts for client {client_id}")]
    ConfigurationConflict { client_id: String },
    #[error("runtime registry lock was poisoned")]
    RegistryPoisoned,
    #[error("runtime construction failed: {0}")]
    Transport(#[from] rocketmq_classic_remoting::TransportError),
}
