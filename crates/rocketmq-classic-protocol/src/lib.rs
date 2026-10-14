//! Typed RocketMQ protocol models and codecs.
//!
//! The Fastjson coercion layer used by the DTO implementations is crate-private.
//!
//! ```compile_fail
//! use rocketmq_classic_protocol::serde_fastjson;
//! # fn main() {}
//! ```

pub mod body;
pub mod codes;
pub mod header;
pub mod heartbeat;
pub mod namespace;
pub mod route;
pub(crate) mod serde_fastjson;
pub mod shared;

pub use namespace::NamespaceUtil;
pub use shared::SharedValue;
