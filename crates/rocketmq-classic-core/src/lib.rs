//! Core RocketMQ client runtime and request APIs.
//!
//! Physical message-ID decoding is provided by the Java-mapped Common API.
//! The Core crate does not expose a second decoder or decoder-specific DTO.
//!
//! ```compile_fail
//! use rocketmq_classic_core::api::admin::{
//!     DecodedMessageId, MessageIdDecodeError, decode_message_id,
//! };
//! # fn main() {}
//! ```

pub mod api;
pub mod auth;
pub mod config;
pub mod error;
mod heartbeat;
mod java_number;
pub mod namesrv;
pub mod route;
pub mod runtime;
#[cfg(test)]
mod test_support;
pub mod trace;
#[doc(hidden)]
pub mod unique_id;
pub use rocketmq_classic_protocol::NamespaceUtil;
pub use rocketmq_classic_remoting::client::RpcHook;
