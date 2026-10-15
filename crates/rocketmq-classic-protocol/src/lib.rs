//! Typed RocketMQ protocol models and codecs.
//!
//! The Fastjson coercion layer used by the DTO implementations is crate-private.
//!
//! ```compile_fail
//! use rocketmq_classic_protocol::serde_fastjson;
//! # fn main() {}
//! ```
//!
//! Header field parsers and synthetic proof headers are implementation/test
//! helpers, not part of the public protocol API.
//!
//! ```compile_fail
//! use rocketmq_classic_protocol::header::{
//!     java_boolean, optional_i32, optional_i64, optional_java_boolean,
//!     optional_string, required_i32, required_i64, required_string, ProofHeader,
//! };
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
