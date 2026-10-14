//! Shared RocketMQ message and utility types.
//!
//! JSON compatibility adapters are implementation details of the common and
//! protocol crates, not public helpers for downstream users.
//!
//! ```compile_fail
//! use rocketmq_classic_common::serde_fastjson::from_fastjson_str;
//! # fn main() {}
//! ```

pub mod compression;
pub mod message;
mod serde_fastjson;
pub mod sysflag;
pub mod topic;
