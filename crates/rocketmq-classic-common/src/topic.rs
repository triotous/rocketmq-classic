//! Client-side topic classification matching RocketMQ Java 5.5.1.

use std::collections::HashSet;
use std::sync::{OnceLock, RwLock};

pub const AUTO_CREATE_TOPIC_KEY_TOPIC: &str = "TBW102";
pub const RMQ_SYS_SCHEDULE_TOPIC: &str = "SCHEDULE_TOPIC_XXXX";
pub const RMQ_SYS_BENCHMARK_TOPIC: &str = "BenchmarkTest";
pub const RMQ_SYS_TRANS_HALF_TOPIC: &str = "RMQ_SYS_TRANS_HALF_TOPIC";
pub const RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC: &str = "RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC";
pub const RMQ_SYS_TRACE_TOPIC: &str = "RMQ_SYS_TRACE_TOPIC";
pub const RMQ_SYS_TRANS_OP_HALF_TOPIC: &str = "RMQ_SYS_TRANS_OP_HALF_TOPIC";
pub const RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC: &str = "RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC";
pub const RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC: &str = "TRANS_CHECK_MAX_TIME_TOPIC";
pub const RMQ_SYS_SELF_TEST_TOPIC: &str = "SELF_TEST_TOPIC";
pub const RMQ_SYS_OFFSET_MOVED_EVENT: &str = "OFFSET_MOVED_EVENT";
pub const RMQ_SYS_ROCKSDB_OFFSET_TOPIC: &str = "CHECKPOINT_TOPIC";
pub const SYSTEM_TOPIC_PREFIX: &str = "rmq_sys_";
pub const SYNC_BROKER_MEMBER_GROUP_PREFIX: &str = "rmq_sys_SYNC_BROKER_MEMBER_";

const INITIAL_SYSTEM_TOPICS: &[&str] = &[
    AUTO_CREATE_TOPIC_KEY_TOPIC,
    RMQ_SYS_SCHEDULE_TOPIC,
    RMQ_SYS_BENCHMARK_TOPIC,
    RMQ_SYS_TRANS_HALF_TOPIC,
    RMQ_SYS_TRACE_TOPIC,
    RMQ_SYS_TRANS_OP_HALF_TOPIC,
    RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC,
    RMQ_SYS_SELF_TEST_TOPIC,
    RMQ_SYS_OFFSET_MOVED_EVENT,
    RMQ_SYS_ROCKSDB_OFFSET_TOPIC,
    RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC,
    RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC,
];

const INITIAL_NOT_ALLOWED_SEND_TOPICS: &[&str] = &[
    RMQ_SYS_SCHEDULE_TOPIC,
    RMQ_SYS_TRANS_HALF_TOPIC,
    RMQ_SYS_TRANS_OP_HALF_TOPIC,
    RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC,
    RMQ_SYS_SELF_TEST_TOPIC,
    RMQ_SYS_OFFSET_MOVED_EVENT,
    RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC,
    RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC,
];

fn system_topics() -> &'static RwLock<HashSet<String>> {
    static SYSTEM_TOPICS: OnceLock<RwLock<HashSet<String>>> = OnceLock::new();
    SYSTEM_TOPICS.get_or_init(|| {
        RwLock::new(
            INITIAL_SYSTEM_TOPICS
                .iter()
                .map(|topic| (*topic).to_owned())
                .collect(),
        )
    })
}

fn not_allowed_send_topics() -> &'static RwLock<HashSet<String>> {
    static NOT_ALLOWED_SEND_TOPICS: OnceLock<RwLock<HashSet<String>>> = OnceLock::new();
    NOT_ALLOWED_SEND_TOPICS.get_or_init(|| {
        RwLock::new(
            INITIAL_NOT_ALLOWED_SEND_TOPICS
                .iter()
                .map(|topic| (*topic).to_owned())
                .collect(),
        )
    })
}

/// Java's `TopicValidator` operations used by namespace and producer validation.
pub struct TopicValidator;

impl TopicValidator {
    pub const AUTO_CREATE_TOPIC_KEY_TOPIC: &'static str = crate::topic::AUTO_CREATE_TOPIC_KEY_TOPIC;
    pub const RMQ_SYS_SCHEDULE_TOPIC: &'static str = crate::topic::RMQ_SYS_SCHEDULE_TOPIC;
    pub const RMQ_SYS_BENCHMARK_TOPIC: &'static str = crate::topic::RMQ_SYS_BENCHMARK_TOPIC;
    pub const RMQ_SYS_TRANS_HALF_TOPIC: &'static str = crate::topic::RMQ_SYS_TRANS_HALF_TOPIC;
    pub const RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC: &'static str =
        crate::topic::RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC;
    pub const RMQ_SYS_TRACE_TOPIC: &'static str = crate::topic::RMQ_SYS_TRACE_TOPIC;
    pub const RMQ_SYS_TRANS_OP_HALF_TOPIC: &'static str = crate::topic::RMQ_SYS_TRANS_OP_HALF_TOPIC;
    pub const RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC: &'static str =
        crate::topic::RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC;
    pub const RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC: &'static str =
        crate::topic::RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC;
    pub const RMQ_SYS_SELF_TEST_TOPIC: &'static str = crate::topic::RMQ_SYS_SELF_TEST_TOPIC;
    pub const RMQ_SYS_OFFSET_MOVED_EVENT: &'static str = crate::topic::RMQ_SYS_OFFSET_MOVED_EVENT;
    pub const RMQ_SYS_ROCKSDB_OFFSET_TOPIC: &'static str =
        crate::topic::RMQ_SYS_ROCKSDB_OFFSET_TOPIC;
    pub const SYSTEM_TOPIC_PREFIX: &'static str = crate::topic::SYSTEM_TOPIC_PREFIX;
    pub const SYNC_BROKER_MEMBER_GROUP_PREFIX: &'static str =
        crate::topic::SYNC_BROKER_MEMBER_GROUP_PREFIX;

    pub fn is_system_topic(topic: &str) -> bool {
        topic.starts_with(SYSTEM_TOPIC_PREFIX)
            || system_topics()
                .read()
                .expect("system topic set lock poisoned")
                .contains(topic)
    }

    pub fn is_not_allowed_send_topic(topic: &str) -> bool {
        not_allowed_send_topics()
            .read()
            .expect("send topic set lock poisoned")
            .contains(topic)
    }

    pub fn add_system_topic(topic: impl Into<String>) {
        system_topics()
            .write()
            .expect("system topic set lock poisoned")
            .insert(topic.into());
    }

    /// Returns the live Java-style mutable set behind a lock.
    pub fn system_topic_set() -> &'static RwLock<HashSet<String>> {
        system_topics()
    }

    /// Returns the live Java-style mutable set behind a lock.
    pub fn not_allowed_send_topic_set() -> &'static RwLock<HashSet<String>> {
        not_allowed_send_topics()
    }
}

#[cfg(test)]
mod tests {
    use super::TopicValidator;

    #[test]
    fn added_system_topics_are_visible_to_all_classifiers() {
        let topic = "phase16-custom-system-topic";
        TopicValidator::add_system_topic(topic);
        assert!(TopicValidator::is_system_topic(topic));
        assert!(
            TopicValidator::system_topic_set()
                .read()
                .unwrap()
                .contains(topic)
        );
    }
}
