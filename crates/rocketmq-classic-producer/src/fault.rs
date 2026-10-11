use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

use crate::config::{DEFAULT_LATENCY_MAX_MILLIS, DEFAULT_NOT_AVAILABLE_DURATION_MILLIS};
use rocketmq_classic_common::message::MessageQueue;

#[derive(Clone, Copy, Debug)]
struct FaultItem {
    current_latency_millis: u64,
    unavailable_until: Instant,
    check_after: Instant,
    reachable: bool,
}

/// Producer-side copy of the pinned Java latency and reachability selection
/// tables. The clock is passed into pure methods so boundary cases can be
/// checked without sleeping.
pub(crate) struct FaultStrategy {
    items: Mutex<HashMap<String, FaultItem>>,
    latency_max_millis: Vec<i64>,
    not_available_duration_millis: Vec<i64>,
}

impl Default for FaultStrategy {
    fn default() -> Self {
        Self::new(
            DEFAULT_LATENCY_MAX_MILLIS.to_vec(),
            DEFAULT_NOT_AVAILABLE_DURATION_MILLIS.to_vec(),
        )
    }
}

impl FaultStrategy {
    pub(crate) fn new(
        latency_max_millis: Vec<i64>,
        not_available_duration_millis: Vec<i64>,
    ) -> Self {
        Self {
            items: Mutex::new(HashMap::new()),
            latency_max_millis,
            not_available_duration_millis,
        }
    }

    pub(crate) fn update(
        &self,
        broker: &str,
        latency: Duration,
        isolation: bool,
        reachable: bool,
        now: Instant,
    ) {
        let duration_key = if isolation {
            10_000
        } else {
            latency.as_millis().min(u128::from(u64::MAX)) as u64
        };
        let duration = unavailable_duration(
            duration_key,
            &self.latency_max_millis,
            &self.not_available_duration_millis,
        )
        .max(0) as u64;
        let mut items = self
            .items
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = items.entry(broker.to_owned()).or_insert(FaultItem {
            current_latency_millis: 0,
            unavailable_until: now,
            check_after: now,
            reachable: true,
        });
        entry.current_latency_millis = latency.as_millis().min(u128::from(u64::MAX)) as u64;
        entry.unavailable_until = entry
            .unavailable_until
            .max(now + Duration::from_millis(duration));
        entry.reachable = reachable;
    }

    pub(crate) fn take_due_for_detection(&self, now: Instant, interval: Duration) -> Vec<String> {
        let mut items = self
            .items
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        items
            .iter_mut()
            .filter_map(|(broker, item)| {
                if item.check_after <= now {
                    item.check_after = now + interval;
                    Some(broker.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub(crate) fn mark_reachable(&self, broker: &str) {
        if let Some(item) = self
            .items
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(broker)
        {
            item.reachable = true;
        }
    }

    pub(crate) fn remove(&self, broker: &str) {
        self.items
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(broker);
    }

    pub(crate) fn select(
        &self,
        queues: &[MessageQueue],
        last_broker: Option<&str>,
        latency_fault_enabled: bool,
        mut next_index: impl FnMut(usize) -> usize,
        now: Instant,
    ) -> Option<MessageQueue> {
        if queues.is_empty() {
            return None;
        }
        let items = self
            .items
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let acceptable = |queue: &MessageQueue, avoid_last: bool| {
            let Some(broker) = queue.broker_name() else {
                return !avoid_last;
            };
            if avoid_last && last_broker == Some(broker) {
                return false;
            }
            if !latency_fault_enabled {
                return true;
            }
            items
                .get(broker)
                .is_none_or(|item| item.unavailable_until <= now && item.reachable)
        };

        if latency_fault_enabled {
            if let Some(queue) = choose_matching(queues, &acceptable, true, &mut next_index) {
                return Some(queue);
            }
            let reachable = |queue: &MessageQueue, avoid_last: bool| {
                let Some(broker) = queue.broker_name() else {
                    return !avoid_last;
                };
                if avoid_last && last_broker == Some(broker) {
                    return false;
                }
                items.get(broker).is_none_or(|item| item.reachable)
            };
            if let Some(queue) = choose_matching(queues, &reachable, true, &mut next_index) {
                return Some(queue);
            }
        }

        choose_matching(
            queues,
            &|queue, avoid_last| {
                if avoid_last && last_broker.is_some() && queue.broker_name() == last_broker {
                    return false;
                }
                true
            },
            true,
            &mut next_index,
        )
        .or_else(|| choose_matching(queues, &|_, _| true, false, &mut next_index))
    }

    #[cfg(test)]
    fn item(&self, broker: &str) -> Option<(u64, Instant, bool)> {
        self.items.lock().unwrap().get(broker).map(|item| {
            (
                item.current_latency_millis,
                item.unavailable_until,
                item.reachable,
            )
        })
    }
}

fn choose_matching(
    queues: &[MessageQueue],
    filter: &impl Fn(&MessageQueue, bool) -> bool,
    avoid_last: bool,
    next_index: &mut impl FnMut(usize) -> usize,
) -> Option<MessageQueue> {
    for _ in 0..queues.len() {
        let index = next_index(queues.len()) % queues.len();
        let queue = &queues[index];
        if filter(queue, avoid_last) {
            return Some(queue.clone());
        }
    }
    None
}

fn unavailable_duration(
    latency_millis: u64,
    latency_max_millis: &[i64],
    not_available_duration_millis: &[i64],
) -> i64 {
    (0..latency_max_millis.len())
        .rev()
        .find(|index| {
            let threshold = latency_max_millis[*index];
            threshold <= 0
                || u64::try_from(threshold).is_ok_and(|threshold| latency_millis >= threshold)
        })
        .and_then(|index| not_available_duration_millis.get(index).copied())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue(broker: &str, queue_id: i32) -> MessageQueue {
        MessageQueue::new("orders", broker, queue_id)
    }

    #[test]
    fn unavailable_duration_matches_java_latency_thresholds() {
        let cases = [
            (49, 0),
            (50, 0),
            (100, 0),
            (549, 0),
            (550, 2_000),
            (1_800, 5_000),
            (3_000, 6_000),
            (5_000, 10_000),
            (15_000, 30_000),
        ];
        for (latency, expected) in cases {
            assert_eq!(
                unavailable_duration(
                    latency,
                    &DEFAULT_LATENCY_MAX_MILLIS,
                    &DEFAULT_NOT_AVAILABLE_DURATION_MILLIS,
                ),
                expected,
                "{latency}"
            );
        }
    }

    #[test]
    fn fault_defaults_and_detector_contract_match_pinned_java_fixture() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let java = &fixture["faultStrategy"];
        let as_i64 = |values: &serde_json::Value| {
            values
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_i64().unwrap())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            as_i64(&java["latencyMaxMillis"]),
            DEFAULT_LATENCY_MAX_MILLIS
        );
        assert_eq!(
            as_i64(&java["notAvailableDurationMillis"]),
            DEFAULT_NOT_AVAILABLE_DURATION_MILLIS
        );
        assert_eq!(java["sendLatencyFaultEnable"], false);
        assert_eq!(java["startDetectorEnable"], false);
        assert_eq!(java["detectTimeoutMillis"], 200);
        assert_eq!(java["detectIntervalMillis"], 2_000);
        assert_eq!(java["scheduleInitialDelaySeconds"], 3);
        assert_eq!(java["schedulePeriodSeconds"], 3);
        assert_eq!(
            java["probeRequestCode"],
            rocketmq_classic_protocol::codes::request::GET_MAX_OFFSET
        );
        assert_eq!(java["probeQueueId"], 0);
        assert_eq!(java["probeCommitted"], true);
    }

    #[test]
    fn configured_latency_tables_control_unavailable_duration() {
        let now = Instant::now();
        let fault = FaultStrategy::new(vec![10, 20], vec![100, 300]);
        fault.update("broker-a", Duration::from_millis(25), false, true, now);
        assert_eq!(
            fault.item("broker-a").unwrap().1,
            now + Duration::from_millis(300)
        );
    }

    #[test]
    fn latency_selection_prefers_available_and_skips_the_previous_broker() {
        let now = Instant::now();
        let fault = FaultStrategy::default();
        fault.update("broker-a", Duration::from_millis(3_000), true, true, now);
        let queues = [queue("broker-a", 0), queue("broker-b", 0)];
        let picked = fault
            .select(&queues, Some("broker-b"), true, |_| 0, now)
            .unwrap();
        assert_eq!(picked.broker_name(), Some("broker-a"));
    }

    #[test]
    fn unavailable_reachable_brokers_are_fallbacks_before_all_routes() {
        let now = Instant::now();
        let fault = FaultStrategy::default();
        fault.update("broker-a", Duration::from_millis(3_000), true, true, now);
        let queues = [queue("broker-a", 0), queue("broker-b", 0)];
        let picked = fault
            .select(&queues, Some("broker-b"), true, |_| 0, now)
            .unwrap();
        assert_eq!(picked.broker_name(), Some("broker-a"));
        assert_eq!(fault.item("broker-a").unwrap().0, 3_000);
    }

    #[test]
    fn broker_becomes_available_at_the_isolation_deadline() {
        let now = Instant::now();
        let fault = FaultStrategy::default();
        fault.update("broker-a", Duration::from_millis(600), false, true, now);
        let queues = [queue("broker-a", 0), queue("broker-b", 0)];
        let mut index = 0;
        assert_eq!(
            fault
                .select(
                    &queues,
                    None,
                    true,
                    |length| {
                        let value = index;
                        index += 1;
                        value % length
                    },
                    now,
                )
                .unwrap()
                .broker_name(),
            Some("broker-b")
        );
        index = 0;
        assert_eq!(
            fault
                .select(
                    &queues,
                    None,
                    true,
                    |length| {
                        let value = index;
                        index += 1;
                        value % length
                    },
                    now + Duration::from_millis(1_999)
                )
                .unwrap()
                .broker_name(),
            Some("broker-b")
        );
        assert_eq!(
            fault
                .select(
                    &queues,
                    None,
                    true,
                    |_| 0,
                    now + Duration::from_millis(2_000)
                )
                .unwrap()
                .broker_name(),
            Some("broker-a")
        );
    }

    #[test]
    fn detector_candidates_are_rate_limited_and_success_restores_reachability() {
        let now = Instant::now();
        let fault = FaultStrategy::default();
        fault.update("broker-a", Duration::from_millis(600), true, false, now);
        assert_eq!(
            fault.take_due_for_detection(now, Duration::from_secs(2)),
            ["broker-a"]
        );
        assert!(
            fault
                .take_due_for_detection(now + Duration::from_millis(1_999), Duration::from_secs(2))
                .is_empty()
        );
        assert_eq!(
            fault.take_due_for_detection(now + Duration::from_secs(2), Duration::from_secs(2)),
            ["broker-a"]
        );
        fault.mark_reachable("broker-a");
        assert!(fault.item("broker-a").unwrap().2);
        fault.remove("broker-a");
        assert!(fault.item("broker-a").is_none());
    }

    #[test]
    fn unreachable_flag_is_preserved_until_a_success_or_detector_update() {
        let now = Instant::now();
        let fault = FaultStrategy::default();
        fault.update("broker-a", Duration::ZERO, true, false, now);
        assert!(!fault.item("broker-a").unwrap().2);
        fault.update("broker-a", Duration::ZERO, false, true, now);
        assert!(fault.item("broker-a").unwrap().2);
    }
}
