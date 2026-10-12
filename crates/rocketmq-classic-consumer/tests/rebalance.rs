use std::collections::HashSet;

use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_consumer::rebalance::{
    AllocateMessageQueueAveragely, AllocateMessageQueueStrategy, RebalanceError,
};

const JAVA_ALLOCATION_FIXTURE: &str = include_str!("../../../fixtures/rebalance/allocation.json");

fn fixture_queue(value: &serde_json::Value) -> MessageQueue {
    MessageQueue::new(
        value["topic"].as_str().expect("fixture topic"),
        value["brokerName"].as_str().expect("fixture broker name"),
        value["queueId"].as_i64().expect("fixture queue id") as i32,
    )
}

fn queue_key(queue: &MessageQueue) -> String {
    format!(
        "{}|{}|{}",
        queue.topic().expect("fixture topic"),
        queue.broker_name().expect("fixture broker name"),
        queue.queue_id()
    )
}

fn java_string_cmp(left: &str, right: &str) -> std::cmp::Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

#[test]
fn average_allocation_matches_java_551_fixtures_and_partition_invariants() {
    let fixture: serde_json::Value =
        serde_json::from_str(JAVA_ALLOCATION_FIXTURE).expect("Java allocation fixture JSON");
    let strategy = AllocateMessageQueueAveragely;
    let mut checked_cases = 0;

    for case in fixture["cases"].as_array().expect("fixture cases") {
        if case["strategy"] != "AVG" {
            continue;
        }
        checked_cases += 1;
        let mut queues = case["queues"]
            .as_array()
            .expect("fixture queue list")
            .iter()
            .map(fixture_queue)
            .collect::<Vec<_>>();
        let mut consumer_ids = case["consumerIds"]
            .as_array()
            .expect("fixture client IDs")
            .iter()
            .map(|value| value.as_str().expect("fixture client ID").to_owned())
            .collect::<Vec<_>>();
        queues.sort();
        consumer_ids.sort_by(|left, right| java_string_cmp(left, right));

        let expected_validation = &case["validation"];
        assert_eq!(
            strategy.allocate("group", "", &queues, &consumer_ids),
            Err(RebalanceError::EmptyCurrentClient),
            "{} empty current client",
            case["id"]
        );
        assert_eq!(
            strategy.allocate("group", "consumer-a", &[], &consumer_ids),
            Err(RebalanceError::EmptyMessageQueueList),
            "{} empty queue list",
            case["id"]
        );
        let nonempty_validation_queues = if queues.is_empty() {
            vec![MessageQueue::new("validation", "broker", 0)]
        } else {
            queues.clone()
        };
        assert_eq!(
            strategy.allocate("group", "consumer-a", &nonempty_validation_queues, &[]),
            Err(RebalanceError::EmptyConsumerIdList),
            "{} empty consumer list",
            case["id"]
        );
        assert_eq!(
            expected_validation["emptyCurrentClient"], "currentCID is empty",
            "Java validation fixture for {}",
            case["id"]
        );
        assert_eq!(
            expected_validation["emptyQueueList"], "mqAll is null or mqAll empty",
            "Java validation fixture for {}",
            case["id"]
        );
        assert_eq!(
            expected_validation["emptyConsumerIdList"],
            if queues.is_empty() {
                "mqAll is null or mqAll empty"
            } else {
                "cidAll is null or cidAll empty"
            },
            "Java validation fixture for {}",
            case["id"]
        );

        if queues.is_empty() {
            for client_id in &consumer_ids {
                assert!(
                    strategy
                        .allocate("group", client_id, &queues, &consumer_ids)
                        .is_err()
                );
            }
            continue;
        }

        let mut all_assignments = Vec::new();
        for client_id in &consumer_ids {
            let expected = case["assignments"][client_id]
                .as_array()
                .expect("Java assignment list")
                .iter()
                .map(|value| value.as_str().expect("Java queue key"))
                .collect::<Vec<_>>();
            let actual = strategy
                .allocate("group", client_id, &queues, &consumer_ids)
                .expect("valid average allocation")
                .iter()
                .map(queue_key)
                .collect::<Vec<_>>();
            assert_eq!(actual, expected, "{} client {client_id}", case["id"]);
            all_assignments.extend(actual);
        }

        let unique = all_assignments.iter().cloned().collect::<HashSet<_>>();
        assert_eq!(unique.len(), all_assignments.len(), "disjoint assignments");
        let expected_queues = queues.iter().map(queue_key).collect::<HashSet<_>>();
        assert_eq!(unique, expected_queues, "all eligible queues assigned once");
        let missing_client = strategy
            .allocate("group", "missing-client", &queues, &consumer_ids)
            .expect("a client missing from cidAll receives an empty assignment");
        assert!(missing_client.is_empty());
    }

    assert_eq!(checked_cases, 5, "fixture covers zero/one/N/Unicode inputs");
}
