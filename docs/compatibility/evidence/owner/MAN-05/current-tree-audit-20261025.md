# MAN-05 source-backed feature audit and disposition

Date: 2026-10-25

Disposition: N/A for additional feature-specific owner validation. This does
not approve production use or close the separately listed MAN-01 through
MAN-04 owner-environment checks.

## Scope and pinned source

- Java oracle: Apache RocketMQ 5.5.1, source commit
  efa1be59d6fc747ad220f036c86030f05f0419f9.
- P18 final API/source audit:
  [PHASE_6_REQUIREMENT_AUDIT.md](../../../PHASE_6_REQUIREMENT_AUDIT.md), including
  132 MQClientAPIImpl declarations, seven overload groups, 29 direct RPC
  invocation cases, Rust reverse exports, configuration/settings, inputs,
  errors, and control flow.
- Remoting setting and public-surface inventory:
  [PHASE_3_REMOTING_INVENTORY.md](../../../PHASE_3_REMOTING_INVENTORY.md).
- P18 final closure code revision: 0cf294b710d06d40bd72ccec1ca5d1df121c5225.
- Current code revision under audit: 1e909440b39720e814a1db9d79d92bbc6d733713.

## Post-closure source/API delta

A comparison of the production crate trees between the P18 closure revision
and the current code revision found changes in PushConsumer subscription
handling and Remoting lifecycle diagnostics. No other production crate API,
configuration setting, or client feature changed in that range.

1. PushConsumer now filters a completed pull result against the current local
   subscription so an older long-poll snapshot cannot deliver a tag that the
   caller has replaced. On Broker response code 25, it makes one best-effort
   heartbeat refresh before following the ordinary pull retry path. This
   handles the observed case where the subscription update heartbeat lost the
   shared nonblocking lock.

   Java 5.5.1 source review: the pull callback logs
   SUBSCRIPTION_NOT_LATEST and schedules the pull request after its configured
   retry delay; periodic heartbeat scheduling is independent. Rust's refresh
   shortens convergence on this race and does not claim identical retry timing.
   The final user-visible subscription and Broker-side tag-filtering result is
   covered by the current P6-V04 evidence at
   [20261025T213618Z](../../phase-19/P6-V04/20261025T213618Z/). The relevant
   Java sources are
   [DefaultMQPushConsumerImpl.java](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.1/client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultMQPushConsumerImpl.java#L2925-L2950)
   and
   [MQClientInstance.java](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.1/client/src/main/java/org/apache/rocketmq/client/impl/factory/MQClientInstance.java#L3200-L3235).
   This narrow retry-timing delta is also recorded in
   [KNOWN_DIFFERENCES.md](../../../KNOWN_DIFFERENCES.md).

2. Remoting adds the public RemotingLifecycleCounts type and
   RemotingClient::lifecycle_counts() read-only snapshot with pending request,
   connection, and owned-task counts. Java has no matching public method. The
   method is used for lifecycle diagnostics after shutdown and does not add a
   wire command, setting, or transport control. The API is listed in the
   [Remoting inventory](../../../PHASE_3_REMOTING_INVENTORY.md) as an explicit
   Rust-native diagnostic extension.

These deltas are classified in the current compatibility inventory. They do
not reveal an unimplemented client capability or a setting without a Java
source mapping. The full aggregate evidence at
[20261025T203947Z](../../phase-19/run-local/20261025T203947Z/) records all
23 local gates passing at the current code revision; the differential manifest
records PASS for all 37 registered scenarios, including the latest P6-V04 run.

## MAN-05 decision

No additional feature-specific validation requires an owner-only service or
topology. The requirements that are inherently owner-specific are separately
tracked: MAN-01 network/DNS/VIP paths, MAN-02 organization security material
and ACL policy, MAN-03 authorized HA fault topology, and MAN-04 representative
workload and SLO capacity. Their absence is not an unclassified product
feature and does not change MAN-05 from N/A. No owner or production
environment was accessed for this audit.
