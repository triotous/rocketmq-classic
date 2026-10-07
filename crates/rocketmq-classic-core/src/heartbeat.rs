use crate::runtime::{ClientRuntime, RuntimeError};
use rocketmq_classic_protocol::heartbeat::HeartbeatData;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, OnceLock, Weak},
    task::Poll,
};

#[derive(Default)]
pub(crate) struct HeartbeatChannelListener(OnceLock<Weak<ClientRuntime>>);
impl HeartbeatChannelListener {
    pub fn bind(&self, runtime: &Arc<ClientRuntime>) {
        let _ = self.0.set(Arc::downgrade(runtime));
    }
}
impl rocketmq_classic_remoting::client::ChannelEventListener for HeartbeatChannelListener {
    fn on_active_async<'a>(
        &'a self,
        address: &'a str,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let Some(runtime) = self.0.get().and_then(Weak::upgrade) else {
                return;
            };
            if matches!(
                runtime.state(),
                crate::runtime::RuntimeState::ShuttingDown
                    | crate::runtime::RuntimeState::Stopped
                    | crate::runtime::RuntimeState::Failed
            ) {
                return;
            }
            let Ok(routes) = runtime.route_snapshot() else {
                return;
            };
            for (broker, addresses) in &routes.broker_addresses {
                if let Some((id, _)) = addresses
                    .iter()
                    .find(|(_, value)| value.as_deref() == Some(address))
                {
                    if runtime
                        .send_heartbeat_to_broker_with_lock_mode(*id, broker, address, false)
                        .await
                        .unwrap_or(false)
                    {
                        runtime.rebalance_immediately();
                    }
                }
            }
        })
    }
}

#[derive(Default)]
pub(crate) struct HeartbeatState {
    gate: tokio::sync::Mutex<()>,
    peers: Mutex<PeerState>,
}

#[derive(Default)]
struct PeerState {
    support_v2: HashSet<String>,
    fingerprints: HashMap<String, i32>,
}

type PeerFuture<'a> = Pin<Box<dyn Future<Output = Result<bool, RuntimeError>> + Send + 'a>>;

/// The fingerprint is client-local change detection, not a server-computed
/// checksum. Canonical set order is Rust-native; Java HashSet iteration is not
/// part of the wire contract. Preserve Java's reset fields and UTF-16 hash.
fn fingerprint(data: &HeartbeatData) -> Result<i32, RuntimeError> {
    let mut value = serde_json::to_value(data).map_err(RuntimeError::HeartbeatEncoding)?;
    value["clientID"] = "".into();
    value["heartbeatFingerprint"] = 0.into();
    value["withoutSub"] = false.into();
    if let Some(consumers) = value["consumerDataSet"].as_array_mut() {
        for consumer in consumers {
            if let Some(subscriptions) = consumer["subscriptionDataSet"].as_array_mut() {
                for sub in subscriptions {
                    sub["subVersion"] = 0.into();
                }
            }
        }
    }
    fn canonicalize(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Array(values) => {
                for value in values.iter_mut() {
                    canonicalize(value);
                }
                values.sort_unstable_by_key(serde_json::Value::to_string);
            }
            serde_json::Value::Object(values) => {
                for value in values.values_mut() {
                    canonicalize(value);
                }
            }
            _ => {}
        }
    }
    canonicalize(&mut value);
    Ok(value.to_string().encode_utf16().fold(0i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    }))
}

impl ClientRuntime {
    fn prepare_heartbeat_for_send(&self) -> Result<Option<HeartbeatData>, RuntimeError> {
        // Java's public heartbeat wrappers contain an owner's exception from
        // prepareHeartbeatData and return false. Keep the directly exposed
        // preparation capability unchanged; contain failure at the send entry.
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.prepare_heartbeat_data(false)
        }))
        .map_or(Ok(None), |result| result.map(Some))
    }

    pub async fn send_heartbeat_to_broker(
        &self,
        id: i64,
        broker: &str,
        address: &str,
    ) -> Result<bool, RuntimeError> {
        self.send_heartbeat_to_broker_with_lock_mode(id, broker, address, true)
            .await
    }

    pub async fn send_heartbeat_to_broker_with_lock_mode(
        &self,
        _id: i64,
        broker: &str,
        address: &str,
        _strict_lock_mode: bool,
    ) -> Result<bool, RuntimeError> {
        let Ok(_guard) = self.heartbeat.gate.try_lock() else {
            return Ok(false);
        };
        let Some(mut full) = self.prepare_heartbeat_for_send()? else {
            return Ok(false);
        };
        if full.producer_data_set.is_empty() && full.consumer_data_set.is_empty() {
            return Ok(false);
        }
        let v2 = self.config().use_heartbeat_v2;
        if v2 {
            full.heartbeat_fingerprint = fingerprint(&full)?;
        }
        let mut simple = full.clone();
        simple.is_without_sub = true;
        simple.consumer_data_set = simple
            .consumer_data_set
            .into_iter()
            .map(|mut consumer| {
                consumer.subscription_data_set.clear();
                consumer
            })
            .collect();
        // Unlike the all-Broker round, the explicit single-peer method does not
        // skip a slave when only producers are registered (Java 5.5.1).
        self.send_heartbeat_peer(broker, address, &full, &simple, v2)
            .await
    }
    pub async fn send_heartbeat_to_all_brokers(&self) -> Result<bool, RuntimeError> {
        self.send_heartbeat_round(false, true).await
    }

    pub async fn send_heartbeat_to_all_brokers_with_rebalance(
        &self,
        is_rebalance: bool,
    ) -> Result<bool, RuntimeError> {
        // Java WithLockV2 uses serial V1 as its fallback, even when the
        // ordinary WithLock entry has concurrent V1 configured.
        self.send_heartbeat_round(is_rebalance, false).await
    }

    async fn send_heartbeat_round(
        &self,
        is_rebalance: bool,
        allow_concurrent_v1: bool,
    ) -> Result<bool, RuntimeError> {
        let Ok(_guard) = self.heartbeat.gate.try_lock() else {
            return Ok(false);
        };
        let Some(mut full) = self.prepare_heartbeat_for_send()? else {
            return Ok(false);
        };
        if full.producer_data_set.is_empty() && full.consumer_data_set.is_empty() {
            return Ok(false);
        }
        let routes = self.route_snapshot()?;
        if routes.broker_addresses.is_empty() {
            return Ok(false);
        }
        let v2 = self.config().use_heartbeat_v2;
        if v2 {
            full.heartbeat_fingerprint = fingerprint(&full)?;
            if is_rebalance {
                self.heartbeat
                    .peers
                    .lock()
                    .map_err(|_| RuntimeError::RegistryPoisoned)?
                    .fingerprints
                    .clear();
            }
        }
        let mut simple = full.clone();
        simple.is_without_sub = true;
        simple.consumer_data_set = simple
            .consumer_data_set
            .into_iter()
            .map(|mut consumer| {
                consumer.subscription_data_set.clear();
                consumer
            })
            .collect();
        let peers = routes
            .broker_addresses
            .iter()
            .flat_map(|(broker, addresses)| {
                addresses
                    .iter()
                    .filter(|(id, _)| !full.consumer_data_set.is_empty() || **id == 0)
                    .filter_map(|(_, address)| {
                        address.as_deref().map(|address| (broker.as_str(), address))
                    })
            })
            .collect::<Vec<_>>();
        if !v2 && allow_concurrent_v1 && self.config().enable_concurrent_heartbeat {
            if peers.is_empty() {
                return Ok(false);
            }
            let width = self.config().concurrent_heartbeat_thread_pool_size as usize;
            let mut pending = peers.into_iter();
            let mut active: Vec<PeerFuture<'_>> = Vec::new();
            loop {
                while active.len() < width {
                    let Some((broker, address)) = pending.next() else {
                        break;
                    };
                    active.push(Box::pin(
                        self.send_heartbeat_peer(broker, address, &full, &simple, false),
                    ));
                }
                if active.is_empty() {
                    break;
                }
                let (index, result) = std::future::poll_fn(|context| {
                    for (index, operation) in active.iter_mut().enumerate() {
                        if let Poll::Ready(result) = operation.as_mut().poll(context) {
                            return Poll::Ready((index, result));
                        }
                    }
                    Poll::Pending
                })
                .await;
                drop(active.swap_remove(index));
                result?;
            }
        } else {
            for (broker, address) in peers {
                self.send_heartbeat_peer(broker, address, &full, &simple, v2)
                    .await?;
            }
        }
        Ok(true)
    }

    async fn send_heartbeat_peer(
        &self,
        broker: &str,
        address: &str,
        full: &HeartbeatData,
        simple: &HeartbeatData,
        v2: bool,
    ) -> Result<bool, RuntimeError> {
        let deadline = tokio::time::Instant::now() + self.rpc_timeout();
        let version = if v2 {
            let reduced = {
                let peers = self
                    .heartbeat
                    .peers
                    .lock()
                    .map_err(|_| RuntimeError::RegistryPoisoned)?;
                peers.support_v2.contains(address)
                    && peers.fingerprints.get(address) == Some(&full.heartbeat_fingerprint)
            };
            let Ok(result) = self
                .api()
                .send_heartbeat_v2(address, if reduced { simple } else { full }, deadline)
                .await
            else {
                return Ok(false);
            };
            let mut peers = self
                .heartbeat
                .peers
                .lock()
                .map_err(|_| RuntimeError::RegistryPoisoned)?;
            if reduced {
                if result.is_sub_change {
                    peers.fingerprints.remove(address);
                }
            } else if result.is_support_heartbeat_v2 {
                peers.support_v2.insert(address.to_owned());
                if result.is_sub_change {
                    peers.fingerprints.remove(address);
                } else {
                    peers
                        .fingerprints
                        .insert(address.to_owned(), full.heartbeat_fingerprint);
                }
            }
            result.version
        } else {
            let Ok(version) = self.api().send_heartbeat(address, full, deadline).await else {
                return Ok(false);
            };
            version
        };
        self.record_broker_version(broker, address, version)
            .map(|_| true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_classic_protocol::heartbeat::{ConsumerData, SubscriptionData};

    #[test]
    fn singleton_fingerprints_match_invoked_java_oracle() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/heartbeat.json")).unwrap();
        for case in fixture["cases"].as_array().unwrap() {
            let data: HeartbeatData = serde_json::from_value(case["body"].clone()).unwrap();
            assert_eq!(
                i64::from(fingerprint(&data).unwrap()),
                case["fingerprint"].as_i64().unwrap(),
                "{}",
                case["case"]
            );
        }
    }

    #[test]
    fn fingerprint_ignores_identity_subscription_version_and_simple_flag() {
        let mut data = HeartbeatData::default();
        data.consumer_data_set.push(ConsumerData {
            group_name: "consumer".into(),
            subscription_data_set: std::collections::HashSet::from([SubscriptionData {
                topic: "topic".into(),
                sub_string: "*".into(),
                sub_version: 1,
                ..Default::default()
            }]),
            ..Default::default()
        });
        let expected = fingerprint(&data).unwrap();
        data.client_id = Some("another".into());
        data.heartbeat_fingerprint = 42;
        data.is_without_sub = true;
        let mut consumer = data.consumer_data_set.drain(..).next().unwrap();
        let mut sub = consumer.subscription_data_set.drain().next().unwrap();
        sub.sub_version = 99;
        consumer.subscription_data_set.insert(sub.clone());
        data.consumer_data_set.push(consumer.clone());
        assert_eq!(fingerprint(&data).unwrap(), expected);
        consumer.subscription_data_set.clear();
        sub.sub_string = "changed".into();
        consumer.subscription_data_set.insert(sub);
        data.consumer_data_set.clear();
        data.consumer_data_set.push(consumer);
        assert_ne!(fingerprint(&data).unwrap(), expected);
    }

    #[test]
    fn fingerprint_preserves_duplicate_multiplicity_without_registration_order() {
        let a = ConsumerData {
            group_name: "a".into(),
            ..Default::default()
        };
        let b = ConsumerData {
            group_name: "b".into(),
            ..Default::default()
        };
        let mut data = HeartbeatData {
            consumer_data_set: vec![a.clone(), b.clone()],
            ..Default::default()
        };
        let expected = fingerprint(&data).unwrap();
        data.consumer_data_set.reverse();
        assert_eq!(fingerprint(&data).unwrap(), expected);
        data.consumer_data_set.push(a);
        assert_ne!(fingerprint(&data).unwrap(), expected);
        data.consumer_data_set.push(b);
        let duplicated = fingerprint(&data).unwrap();
        data.consumer_data_set.reverse();
        assert_eq!(fingerprint(&data).unwrap(), duplicated);
    }
}
