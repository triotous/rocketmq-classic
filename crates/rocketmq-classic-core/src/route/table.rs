use super::cache::{RouteCache, route_values_equal};
use crate::runtime::RuntimeError;
use rocketmq_classic_protocol::route::TopicRouteData;
use std::{collections::BTreeMap, sync::Arc};

/// Live raw topic map. Writes neither fetch nor validate/convert a route.
/// Values use Java route content equality, not owner registration identity.
/// The handle retains cache data only, never runtime workers or transport.
#[derive(Clone)]
pub struct TopicRouteTable(RouteCache);

impl TopicRouteTable {
    pub(crate) fn new(cache: RouteCache) -> Self {
        Self(cache)
    }
    pub fn get(&self, key: &str) -> Result<Option<Arc<TopicRouteData>>, RuntimeError> {
        Ok(self.0.snapshot()?.raw_routes.get(key).cloned())
    }
    /// Map.getOrDefault counterpart using raw-table presence only.
    pub fn get_or_default(
        &self,
        key: &str,
        default: Arc<TopicRouteData>,
    ) -> Result<Arc<TopicRouteData>, RuntimeError> {
        Ok(self.get(key)?.unwrap_or(default))
    }
    pub fn contains_key(&self, key: &str) -> Result<bool, RuntimeError> {
        Ok(self.0.snapshot()?.raw_routes.contains_key(key))
    }
    /// Uses the source route-content equality used by map-value comparisons.
    pub fn contains_value(&self, value: &TopicRouteData) -> Result<bool, RuntimeError> {
        Ok(self
            .0
            .snapshot()?
            .raw_routes
            .values()
            .any(|current| route_values_equal(current, value)))
    }
    pub fn len(&self) -> Result<usize, RuntimeError> {
        Ok(self.0.snapshot()?.raw_routes.len())
    }
    pub fn is_empty(&self) -> Result<bool, RuntimeError> {
        Ok(self.len()? == 0)
    }
    /// Detached owned iteration copy; retaining this table handle stays live.
    pub fn snapshot(&self) -> Result<BTreeMap<String, Arc<TopicRouteData>>, RuntimeError> {
        Ok(self.0.snapshot()?.raw_routes.clone())
    }
    /// Visits a detached map snapshot without holding the route-cache lock.
    /// Java's weakly consistent live traversal is not represented here.
    pub fn for_each(
        &self,
        mut action: impl FnMut(&str, &Arc<TopicRouteData>),
    ) -> Result<(), RuntimeError> {
        let snapshot = self.snapshot()?;
        for (key, value) in &snapshot {
            action(key, value);
        }
        Ok(())
    }
    pub fn put(
        &self,
        key: impl Into<String>,
        value: Arc<TopicRouteData>,
    ) -> Result<Option<Arc<TopicRouteData>>, RuntimeError> {
        let key = key.into();
        self.0.update_raw(|map| (true, map.insert(key, value)))
    }
    /// Writes all entries into the raw table without converting route snapshots.
    pub fn put_all(
        &self,
        entries: BTreeMap<String, Arc<TopicRouteData>>,
    ) -> Result<(), RuntimeError> {
        if entries.is_empty() {
            return Ok(());
        }
        self.0.update_raw(|map| {
            map.extend(entries);
            (true, ())
        })
    }
    pub fn put_if_absent(
        &self,
        key: impl Into<String>,
        value: Arc<TopicRouteData>,
    ) -> Result<Option<Arc<TopicRouteData>>, RuntimeError> {
        let key = key.into();
        self.0.update_raw(|map| {
            if let Some(previous) = map.get(&key) {
                (false, Some(previous.clone()))
            } else {
                map.insert(key, value);
                (true, None)
            }
        })
    }
    /// Applies a Java-style ConcurrentMap remapping function. Conditional
    /// updates use source route-content equality and may retry after a race.
    pub fn compute_if_absent(
        &self,
        key: impl Into<String>,
        mut mapping: impl FnMut(&str) -> Option<Arc<TopicRouteData>>,
    ) -> Result<Option<Arc<TopicRouteData>>, RuntimeError> {
        let key = key.into();
        if let Some(current) = self.get(&key)? {
            return Ok(Some(current));
        }
        let Some(value) = mapping(&key) else {
            return Ok(None);
        };
        Ok(Some(
            self.put_if_absent(&key, value.clone())?.unwrap_or(value),
        ))
    }

    pub fn compute_if_present(
        &self,
        key: impl Into<String>,
        mut remapping: impl FnMut(&str, &Arc<TopicRouteData>) -> Option<Arc<TopicRouteData>>,
    ) -> Result<Option<Arc<TopicRouteData>>, RuntimeError> {
        let key = key.into();
        loop {
            let Some(current) = self.get(&key)? else {
                return Ok(None);
            };
            match remapping(&key, &current) {
                Some(value) => {
                    if self.replace_if_equal(&key, &current, value.clone())? {
                        return Ok(Some(value));
                    }
                }
                None => {
                    if self.remove_if_equal(&key, &current)? {
                        return Ok(None);
                    }
                }
            }
        }
    }

    pub fn compute(
        &self,
        key: impl Into<String>,
        mut remapping: impl FnMut(&str, Option<&Arc<TopicRouteData>>) -> Option<Arc<TopicRouteData>>,
    ) -> Result<Option<Arc<TopicRouteData>>, RuntimeError> {
        let key = key.into();
        loop {
            let current = self.get(&key)?;
            let value = remapping(&key, current.as_ref());
            match (current.as_ref(), value) {
                (None, None) => return Ok(None),
                (None, Some(value)) => {
                    if self.put_if_absent(&key, value.clone())?.is_none() {
                        return Ok(Some(value));
                    }
                }
                (Some(current), Some(value)) => {
                    if self.replace_if_equal(&key, current, value.clone())? {
                        return Ok(Some(value));
                    }
                }
                (Some(current), None) => {
                    if self.remove_if_equal(&key, current)? {
                        return Ok(None);
                    }
                }
            }
        }
    }

    pub fn merge(
        &self,
        key: impl Into<String>,
        value: Arc<TopicRouteData>,
        mut remapping: impl FnMut(
            &Arc<TopicRouteData>,
            &Arc<TopicRouteData>,
        ) -> Option<Arc<TopicRouteData>>,
    ) -> Result<Option<Arc<TopicRouteData>>, RuntimeError> {
        let key = key.into();
        loop {
            let Some(current) = self.get(&key)? else {
                if self.put_if_absent(&key, value.clone())?.is_none() {
                    return Ok(Some(value));
                }
                continue;
            };
            match remapping(&current, &value) {
                Some(next) => {
                    if self.replace_if_equal(&key, &current, next.clone())? {
                        return Ok(Some(next));
                    }
                }
                None => {
                    if self.remove_if_equal(&key, &current)? {
                        return Ok(None);
                    }
                }
            }
        }
    }

    /// Replaces values for keys in one detached key snapshot. The mapping
    /// callback runs without a cache lock and may be retried after a race.
    pub fn replace_all(
        &self,
        mut remapping: impl FnMut(&str, &Arc<TopicRouteData>) -> Arc<TopicRouteData>,
    ) -> Result<(), RuntimeError> {
        for key in self.snapshot()?.into_keys() {
            loop {
                let Some(current) = self.get(&key)? else {
                    break;
                };
                let value = remapping(&key, &current);
                if self.replace_if_equal(&key, &current, value)? {
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn replace(
        &self,
        key: impl Into<String>,
        value: Arc<TopicRouteData>,
    ) -> Result<Option<Arc<TopicRouteData>>, RuntimeError> {
        let key = key.into();
        self.0.update_raw(|map| {
            if let std::collections::btree_map::Entry::Occupied(mut entry) = map.entry(key) {
                (true, Some(entry.insert(value)))
            } else {
                (false, None)
            }
        })
    }
    pub fn replace_if_equal(
        &self,
        key: impl Into<String>,
        expected: &TopicRouteData,
        value: Arc<TopicRouteData>,
    ) -> Result<bool, RuntimeError> {
        let key = key.into();
        self.0.update_raw(|map| {
            let matched = map
                .get(&key)
                .is_some_and(|current| route_values_equal(current, expected));
            if matched {
                map.insert(key, value);
            }
            (matched, matched)
        })
    }
    pub fn remove(&self, key: &str) -> Result<Option<Arc<TopicRouteData>>, RuntimeError> {
        self.0.update_raw(|map| {
            let previous = map.remove(key);
            (previous.is_some(), previous)
        })
    }
    pub fn remove_if_equal(
        &self,
        key: &str,
        expected: &TopicRouteData,
    ) -> Result<bool, RuntimeError> {
        self.0.update_raw(|map| {
            let matched = map
                .get(key)
                .is_some_and(|current| route_values_equal(current, expected));
            if matched {
                map.remove(key);
            }
            (matched, matched)
        })
    }
    pub fn clear(&self) -> Result<(), RuntimeError> {
        self.0.update_raw(|map| {
            let changed = !map.is_empty();
            map.clear();
            (changed, ())
        })
    }
}
