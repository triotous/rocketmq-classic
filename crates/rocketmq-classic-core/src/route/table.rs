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
    pub fn contains_key(&self, key: &str) -> Result<bool, RuntimeError> {
        Ok(self.0.snapshot()?.raw_routes.contains_key(key))
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
    pub fn put(
        &self,
        key: impl Into<String>,
        value: Arc<TopicRouteData>,
    ) -> Result<Option<Arc<TopicRouteData>>, RuntimeError> {
        let key = key.into();
        self.0.update_raw(|map| (true, map.insert(key, value)))
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
