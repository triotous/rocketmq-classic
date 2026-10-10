use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    fmt,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

/// A cloneable, shared mutable DTO value.
///
/// Java DTO setters commonly retain an object reference and return that same
/// object from their getter. `SharedValue` provides that identity behavior for
/// Rust DTOs while keeping access synchronized and snapshots owned.
pub struct SharedValue<T>(Arc<RwLock<T>>);

impl<T> SharedValue<T> {
    pub fn new(value: T) -> Self {
        Self(Arc::new(RwLock::new(value)))
    }

    pub fn snapshot(&self) -> T
    where
        T: Clone,
    {
        self.read().clone()
    }

    pub fn with<R>(&self, read: impl FnOnce(&T) -> R) -> R {
        read(&self.read())
    }

    /// Runs a mutation visible to every clone of this handle.
    ///
    /// The closure must not re-enter this same handle. Poisoned locks recover
    /// the contained value, matching Java's continued access after an
    /// exception has interrupted a mutation.
    pub fn with_mut<R>(&self, update: impl FnOnce(&mut T) -> R) -> R {
        update(&mut self.write())
    }

    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    fn read(&self) -> RwLockReadGuard<'_, T> {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl<T> Clone for SharedValue<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> From<T> for SharedValue<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default> Default for SharedValue<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: fmt::Debug> fmt::Debug for SharedValue<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.read().fmt(formatter)
    }
}

impl<T: Serialize> Serialize for SharedValue<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.read().serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for SharedValue<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize(deserializer).map(Self::new)
    }
}
