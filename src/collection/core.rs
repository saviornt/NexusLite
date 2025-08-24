use crate::cache::Cache;
use crate::index::IndexManager;
use crate::wasp::StorageEngine;
use parking_lot::RwLock;
use std::sync::Arc;

pub struct Collection {
    pub name: Arc<RwLock<String>>,
    pub cache: Cache,
    pub(crate) storage: Arc<RwLock<Box<dyn StorageEngine>>>,
    pub indexes: RwLock<IndexManager>,
    pub(crate) build_lock: RwLock<()>,
}

impl Collection {
    pub fn new(
        name: String,
        storage: Arc<RwLock<Box<dyn StorageEngine>>>,
        cache_capacity: usize,
    ) -> Self {
        Self {
            name: Arc::new(RwLock::new(name)),
            cache: Cache::new(cache_capacity),
            storage,
            indexes: RwLock::new(IndexManager::new()),
            build_lock: RwLock::new(()),
        }
    }

    pub fn new_with_config(
        name: String,
        storage: Arc<RwLock<Box<dyn StorageEngine>>>,
        config: crate::cache::CacheConfig,
    ) -> Self {
        Self {
            name: Arc::new(RwLock::new(name)),
            cache: Cache::new_with_config(config),
            storage,
            indexes: RwLock::new(IndexManager::new()),
            build_lock: RwLock::new(()),
        }
    }

    pub fn set_name(&self, new_name: String) {
        *self.name.write() = new_name;
    }

    /// Returns the collection's name as a String (cloned), hiding the `RwLock`.
    pub fn name_str(&self) -> String {
        self.name.read().clone()
    }
}
