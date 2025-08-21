use crate::cache::CacheConfig;
use crate::collection::Collection;
use crate::document::DocumentType;
use crate::wal::Wal;
use crate::wasp::{StorageEngine, Wasp};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

const DEFAULT_CACHE_CAPACITY: usize = 1024;

pub struct Engine {
    pub collections: RwLock<HashMap<String, Arc<Collection>>>,
    pub storage: Arc<RwLock<Box<dyn StorageEngine>>>,
}

impl Engine {
    pub fn new(wal_path: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let wal = Wal::new(wal_path)?;
        let engine = Self {
            collections: RwLock::new(HashMap::new()),
            storage: Arc::new(RwLock::new(Box::new(wal))),
        };

        // Create the hidden collection for temporary documents on startup.
        let temp_collection = engine.create_collection("_tempDocuments".to_string());

        // Load ephemeral documents from the WAL into the cache.
        {
            let storage = engine.storage.read();
            let operations = storage.read_all()?;
            for op in operations {
                if let Ok(operation) = op {
                    match operation {
                        crate::types::Operation::Insert { document } => {
                            if document.metadata.document_type == DocumentType::Ephemeral {
                                temp_collection.cache.insert(document);
                            }
                        }
                        // Handle other operations if necessary to reconstruct state
                        _ => {}
                    }
                }
            }
        }

        Ok(engine)
    }

    pub fn create_collection(&self, name: String) -> Arc<Collection> {
        let mut collections = self.collections.write();
        let collection = Arc::new(Collection::new(
            name.clone(),
            self.storage.clone(),
            DEFAULT_CACHE_CAPACITY,
        ));
        collections.insert(name, collection.clone());
        collection
    }

    pub fn create_collection_with_config(&self, name: String, config: CacheConfig) -> Arc<Collection> {
        let mut collections = self.collections.write();
        let collection = Arc::new(Collection::new_with_config(
            name.clone(),
            self.storage.clone(),
            config,
        ));
        collections.insert(name, collection.clone());
        collection
    }

    pub fn get_collection(&self, name: &str) -> Option<Arc<Collection>> {
        self.collections.read().get(name).cloned()
    }

    pub fn delete_collection(&self, name: &str) -> bool {
        self.collections.write().remove(name).is_some()
    }

    pub fn list_collection_names(&self) -> Vec<String> {
        self.collections.read().keys().cloned().collect()
    }

    pub fn rename_collection(&self, old: &str, new: &str) -> Result<(), crate::errors::DbError> {
        let mut map = self.collections.write();
        if !map.contains_key(old) {
            return Err(crate::errors::DbError::NoSuchCollection(old.to_string()));
        }
        if map.contains_key(new) {
            return Err(crate::errors::DbError::CollectionAlreadyExists(new.to_string()));
        }
    let col = map.remove(old).expect("checked above");
    col.set_name(new.to_string());
    map.insert(new.to_string(), col);
        Ok(())
    }
}

impl Engine {
    /// Construct an Engine backed by the WASP storage engine.
    pub fn with_wasp(path: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let wasp = Wasp::new(path)?;
        Ok(Self {
            collections: RwLock::new(HashMap::new()),
            storage: Arc::new(RwLock::new(Box::new(wasp))),
        })
    }
}