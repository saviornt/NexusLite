use crate::collection::Collection;
use crate::document::DocumentType;
use crate::cache::CacheConfig;
use crate::wal::Wal;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

const DEFAULT_CACHE_CAPACITY: usize = 1024;

pub struct Engine {
    pub collections: RwLock<HashMap<String, Arc<Collection>>>,
    pub wal: Arc<RwLock<Wal>>,
}

impl Engine {
    pub fn new(wal_path: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let wal = Arc::new(RwLock::new(Wal::new(wal_path)?));
        let engine = Self {
            collections: RwLock::new(HashMap::new()),
            wal,
        };

        // Create the hidden collection for temporary documents on startup.
        let temp_collection = engine.create_collection("_tempDocuments".to_string());

        // Load ephemeral documents from the WAL into the cache.
        {
            let wal_lock = engine.wal.read();
            let operations = wal_lock.read_all()?;
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
            self.wal.clone(),
            DEFAULT_CACHE_CAPACITY,
        ));
        collections.insert(name, collection.clone());
        collection
    }

    pub fn create_collection_with_config(&self, name: String, config: CacheConfig) -> Arc<Collection> {
        let mut collections = self.collections.write();
        let collection = Arc::new(Collection::new_with_config(
            name.clone(),
            self.wal.clone(),
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
}