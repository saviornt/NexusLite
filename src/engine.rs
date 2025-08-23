use crate::cache::CacheConfig;
use crate::collection::Collection;
use crate::document::DocumentType;
use crate::wal::Wal;
use crate::wasp::{StorageEngine, Wasp};
use crate::index::{IndexDescriptor, INDEX_METADATA_VERSION, IndexKind};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::fs;
use serde::{Serialize, Deserialize};

const DEFAULT_CACHE_CAPACITY: usize = 1024;

pub struct Engine {
    pub collections: RwLock<HashMap<String, Arc<Collection>>>,
    pub storage: Arc<RwLock<Box<dyn StorageEngine>>>,
    metadata_path: PathBuf,
}

impl Engine {
    /// Create a new Engine backed by the WAL storage engine.
    ///
    /// # Errors
    /// Returns an error if the underlying storage engine fails to initialize.
    pub fn new(wal_path: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
    let wal = Wal::new(wal_path)?;
    // Resolve and cache metadata path at engine creation to avoid env var races in tests
    let metadata_path = std::env::var("NEXUS_INDEX_META")
        .map_or_else(|_| PathBuf::from("nexus_indexes.json"), PathBuf::from);
    let metadata_path = if metadata_path.is_absolute() { metadata_path } else { std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")).join(metadata_path) };
        let engine = Self {
            collections: RwLock::new(HashMap::new()),
            storage: Arc::new(RwLock::new(Box::new(wal))),
            metadata_path,
        };
    // Initialize ephemeral handling
    engine.init_ephemeral_cache()?;

    // Rebuild indexes from metadata if present
    engine.load_indexes_metadata();
    Ok(engine)
    }

    pub fn create_collection(&self, name: String) -> Arc<Collection> {
        let collection = Arc::new(Collection::new(
            name.clone(),
            self.storage.clone(),
            DEFAULT_CACHE_CAPACITY,
        ));
        self.collections.write().insert(name, collection.clone());
        // Attempt to rebuild indexes for this collection if metadata exists
        self.load_collection_indexes(&collection);
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

    /// Rename an existing collection.
    ///
    /// # Errors
    /// Returns `NoSuchCollection` if `old` doesn't exist or `CollectionAlreadyExists` if `new` already exists.
    ///
    /// # Panics
    /// Panics only if the internal insertion into the collection map fails during re-insertion, which
    /// should not occur under normal operation.
    pub fn rename_collection(&self, old: &str, new: &str) -> Result<(), crate::errors::DbError> {
        let (mut col, mut should_insert) = (None, false);
        {
            let mut map = self.collections.write();
            if !map.contains_key(old) {
                return Err(crate::errors::DbError::NoSuchCollection(old.to_string()));
            }
            if map.contains_key(new) {
                return Err(crate::errors::DbError::CollectionAlreadyExists(new.to_string()));
            }
            if let Some(c) = map.remove(old) { col = Some(c); should_insert = true; }
        }
        if should_insert {
            if let Some(c) = &col { c.set_name(new.to_string()); }
            self.collections.write().insert(new.to_string(), col.unwrap());
        }
        Ok(())
    }
}

impl Engine {
    /// Construct an Engine backed by the WASP storage engine.
    ///
    /// # Errors
    /// Returns an error if the storage engine fails to initialize or if ephemeral cache setup fails.
    pub fn with_wasp(path: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
    let wasp = Wasp::new(path)?;
    // Resolve and cache metadata path at engine creation to avoid env var races in tests
    let metadata_path = std::env::var("NEXUS_INDEX_META")
        .map_or_else(|_| PathBuf::from("nexus_indexes.json"), PathBuf::from);
    let metadata_path = if metadata_path.is_absolute() { metadata_path } else { std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")).join(metadata_path) };
        let engine = Self {
            collections: RwLock::new(HashMap::new()),
            storage: Arc::new(RwLock::new(Box::new(wasp))),
        metadata_path,
        };
        // Initialize ephemeral handling
        engine.init_ephemeral_cache()?;
        // Rebuild indexes from metadata if present
        engine.load_indexes_metadata();
        Ok(engine)
    }
}

impl Engine {
    /// Ensure the ephemeral collection exists and preload ephemeral docs from storage.
    /// # Errors
    /// Returns an error if reading operations from the storage engine fails.
    fn init_ephemeral_cache(&self) -> Result<(), Box<dyn std::error::Error>> {
        let temp_collection = self.create_collection("_tempDocuments".to_string());
        let operations = self.storage.read().read_all()?;
        for operation in operations.into_iter().flatten() {
            if let crate::types::Operation::Insert { document } = operation
                && document.metadata.document_type == DocumentType::Ephemeral
            {
                temp_collection.cache.insert(document);
            }
        }
        Ok(())
    }
    fn indexes_meta_path(&self) -> PathBuf { self.metadata_path.clone() }

    pub fn load_indexes_metadata(&self) {
        let path = self.indexes_meta_path();
        if let Ok(bytes) = fs::read(&path) {
            if let Ok(mut meta) = serde_json::from_slice::<IndexesMetadata>(&bytes) {
                for (col_name, descs) in meta.collections.clone() {
                    let col = self
                        .get_collection(&col_name)
                        .map_or_else(|| self.create_collection(col_name.clone()), |c| c);
                    for d in descs { col.create_index(&d.field, d.kind); }
                }
                if meta.version != INDEX_METADATA_VERSION {
                    meta.version = INDEX_METADATA_VERSION;
                    let _ = fs::write(&path, serde_json::to_vec_pretty(&meta).unwrap_or_default());
                }
            } else if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                // Fallback tolerant parse for legacy shapes
                let mut collections: HashMap<String, Vec<IndexDescriptor>> = HashMap::new();
                if let Some(map) = val.get("collections").and_then(|v| v.as_object()) {
                    for (cname, arr) in map {
                        if let Some(items) = arr.as_array() {
                            let mut v = Vec::new();
                            for it in items {
                                let field = it.get("field").and_then(|x| x.as_str()).unwrap_or("").to_string();
                                let kind_str = it.get("kind").and_then(|x| x.as_str()).unwrap_or("");
                                let kind = match kind_str {
                                    "BTree" | "btree" | "Btree" => IndexKind::BTree,
                                    _ => IndexKind::Hash,
                                };
                                v.push(IndexDescriptor { field, kind });
                            }
                            collections.insert(cname.clone(), v);
                        }
                    }
                }
                for (col_name, descs) in collections.clone() {
                    let col = self
                        .get_collection(&col_name)
                        .map_or_else(|| self.create_collection(col_name.clone()), |c| c);
                    for d in descs { col.create_index(&d.field, d.kind); }
                }
                let meta = IndexesMetadata { version: INDEX_METADATA_VERSION, collections };
                let _ = fs::write(&path, serde_json::to_vec_pretty(&meta).unwrap_or_default());
            }
        }
    }

    /// # Errors
    /// Returns an error if writing index metadata to disk fails.
    pub fn save_indexes_metadata(&self) -> std::io::Result<()> {
        let mut collections_meta: HashMap<String, Vec<IndexDescriptor>> = HashMap::new();
        for (name, col) in self.collections.read().iter() {
            let mgr = col.indexes.read();
            collections_meta.insert(name.clone(), mgr.descriptors());
        }
        let meta = IndexesMetadata { version: INDEX_METADATA_VERSION, collections: collections_meta };
        fs::write(self.indexes_meta_path(), serde_json::to_vec_pretty(&meta).unwrap_or_default())
    }

    fn load_collection_indexes(&self, col: &Arc<Collection>) {
        let path = self.indexes_meta_path();
        if let Ok(bytes) = fs::read(&path)
            && let Ok(meta) = serde_json::from_slice::<IndexesMetadata>(&bytes) {
            if meta.version != INDEX_METADATA_VERSION { return; }
            let name = col.name_str();
            if let Some(descs) = meta.collections.get(&name) {
                for d in descs { col.create_index(&d.field, d.kind); }
            }
        }
    }

    /// Persist a checkpoint of data and index metadata into the main DB file when using WASP.
    /// # Errors
    /// Returns an error if the underlying storage engine checkpoint fails.
    pub fn checkpoint_with_indexes(&self, db_path: &std::path::Path) -> std::io::Result<()> {
        // Collect index descriptors per collection
        let mut map: HashMap<String, Vec<IndexDescriptor>> = HashMap::new();
        for (name, col) in self.collections.read().iter() {
            let mgr = col.indexes.read();
            map.insert(name.clone(), mgr.descriptors());
        }
        // Delegate to storage engine
    self.storage.write().checkpoint_with_meta(db_path, map)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexesMetadata {
    version: u32,
    collections: HashMap<String, Vec<IndexDescriptor>>,
}