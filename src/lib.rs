#![forbid(unsafe_code)]

pub mod api;
pub mod cache;
pub mod cli;
pub mod collection;
pub mod crypto;
pub mod document;
pub mod engine;
pub mod errors;
pub mod export;
pub mod import;
pub mod query;
pub mod logger;
pub mod types;
pub mod wal;
pub mod wasp;
pub mod index;


use crate::collection::Collection;
use crate::document::Document;
use crate::engine::Engine;
use crate::errors::DbError;
use crate::types::DocumentId;
use std::path::Path;
use std::sync::Arc;
use std::sync::LazyLock;
use bincode::serde::decode_from_slice;
use bincode::config::standard;
use std::path::PathBuf;
use std::collections::HashMap;

/// The main database struct.
pub struct Database {
    engine: Arc<Engine>,
    name: String,
}

impl Database {
    /// Create a database, optionally at the provided file path/name.
    /// - If `name_or_path` is Some and non-empty, it is used (extension defaults to .db if missing).
    /// - If None or empty, defaults to `nexuslite.db` in the current directory.
    /// This ensures the main `.db` file exists and also creates the `.wasp` file if missing.
    pub fn new(name_or_path: Option<&str>) -> Result<Self, DbError> {
        let db_path_buf = normalize_db_path(name_or_path);
        let db_path = db_path_buf.as_path();
        let wasp_path = db_path.with_extension("wasp");

        // Create the main database file if it doesn't exist
        if !db_path.exists() {
            std::fs::File::create(db_path)
                .map_err(|e| DbError::Io(format!("Failed to create database file: {}", e)))?;
        }
        // Create the WASP file if it doesn't exist
        if !wasp_path.exists() {
            std::fs::File::create(&wasp_path)
                .map_err(|e| DbError::Io(format!("Failed to create WASP file: {e}")))?;
        }

        // Initialize logging next to DB: {db_dir}/{db_stem}_logs/{db_stem}.log
        if let Some(stem) = db_path.file_stem().and_then(|s| s.to_str()) {
            let base = db_path.parent().unwrap_or_else(|| std::path::Path::new("."));
            let _ = crate::logger::init_for_db_in(base, stem);
        }

        let engine = Engine::with_wasp(wasp_path)
            .map_err(|e| DbError::Io(e.to_string()))?;

        let engine_arc = Arc::new(engine);
        crate::register_engine(&engine_arc);

        // If a snapshot exists in the .db file, load index descriptors and ensure indexes exist
        if let Ok(bytes) = std::fs::read(db_path) {
            if let Ok((snap, _)) = decode_from_slice::<crate::wasp::DbSnapshot, _>(&bytes, standard()) {
                for (cname, descs) in &snap.indexes {
                    let col = engine_arc.get_collection(cname).unwrap_or_else(|| engine_arc.create_collection(cname.clone()));
                    for d in descs { col.create_index(&d.field, d.kind); }
                }
            }
        }

        let name = db_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("nexuslite")
            .to_string();

    let db = Self { engine: engine_arc.clone(), name };
        // Register as opened
        register_db(&db_path_buf, &engine_arc);
        Ok(db)
    }

    /// Open an existing database and ensure the associated `.wasp` file exists.
    ///
    /// The main database is stored at `{filepath}` and the WASP engine state at `{filepath}.wasp`.
    /// If the main `.db` file does not exist, returns `DbError::DatabaseNotFound`.
    /// If the DB file doesn't exist, returns DbError::DatabaseNotFound.
    pub fn open(name_or_path: &str) -> Result<Self, DbError> {
        let db_path_buf = normalize_db_path(Some(name_or_path));
        let db_path = db_path_buf.as_path();
        if !db_path.exists() { return Err(DbError::DatabaseNotFound); }
        let wasp_path = db_path.with_extension("wasp");
        // Create the WASP file if it doesn't exist
        if !wasp_path.exists() {
            std::fs::File::create(&wasp_path)
                .map_err(|e| DbError::Io(format!("Failed to create WASP file: {e}")))?;
        }
        // Initialize logging next to DB: {db_dir}/{db_stem}_logs/{db_stem}.log
        if let Some(stem) = db_path.file_stem().and_then(|s| s.to_str()) {
            let base = db_path.parent().unwrap_or_else(|| std::path::Path::new("."));
            let _ = crate::logger::init_for_db_in(base, stem);
        }
        let engine = Engine::with_wasp(wasp_path)
            .map_err(|e| DbError::Io(e.to_string()))?;
        let engine_arc = Arc::new(engine);
        crate::register_engine(&engine_arc);
        if let Ok(bytes) = std::fs::read(db_path) {
            if let Ok((snap, _)) = decode_from_slice::<crate::wasp::DbSnapshot, _>(&bytes, standard()) {
                for (cname, descs) in &snap.indexes {
                    let col = engine_arc.get_collection(cname).unwrap_or_else(|| engine_arc.create_collection(cname.clone()));
                    for d in descs { col.create_index(&d.field, d.kind); }
                }
            }
        }
        let name = db_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("nexuslite")
            .to_string();
    let db = Self { engine: engine_arc.clone(), name };
        register_db(&db_path_buf, &engine_arc);
        Ok(db)
    }

    // open_with_name removed: callers should pass the desired path; name derives from file stem.

    /// Creates a new collection with the given name.
    pub fn create_collection(&self, name: &str) -> Arc<Collection> {
        self.engine.create_collection(name.to_string())
    }

    /// Retrieves a collection by its name.
    pub fn get_collection(&self, name: &str) -> Option<Arc<Collection>> {
        self.engine.get_collection(name)
    }

    /// Deletes a collection by its name.
    pub fn delete_collection(&self, name: &str) -> bool {
        self.engine.delete_collection(name)
    }

    /// Inserts a document into the specified collection.
    pub fn insert_document(&self, collection_name: &str, document: Document) -> Result<DocumentId, DbError> {
        let collection = self.engine.get_collection(collection_name)
            .ok_or(DbError::NoSuchCollection(collection_name.to_string()))?;
        Ok(collection.insert_document(document))
    }

    /// Updates a document in the specified collection.
    pub fn update_document(&self, collection_name: &str, document_id: &DocumentId, new_document: Document) -> Result<bool, DbError> {
        let collection = self.engine.get_collection(collection_name)
            .ok_or(DbError::NoSuchCollection(collection_name.to_string()))?;
        Ok(collection.update_document(document_id, new_document))
    }

    /// Deletes a document from the specified collection by its ID.
    pub fn delete_document(&self, collection_name: &str, document_id: &DocumentId) -> Result<bool, DbError> {
        let collection = self.engine.get_collection(collection_name)
            .ok_or(DbError::NoSuchCollection(collection_name.to_string()))?;
        Ok(collection.delete_document(document_id))
    }

    /// Lists the names of all collections.
    pub fn list_collection_names(&self) -> Vec<String> {
        self.engine.list_collection_names()
    }

    /// Rename a collection.
    pub fn rename_collection(&self, old: &str, new: &str) -> Result<(), DbError> {
        self.engine.rename_collection(old, new)
    }

    // --- Query API (façade over query module) ---
    pub fn find(&self, collection_name: &str, filter: &crate::query::Filter, opts: &crate::query::FindOptions) -> Result<crate::query::Cursor, DbError> {
        let col = self.engine.get_collection(collection_name).ok_or(DbError::NoSuchCollection(collection_name.to_string()))?;
        Ok(crate::query::find_docs(&col, filter, opts))
    }

    pub fn count(&self, collection_name: &str, filter: &crate::query::Filter) -> Result<usize, DbError> {
        let col = self.engine.get_collection(collection_name).ok_or(DbError::NoSuchCollection(collection_name.to_string()))?;
        Ok(crate::query::count_docs(&col, filter))
    }

    pub fn update_many(&self, collection_name: &str, filter: &crate::query::Filter, update: &crate::query::UpdateDoc) -> Result<crate::query::UpdateReport, DbError> {
        let col = self.engine.get_collection(collection_name).ok_or(DbError::NoSuchCollection(collection_name.to_string()))?;
        Ok(crate::query::update_many(&col, filter, update))
    }

    pub fn update_one(&self, collection_name: &str, filter: &crate::query::Filter, update: &crate::query::UpdateDoc) -> Result<crate::query::UpdateReport, DbError> {
        let col = self.engine.get_collection(collection_name).ok_or(DbError::NoSuchCollection(collection_name.to_string()))?;
        Ok(crate::query::update_one(&col, filter, update))
    }

    pub fn delete_many(&self, collection_name: &str, filter: &crate::query::Filter) -> Result<crate::query::DeleteReport, DbError> {
        let col = self.engine.get_collection(collection_name).ok_or(DbError::NoSuchCollection(collection_name.to_string()))?;
        Ok(crate::query::delete_many(&col, filter))
    }

    pub fn delete_one(&self, collection_name: &str, filter: &crate::query::Filter) -> Result<crate::query::DeleteReport, DbError> {
        let col = self.engine.get_collection(collection_name).ok_or(DbError::NoSuchCollection(collection_name.to_string()))?;
        Ok(crate::query::delete_one(&col, filter))
    }

    /// Checkpoint: write a snapshot embedding index descriptors into the main `.db` file, then truncate `.wasp`
    pub fn checkpoint(&self, filepath: &Path) -> Result<(), DbError> {
        self.engine.checkpoint_with_indexes(filepath).map_err(|e| DbError::Io(e.to_string()))
    }

    /// Returns the logical database name.
    pub fn name(&self) -> &str { &self.name }

    /// Closes an open database handle by path (optional). If not found, returns DatabaseNotFound.
    /// This removes the handle from the internal registry; resources are dropped when no longer referenced.
    pub fn close(name_or_path: Option<&str>) -> Result<(), DbError> {
        let db_path = normalize_db_path(name_or_path);
        if unregister_db(&db_path) { Ok(()) } else { Err(DbError::DatabaseNotFound) }
    }
}

/// Initializes the database system.
///
/// This function should be called before any other database operations.
/// It sets up the logger and other necessary components.
pub fn init() -> Result<(), Box<dyn std::error::Error>> {
    logger::init()?;
    Ok(())
}
use parking_lot::RwLock;
use std::sync::Weak;

static ENGINE_WEAK: LazyLock<RwLock<Option<Weak<engine::Engine>>>> = LazyLock::new(|| RwLock::new(None));
static DB_REGISTRY: LazyLock<RwLock<HashMap<String, Weak<engine::Engine>>>> = LazyLock::new(|| RwLock::new(HashMap::new()));

#[allow(dead_code)]
pub fn register_engine(engine: &Arc<engine::Engine>) {
    *ENGINE_WEAK.write() = Some(Arc::downgrade(engine));
}

#[allow(dead_code)]
pub fn engine_save_indexes_metadata() -> Option<impl FnOnce()> {
    let opt = ENGINE_WEAK.read().clone();
    if let Some(w) = opt {
        if let Some(e) = w.upgrade() { return Some(move || { let _ = e.save_indexes_metadata(); }); }
    }
    None
}

fn normalize_db_path(name_or_path: Option<&str>) -> PathBuf {
    let raw = match name_or_path { Some(s) if !s.trim().is_empty() => PathBuf::from(s), _ => PathBuf::from("nexuslite") };
    let pb = if raw.extension().is_none() { let mut p = raw; p.set_extension("db"); p } else { raw };
    // Ensure relative paths are resolved from current dir for registry key stability
    if pb.is_absolute() { pb } else { std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")).join(pb) }
}

fn register_db(path: &Path, engine: &Arc<engine::Engine>) {
    let key = path.to_string_lossy().to_string();
    DB_REGISTRY.write().insert(key, Arc::downgrade(engine));
}

fn unregister_db(path: &Path) -> bool {
    let key = path.to_string_lossy().to_string();
    DB_REGISTRY.write().remove(&key).is_some()
}
