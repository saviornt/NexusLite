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

use crate::collection::Collection;
use crate::document::Document;
use crate::engine::Engine;
use crate::errors::DbError;
use crate::types::DocumentId;
use std::path::Path;
use std::sync::Arc;

/// The main database struct.
pub struct Database {
    engine: Arc<Engine>,
}

impl Database {
    /// Creates a new in-memory database instance.
    pub fn new() -> Result<Self, DbError> {
    // Default on-disk files to align with API expectations
    Self::open("nexuslite.db")
    }

    /// Opens or creates a database file and its associated WASP file.
    ///
    /// The main database is stored at `{filepath}` and the WASP engine state is stored at `{filepath}.wasp`.
    pub fn open<P: AsRef<Path>>(filepath: P) -> Result<Self, DbError> {
        let db_path = filepath.as_ref();
        let wasp_path = db_path.with_extension("wasp");

        // Create the main database file if it doesn't exist
        if !db_path.exists() {
            std::fs::File::create(db_path)
                .map_err(|e| DbError::Io(format!("Failed to create database file: {}", e)))?;
        }

        // Create the WASP file if it doesn't exist
        if !wasp_path.exists() {
            std::fs::File::create(&wasp_path)
                .map_err(|e| DbError::Io(format!("Failed to create WASP file: {}", e)))?;
        }

        // Initialize logging scoped to db name
        if let Some(stem) = db_path.file_stem().and_then(|s| s.to_str()) {
            let _ = crate::logger::init_for_db(stem);
        }
        // Open the WASP engine using the .wasp file
        let engine = Engine::with_wasp(wasp_path)
            .map_err(|e| DbError::Io(e.to_string()))?;

        Ok(Database {
            engine: Arc::new(engine),
        })
    }

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
}

/// Initializes the database system.
///
/// This function should be called before any other database operations.
/// It sets up the logger and other necessary components.
pub fn init() -> Result<(), Box<dyn std::error::Error>> {
    logger::init()?;
    Ok(())
}
