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
pub mod logger;
pub mod types;
pub mod wal;

use std::sync::Arc;
use uuid::Uuid;
use bson::Bson;
use crate::engine::Engine;
use crate::collection::Collection;
use crate::document::Document;
use crate::errors::DbError;
use std::path::Path;

/// The main database struct.
pub struct Database {
    engine: Engine,
    db_file_path: Option<String>,
}

impl Database {
    /// Creates a new in-memory database instance.
    pub fn new() -> Self {
        Database {
            engine: Engine::new(),
            db_file_path: None,
        }
    }

    /// Opens or creates a database file.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, DbError> {
        let engine = if path.as_ref().exists() {
            Engine::load(path.as_ref()).map_err(|e| DbError::Io(e.to_string()))?
        } else {
            let new_engine = Engine::new();
            // Automatically create _tempDocuments collection
            new_engine.create_collection("_tempDocuments".to_string());
            new_engine.save(path.as_ref()).map_err(|e| DbError::Io(e.to_string()))?;
            new_engine
        };

        Ok(Database {
            engine,
            db_file_path: Some(path.as_ref().to_string_lossy().into_owned()),
        })
    }

    /// Saves the database to its associated file path.
    pub fn save(&self) -> Result<(), DbError> {
        if let Some(ref path) = self.db_file_path {
            self.engine.save(Path::new(path)).map_err(|e| DbError::Io(e.to_string()))?;
            Ok(())
        } else {
            Err(DbError::Io("Database not associated with a file path.".to_string()))
        }
    }

    /// Creates a new collection with the given name.
    pub fn create_collection(&self, name: String) -> Arc<Collection> {
        self.engine.create_collection(name)
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
    pub fn insert_document(&self, collection_name: &str, document: Document) -> Result<Uuid, DbError> {
        let collection = self.engine.get_collection(collection_name)
            .ok_or(DbError::NoSuchCollection(collection_name.to_string()))?;
        Ok(collection.insert_document(document))
    }

    /// Finds a document in the specified collection by a field's key/value pair.
    pub fn find_document(&self, collection_name: &str, field_key: &str, field_value: &Bson) -> Result<Option<Document>, DbError> {
        let collection = self.engine.get_collection(collection_name)
            .ok_or(DbError::NoSuchCollection(collection_name.to_string()))?;

        let documents = collection.documents.read().unwrap();
        for (_, doc) in documents.iter() {
            if let Some(value) = doc.find(field_key) {
                if value == field_value {
                    return Ok(Some(doc.clone()));
                }
            }
        }
        Ok(None)
    }

    /// Updates a document in the specified collection.
    pub fn update_document(&self, collection_name: &str, document_id: &Uuid, new_document: Document) -> Result<bool, DbError> {
        let collection = self.engine.get_collection(collection_name)
            .ok_or(DbError::NoSuchCollection(collection_name.to_string()))?;
        Ok(collection.update_document(document_id, new_document))
    }

    /// Deletes a document from the specified collection by its ID.
    pub fn delete_document(&self, collection_name: &str, document_id: &Uuid) -> Result<bool, DbError> {
        let collection = self.engine.get_collection(collection_name)
            .ok_or(DbError::NoSuchCollection(collection_name.to_string()))?;
        Ok(collection.delete_document(document_id))
    }

    /// Lists the names of all collections.
    pub fn list_collection_names(&self) -> Vec<String> {
        self.engine.list_collection_names()
    }

    /// Lists the IDs of all documents in the specified collection.
    pub fn list_document_ids(&self, collection_name: &str) -> Result<Vec<Uuid>, DbError> {
        let collection = self.engine.get_collection(collection_name)
            .ok_or(DbError::NoSuchCollection(collection_name.to_string()))?;
        Ok(collection.list_document_ids())
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
