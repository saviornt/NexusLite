use nexus_lite::collection::Collection;
use nexus_lite::document::{Document, DocumentType};
use nexus_lite::wal::Wal;
use nexus_lite::wasp::StorageEngine;
use bson::doc;
use parking_lot::RwLock;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn test_collection_new() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let wal = Wal::new(wal_path).unwrap();
    let storage: Arc<RwLock<Box<dyn StorageEngine>>> = Arc::new(RwLock::new(Box::new(wal)));
    let collection = Collection::new("test_collection".to_string(), storage, 10);
    assert_eq!(collection.name_str(), "test_collection");
}

#[tokio::test]
async fn test_insert_and_find_document() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let wal = Wal::new(wal_path).unwrap();
    let storage: Arc<RwLock<Box<dyn StorageEngine>>> = Arc::new(RwLock::new(Box::new(wal)));
    let collection = Collection::new("test_collection".to_string(), storage, 10);
    let document = Document::new(doc! { "key": "value" }, DocumentType::Persistent);
    let doc_id = document.id.clone();

    collection.insert_document(document.clone());

    let found_doc = collection.find_document(&doc_id).unwrap();
    assert_eq!(found_doc, document);
}

#[tokio::test]
async fn test_update_document() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let wal = Wal::new(wal_path).unwrap();
    let storage: Arc<RwLock<Box<dyn StorageEngine>>> = Arc::new(RwLock::new(Box::new(wal)));
    let collection = Collection::new("test_collection".to_string(), storage, 10);
    let mut document = Document::new(doc! { "key": "value" }, DocumentType::Persistent);
    let doc_id = document.id.clone();

    collection.insert_document(document.clone());

    document.data = nexus_lite::types::SerializableBsonDocument(doc! { "key": "new_value" });
    let updated = collection.update_document(&doc_id, document.clone());
    assert!(updated);

    let found_doc = collection.find_document(&doc_id).unwrap();
    assert_eq!(found_doc, document);
}

#[tokio::test]
async fn test_delete_document() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let wal = Wal::new(wal_path).unwrap();
    let storage: Arc<RwLock<Box<dyn StorageEngine>>> = Arc::new(RwLock::new(Box::new(wal)));
    let collection = Collection::new("test_collection".to_string(), storage, 10);
    let document = Document::new(doc! { "key": "value" }, DocumentType::Persistent);
    let doc_id = document.id.clone();

    collection.insert_document(document.clone());
    assert!(collection.find_document(&doc_id).is_some());

    let deleted = collection.delete_document(&doc_id);
    assert!(deleted);
    assert!(collection.find_document(&doc_id).is_none());
}