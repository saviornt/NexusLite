use bson::doc;
use nexuslite::collection::Collection;
use nexuslite::document::{Document, DocumentType};
use nexuslite::index::IndexKind;
use nexuslite::wasp::StorageEngine;
use parking_lot::RwLock;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn test_collection_new() {
    let dir = tempdir().unwrap();
    let wasp_path = dir.path().join("test.wasp");
    let wasp = nexuslite::wasp::Wasp::new(wasp_path).unwrap();
    let storage: Arc<RwLock<Box<dyn StorageEngine>>> = Arc::new(RwLock::new(Box::new(wasp)));
    let collection = Collection::new("test_collection".to_string(), storage, 10);
    assert_eq!(collection.name_str(), "test_collection");
}

#[tokio::test]
async fn test_insert_and_find_document() {
    let dir = tempdir().unwrap();
    let wasp_path = dir.path().join("test.wasp");
    let wasp = nexuslite::wasp::Wasp::new(wasp_path).unwrap();
    let storage: Arc<RwLock<Box<dyn StorageEngine>>> = Arc::new(RwLock::new(Box::new(wasp)));
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
    let wasp_path = dir.path().join("test.wasp");
    let wasp = nexuslite::wasp::Wasp::new(wasp_path).unwrap();
    let storage: Arc<RwLock<Box<dyn StorageEngine>>> = Arc::new(RwLock::new(Box::new(wasp)));
    let collection = Collection::new("test_collection".to_string(), storage, 10);
    let mut document = Document::new(doc! { "key": "value" }, DocumentType::Persistent);
    let doc_id = document.id.clone();

    collection.insert_document(document.clone());

    document.data = nexuslite::types::SerializableBsonDocument(doc! { "key": "new_value" });
    let updated = collection.update_document(&doc_id, document.clone());
    assert!(updated);

    let found_doc = collection.find_document(&doc_id).unwrap();
    assert_eq!(found_doc, document);
}

#[tokio::test]
async fn test_delete_document() {
    let dir = tempdir().unwrap();
    let wasp_path = dir.path().join("test.wasp");
    let wasp = nexuslite::wasp::Wasp::new(wasp_path).unwrap();
    let storage: Arc<RwLock<Box<dyn StorageEngine>>> = Arc::new(RwLock::new(Box::new(wasp)));
    let collection = Collection::new("test_collection".to_string(), storage, 10);
    let document = Document::new(doc! { "key": "value" }, DocumentType::Persistent);
    let doc_id = document.id.clone();

    collection.insert_document(document.clone());
    assert!(collection.find_document(&doc_id).is_some());

    let deleted = collection.delete_document(&doc_id);
    assert!(deleted);
    assert!(collection.find_document(&doc_id).is_none());
}

#[tokio::test]
async fn test_create_index_and_query_equality() {
    let dir = tempdir().unwrap();
    let wasp_path = dir.path().join("test.wasp");
    let wasp = nexuslite::wasp::Wasp::new(wasp_path).unwrap();
    let storage: Arc<RwLock<Box<dyn StorageEngine>>> = Arc::new(RwLock::new(Box::new(wasp)));
    let collection = Collection::new("test_index".to_string(), storage, 10_000);
    for i in 0..100i32 {
        let d = Document::new(doc! { "k": i, "v": format!("v{}", i) }, DocumentType::Persistent);
        collection.insert_document(d);
    }
    collection.create_index("k", IndexKind::Hash);
    let filter = nexuslite::query::Filter::Cmp {
        path: "k".into(),
        op: nexuslite::query::CmpOp::Eq,
        value: bson::Bson::Int32(42),
    };
    let opts = nexuslite::query::FindOptions::default();
    let arc = Arc::new(collection);
    let cur = nexuslite::query::find_docs(&arc, &filter, &opts);
    let docs = cur.to_vec();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].data.0.get_i32("k").unwrap(), 42);
}
