use bson::doc;
use nexuslite::{
    Database,
    document::{Document, DocumentType},
};
use tempfile::tempdir;

#[tokio::test]
async fn test_database_operations() {
    // 1. Create a new database in a temp directory
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("libtest.db");
    let db = Database::new(db_path.to_str()).unwrap();

    // 2. Create a collection
    let collection_name = "users";
    let _ = db.create_collection(collection_name);

    // 3. Insert a document
    let document = Document::new(doc! { "name": "Alice", "age": 30 }, DocumentType::Persistent);
    let doc_id = db.insert_document(collection_name, document.clone()).unwrap();

    // 4. Get the collection and find the document
    let collection = db.get_collection(collection_name).unwrap();
    let found_doc = collection.find_document(&doc_id).unwrap();
    assert_eq!(found_doc, document);

    // 5. Update the document
    let mut updated_document = document.clone();
    updated_document.data =
        nexuslite::types::SerializableBsonDocument(doc! { "name": "Alice", "age": 31 });
    db.update_document(collection_name, &doc_id, updated_document.clone()).unwrap();

    let found_doc = collection.find_document(&doc_id).unwrap();
    assert_eq!(found_doc, updated_document);

    // 6. Delete the document
    db.delete_document(collection_name, &doc_id).unwrap();
    assert!(collection.find_document(&doc_id).is_none());
}
