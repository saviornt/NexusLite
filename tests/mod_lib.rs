use nexus_lite::{
    document::{Document, DocumentType},
    Database,
};
use bson::doc;

#[tokio::test]
async fn test_database_operations() {
    // 1. Create a new database
    let db = Database::new().unwrap();

    // 2. Create a collection
    let collection_name = "users";
    db.create_collection(collection_name);

    // 3. Insert a document
    let document = Document::new(doc! { "name": "Alice", "age": 30 }, DocumentType::Persistent);
    let doc_id = db.insert_document(collection_name, document.clone()).unwrap();

    // 4. Get the collection and find the document
    let collection = db.get_collection(collection_name).unwrap();
    let found_doc = collection.find_document(&doc_id).unwrap();
    assert_eq!(found_doc, document);

    // 5. Update the document
    let mut updated_document = document.clone();
    updated_document.data = nexus_lite::types::SerializableBsonDocument(doc! { "name": "Alice", "age": 31 });
    db.update_document(collection_name, &doc_id, updated_document.clone()).unwrap();

    let found_doc = collection.find_document(&doc_id).unwrap();
    assert_eq!(found_doc, updated_document);

    // 6. Delete the document
    db.delete_document(collection_name, &doc_id).unwrap();
    assert!(collection.find_document(&doc_id).is_none());
}
