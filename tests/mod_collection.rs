use nexus_lite::collection::Collection;
use nexus_lite::document::{Document, DocumentType};
use bson::doc;

#[test]
fn test_collection_new() {
    let collection = Collection::new("test_collection".to_string());
    assert_eq!(collection.name, "test_collection");
    assert!(collection.documents.read().unwrap().is_empty());
}

#[test]
fn test_insert_document() {
    let collection = Collection::new("test_collection".to_string());
    let data = doc! { "key": "value" };
    let document = Document::new(data.clone(), DocumentType::Persistent);
    let doc_id = document.id;

    let inserted_id = collection.insert_document(document);
    assert_eq!(inserted_id, doc_id);
    assert_eq!(collection.documents.read().unwrap().len(), 1);
    assert!(collection.documents.read().unwrap().contains_key(&doc_id));
}

#[test]
fn test_find_document() {
    let collection = Collection::new("test_collection".to_string());
    let data = doc! { "key": "value" };
    let document = Document::new(data.clone(), DocumentType::Persistent);
    let doc_id = document.id;

    collection.insert_document(document.clone());

    let found_doc = collection.find_document(&doc_id).unwrap();
    assert_eq!(found_doc.id, doc_id);
    assert_eq!(found_doc.data, data);

    let non_existent_id = uuid::Uuid::new_v4();
    assert!(collection.find_document(&non_existent_id).is_none());
}

#[test]
fn test_update_document() {
    let collection = Collection::new("test_collection".to_string());
    let data = doc! { "key": "value" };
    let document = Document::new(data.clone(), DocumentType::Persistent);
    let doc_id = document.id;

    collection.insert_document(document.clone());

    let new_data = doc! { "key": "new_value" };
    let updated_document = Document::new(new_data.clone(), DocumentType::Persistent);

    let updated = collection.update_document(&doc_id, updated_document.clone());
    assert!(updated);

    let found_doc = collection.find_document(&doc_id).unwrap();
    assert_eq!(found_doc.data, new_data);

    let non_existent_id = uuid::Uuid::new_v4();
    let non_existent_doc = Document::new(doc!{}, DocumentType::Persistent);
    let updated_non_existent = collection.update_document(&non_existent_id, non_existent_doc);
    assert!(!updated_non_existent);
}

#[test]
fn test_delete_document() {
    let collection = Collection::new("test_collection".to_string());
    let data = doc! { "key": "value" };
    let document = Document::new(data.clone(), DocumentType::Persistent);
    let doc_id = document.id;

    collection.insert_document(document.clone());

    let deleted = collection.delete_document(&doc_id);
    assert!(deleted);
    assert!(collection.documents.read().unwrap().is_empty());
    assert!(collection.find_document(&doc_id).is_none());

    let non_existent_id = uuid::Uuid::new_v4();
    let deleted_non_existent = collection.delete_document(&non_existent_id);
    assert!(!deleted_non_existent);
}

#[test]
fn test_list_document_ids() {
    let collection = Collection::new("test_collection".to_string());
    let doc1 = Document::new(doc!{"a": 1}, DocumentType::Persistent);
    let doc2 = Document::new(doc!{"b": 2}, DocumentType::Persistent);

    let id1 = doc1.id;
    let id2 = doc2.id;

    collection.insert_document(doc1);
    collection.insert_document(doc2);

    let ids = collection.list_document_ids();
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&id1));
    assert!(ids.contains(&id2));
}
