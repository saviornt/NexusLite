use serde_json::json;
use nexus_lite::{Document, Database};
use utils::test_logger::log_test;

#[test]
fn test_collection_insert_find_delete() {
    let mut db = Database::new();
    db.create_collection("users");

    let doc = Document::new(json!({"username": "alice"}));
    let users = db.get_collection_mut("users").unwrap();
    let id = users.insert_document(doc.clone());
    log_test(&format!("Inserted document with ID: {}", id));

    let found = users.find_document(&id).unwrap();
    log_test(&format!("Found document: {:?}", found));
    assert_eq!(found.data["username"], "alice");

    let removed = users.delete_document(&id).unwrap();
    log_test(&format!("Removed document: {:?}", removed));
    assert_eq!(removed.data["username"], "alice");
}
