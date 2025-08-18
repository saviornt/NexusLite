use serde_json::json;
use nexus_lite::{Database, Document};
use utils::test_logger::log_test;

#[test]
fn test_full_workflow() {
    let mut db = Database::new();
    db.create_collection("products");

    let doc = Document::new(json!({"item": "Laptop", "price": 1200}));
    let products = db.get_collection_mut("products").unwrap();
    let id = products.insert_document(doc);

    let found = products.find_document(&id).unwrap();
    log_test(&format!("End-to-end test: {:?}", found));
    assert_eq!(found.data["item"], "Laptop");
}
