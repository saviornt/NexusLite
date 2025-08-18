use serde_json::json;
use nexus_lite::{Document};
use utils::test_logger::log_test;

#[test]
fn test_document_creation() {
    let doc = Document::new(json!({"name": "Alice"}));
    log_test(&format!("Created document: {:?}", doc));
    assert_eq!(doc.data["name"], "Alice");
}

#[test]
fn test_document_update() {
    let mut doc = Document::new(json!({"age": 25}));
    doc.update(json!({"age": 26}));
    log_test(&format!("Updated document: {:?}", doc));
    assert_eq!(doc.data["age"], 26);
}
