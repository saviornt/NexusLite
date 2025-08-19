use nexus_lite::document::{Document, DocumentType};
use bson::{doc};
use std::thread::sleep;
use std::time::Duration;

#[test]
fn test_create_persistent_document() {
    let data = doc! { "name": "persistent_doc", "value": 1 };
    let doc = Document::new(data.clone(), DocumentType::Persistent);

    assert_eq!(doc.data, data);
    assert_eq!(doc.metadata.document_type, DocumentType::Persistent);
    assert!(doc.metadata.ttl.is_none());
}

#[test]
fn test_create_ephemeral_document() {
    let data = doc! { "name": "ephemeral_doc", "value": 2 };
    let mut doc = Document::new(data.clone(), DocumentType::Ephemeral);

    assert_eq!(doc.data, data);
    assert_eq!(doc.metadata.document_type, DocumentType::Ephemeral);
    assert!(doc.metadata.ttl.is_none());

    let ttl = Duration::from_secs(5);
    doc.set_ttl(ttl);
    assert_eq!(doc.get_ttl(), Some(ttl));
}

#[test]
fn test_document_expiry() {
    let data = doc! { "name": "expiring_doc", "value": 3 };
    let mut doc = Document::new(data, DocumentType::Ephemeral);

    let ttl = Duration::from_millis(100);
    doc.set_ttl(ttl);

    assert!(!doc.is_expired());

    sleep(Duration::from_millis(150));

    assert!(doc.is_expired());
}

#[test]
fn test_persistent_document_cannot_have_ttl() {
    let data = doc! { "name": "persistent_doc_ttl", "value": 4 };
    let mut doc = Document::new(data, DocumentType::Persistent);

    let ttl = Duration::from_secs(5);
    doc.set_ttl(ttl); // This should have no effect

    assert!(doc.metadata.ttl.is_none());
    assert!(!doc.is_expired());
}
