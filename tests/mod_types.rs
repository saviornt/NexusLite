use nexus_lite::types::{DocumentId, SerializableBsonDocument, SerializableDateTime, Operation};
use nexus_lite::document::{Document, DocumentType};
use bson::doc;
use bincode::config::standard;
use bincode::serde::encode_to_vec;
use chrono::Utc;

#[test]
fn test_document_id_creation() {
    let doc_id = DocumentId::new();
    assert!(!doc_id.0.is_nil());
}

#[test]
fn test_serializable_bson_document_serde() {
    let bson_doc = doc! { "key": "value" };
    let serializable_doc = SerializableBsonDocument(bson_doc.clone());

    let encoded = encode_to_vec(&serializable_doc, standard()).unwrap();
    let (decoded, _): (SerializableBsonDocument, usize) = bincode::serde::decode_from_slice(&encoded, standard()).unwrap();

    assert_eq!(serializable_doc, decoded);
}

#[test]
fn test_serializable_datetime_serde() {
    let now = Utc::now();
    let serializable_dt = SerializableDateTime(now.clone());

    let encoded = encode_to_vec(&serializable_dt, standard()).unwrap();
    let (decoded, _): (SerializableDateTime, usize) = bincode::serde::decode_from_slice(&encoded, standard()).unwrap();

    // Note: Chrono's DateTime may have nanosecond precision differences after serialization
    assert_eq!(serializable_dt.0.timestamp(), decoded.0.timestamp());
}

#[test]
fn test_operation_serde() {
    let doc = Document::new(doc! { "key": "value" }, DocumentType::Persistent);
    let doc_id = doc.id.clone();
    let op = Operation::Insert { document: doc };

    let encoded = encode_to_vec(&op, standard()).unwrap();
    let (decoded, _): (Operation, usize) = bincode::serde::decode_from_slice(&encoded, standard()).unwrap();

    assert!(matches!(decoded, Operation::Insert { .. }));
    if let Operation::Insert { document } = decoded {
        assert_eq!(document.id, doc_id);
    }
}