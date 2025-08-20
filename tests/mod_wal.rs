use nexus_lite::document::{Document, DocumentType};
use nexus_lite::types::{DocumentId, Operation};
use nexus_lite::wal::Wal;
use bson::doc;
use tempfile::tempdir;

#[test]
fn test_wal_append_and_read() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let mut wal = Wal::new(wal_path).unwrap();

    let doc = Document::new(doc! { "key": "value" }, DocumentType::Persistent);
    let op1 = Operation::Insert { document: doc };
    wal.append(&op1).unwrap();

    let doc2 = Document::new(doc! { "key2": "value2" }, DocumentType::Persistent);
    let op2 = Operation::Update {
        document_id: DocumentId::new(),
        new_document: doc2,
    };
    wal.append(&op2).unwrap();

    let operations = wal.read_all().unwrap();
    assert_eq!(operations.len(), 2);

    let decoded_op1 = operations[0].as_ref().unwrap();
    let decoded_op2 = operations[1].as_ref().unwrap();

    assert!(matches!(decoded_op1, Operation::Insert { .. }));
    assert!(matches!(decoded_op2, Operation::Update { .. }));
}
