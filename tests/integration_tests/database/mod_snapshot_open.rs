use bson::doc;
use nexus_lite::Database;
use nexus_lite::index::IndexKind;
use tempfile::tempdir;

#[test]
fn database_open_rebuilds_indexes_from_snapshot() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("open.db");

    // Create a DB, make an index, insert a doc, and checkpoint
    let db = Database::new(db_path.to_str()).unwrap();
    let col = db.create_collection("users");
    col.create_index("k", IndexKind::Hash);
    let d = nexus_lite::document::Document::new(
        doc! {"k": 42},
        nexus_lite::document::DocumentType::Persistent,
    );
    let _ = db.insert_document("users", d);
    db.checkpoint(&db_path).expect("checkpoint should succeed");

    // Re-open the DB and verify the index descriptors were applied
    let db2 = Database::open(db_path.to_str().unwrap()).unwrap();
    let col2 = db2.get_collection("users").unwrap();
    let descs = col2.indexes.read().descriptors();
    assert_eq!(descs.len(), 1);
    assert_eq!(descs[0].field, "k");
}

#[test]
fn database_checkpoint_round_trip_basic() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("basic.db");
    let db = Database::new(db_path.to_str()).unwrap();
    let col = db.create_collection("c");
    col.create_index("a", IndexKind::BTree);
    db.checkpoint(&db_path).expect("checkpoint ok");
    let bytes = std::fs::read(&db_path).unwrap();
    let snap = nexus_lite::wasp::decode_snapshot_from_bytes(&bytes).unwrap();
    assert!(snap.indexes.get("c").is_some());
}
