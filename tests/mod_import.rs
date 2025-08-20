use nexus_lite::engine::Engine;
use nexus_lite::import::{import_from_reader, ImportFormat, ImportOptions};
use std::io::Cursor;
use tempfile::tempdir;

#[tokio::test]
async fn test_import_ndjson_basic() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let engine = Engine::new(wal_path).unwrap();
    let data = "{\"name\":\"alice\"}\n{\"name\":\"bob\"}\n";
    let mut opts = ImportOptions::default();
    opts.collection = "users".into();
    let report = import_from_reader(&engine, Cursor::new(data.as_bytes()), ImportFormat::Ndjson, &opts).unwrap();
    assert_eq!(report.inserted, 2);
    let col = engine.get_collection("users").unwrap();
    assert_eq!(col.get_all_documents().len(), 2);
}

#[tokio::test]
async fn test_import_csv_headers() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let engine = Engine::new(wal_path).unwrap();
    let data = "name,age\nalice,30\n";
    let mut opts = ImportOptions::default();
    opts.collection = "users".into();
    opts.format = ImportFormat::Csv;
    let report = import_from_reader(&engine, Cursor::new(data.as_bytes()), ImportFormat::Csv, &opts).unwrap();
    assert_eq!(report.inserted, 1);
}
