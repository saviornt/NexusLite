use bson::doc;
use nexus_lite::document::{Document, DocumentType};
use nexus_lite::engine::Engine;
use nexus_lite::export::{export_file, ExportFormat, ExportOptions};
use std::fs;
use tempfile::tempdir;

#[tokio::test]
async fn test_export_ndjson_file() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let engine = Engine::new(wal_path).unwrap();
    let col = engine.create_collection("users".into());
    col.insert_document(Document::new(doc!{"name":"alice"}, DocumentType::Persistent));

    let out = dir.path().join("out.jsonl");
    let mut opts = ExportOptions::default();
    opts.format = ExportFormat::Ndjson;
    let rep = export_file(&engine, "users", &out, &opts).unwrap();
    assert_eq!(rep.written, 1);
    let s = fs::read_to_string(out).unwrap();
    assert!(s.contains("alice"));
}

#[tokio::test]
async fn test_export_csv_file() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let engine = Engine::new(wal_path).unwrap();
    let col = engine.create_collection("users".into());
    col.insert_document(Document::new(doc!{"name":"alice","age":30}, DocumentType::Persistent));

    let out = dir.path().join("out.csv");
    let mut opts = ExportOptions::default();
    opts.format = ExportFormat::Csv;
    let rep = export_file(&engine, "users", &out, &opts).unwrap();
    assert_eq!(rep.written, 1);
    let s = fs::read_to_string(out).unwrap();
    assert!(s.contains("name"));
}
