use bson::doc;
use nexus_lite::document::{Document, DocumentType};
use nexus_lite::engine::Engine;
use nexus_lite::export::{ExportFormat, ExportOptions, export_file};
use nexus_lite::query::{Filter, FilterSerde};
use std::fs;
use tempfile::tempdir;

#[tokio::test]
async fn test_export_ndjson_file() {
    let dir = tempdir().unwrap();
    let wasp_path = dir.path().join("test.wasp");
    let engine = Engine::new(wasp_path).unwrap();
    let col = engine.create_collection("users".into());
    col.insert_document(Document::new(doc! {"name":"alice"}, DocumentType::Persistent));

    let out = dir.path().join("out.jsonl");
    let opts = ExportOptions { format: ExportFormat::Ndjson, ..Default::default() };
    let rep = export_file(&engine, "users", &out, &opts).unwrap();
    assert_eq!(rep.written, 1);
    let s = fs::read_to_string(out).unwrap();
    assert!(s.contains("alice"));
}

#[tokio::test]
async fn test_export_csv_file() {
    let dir = tempdir().unwrap();
    let wasp_path = dir.path().join("test.wasp");
    let engine = Engine::new(wasp_path).unwrap();
    let col = engine.create_collection("users".into());
    col.insert_document(Document::new(doc! {"name":"alice","age":30}, DocumentType::Persistent));

    let out = dir.path().join("out.csv");
    let opts = ExportOptions { format: ExportFormat::Csv, ..Default::default() };
    let rep = export_file(&engine, "users", &out, &opts).unwrap();
    assert_eq!(rep.written, 1);
    let s = fs::read_to_string(out).unwrap();
    assert!(s.contains("name"));
}

#[tokio::test]
async fn test_export_overwrite_is_atomic() {
    let dir = tempdir().unwrap();
    let wasp_path = dir.path().join("test.wasp");
    let engine = Engine::new(wasp_path).unwrap();
    let col = engine.create_collection("users".into());
    col.insert_document(Document::new(doc! {"name":"alice"}, DocumentType::Persistent));

    let out = dir.path().join("out.jsonl");
    // First export
    let opts = ExportOptions { format: ExportFormat::Ndjson, ..Default::default() };
    let rep1 = export_file(&engine, "users", &out, &opts).unwrap();
    assert_eq!(rep1.written, 1);
    let s1 = fs::read_to_string(&out).unwrap();
    assert!(s1.contains("alice"));

    // Overwrite with new content
    let col2 = engine.get_collection("users").unwrap();
    col2.insert_document(Document::new(doc! {"name":"bob"}, DocumentType::Persistent));
    let rep2 = export_file(&engine, "users", &out, &opts).unwrap();
    assert_eq!(rep2.written, 2);
    let s2 = fs::read_to_string(&out).unwrap();
    assert!(s2.contains("alice") && s2.contains("bob"));
}

#[tokio::test]
async fn test_concurrent_exports_spawn_blocking() {
    use std::sync::Arc;
    let dir = tempdir().unwrap();
    let wasp_path = dir.path().join("test.wasp");
    let engine = Arc::new(Engine::new(wasp_path).unwrap());
    let col = engine.create_collection("users".into());
    for i in 0..200i32 {
        col.insert_document(Document::new(
            doc! {"i": i64::from(i), "name":"n"},
            DocumentType::Persistent,
        ));
    }

    let out1 = dir.path().join("a.jsonl");
    let out2 = dir.path().join("b.csv");
    let e1 = engine.clone();
    let e2 = engine.clone();
    let h1 = tokio::task::spawn_blocking(move || {
        let opts = ExportOptions { format: ExportFormat::Ndjson, ..Default::default() };
        export_file(&e1, "users", &out1, &opts).map(|r| r.written)
    });
    let h2 = tokio::task::spawn_blocking(move || {
        let opts = ExportOptions { format: ExportFormat::Csv, ..Default::default() };
        export_file(&e2, "users", &out2, &opts).map(|r| r.written)
    });
    let (a, b) = tokio::join!(h1, h2);
    let wa = a.unwrap().unwrap();
    let wb = b.unwrap().unwrap();
    assert!(wa >= 200 && wb >= 200);
}

#[tokio::test]
async fn test_export_with_filter_and_limit() {
    let dir = tempdir().unwrap();
    let wasp_path = dir.path().join("test.wasp");
    let engine = Engine::new(wasp_path).unwrap();
    let col = engine.create_collection("users".into());
    for i in 0..50i64 {
        col.insert_document(Document::new(doc! {"i": i}, DocumentType::Persistent));
    }
    let out = dir.path().join("f.jsonl");
    // Filter: i >= 10 and i < 20; limit: 5
    let filter_json = serde_json::json!({
        "$and": [
            {"field": "i", "$gte": 10},
            {"field": "i", "$lt": 20}
        ]
    });
    let fs: FilterSerde = serde_json::from_value(filter_json).unwrap();
    let filter = Filter::try_from(fs).unwrap();
    let opts = ExportOptions { format: ExportFormat::Ndjson, filter: Some(filter), limit: Some(5), ..Default::default() };
    let rep = export_file(&engine, "users", &out, &opts).unwrap();
    assert_eq!(rep.written, 5);
    let s = fs::read_to_string(out).unwrap();
    assert!(s.lines().count() == 5);
}

#[tokio::test]
async fn test_export_creates_missing_parent_dir() {
    let dir = tempdir().unwrap();
    let wasp_path = dir.path().join("test.wasp");
    let engine = Engine::new(wasp_path).unwrap();
    let col = engine.create_collection("users".into());
    col.insert_document(Document::new(doc! {"name": "x"}, DocumentType::Persistent));
    let nested = dir.path().join("nested").join("deep").join("out.jsonl");
    let opts = ExportOptions { format: ExportFormat::Ndjson, ..Default::default() };
    let rep = export_file(&engine, "users", &nested, &opts).unwrap();
    assert_eq!(rep.written, 1);
    let s = fs::read_to_string(nested).unwrap();
    assert!(s.contains("x"));
}
