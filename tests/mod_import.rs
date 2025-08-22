use nexus_lite::engine::Engine;
use nexus_lite::import::{import_from_reader, import_file, ImportFormat, ImportOptions};
use nexus_lite::export::{export_file, ExportOptions, ExportFormat};
use std::io::{Cursor, Read};
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

#[test]
fn test_import_ndjson_array_mode() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("imp_array_wal.bin");
    let engine = nexus_lite::engine::Engine::new(wal).unwrap();
    let col = "arr";
    let data = dir.path().join("arr.json");
    {
        let mut f = std::fs::File::create(&data).unwrap();
        write!(f, "[{{\"a\":1}},{{\"a\":2}}]").unwrap();
    }
    let mut opts = ImportOptions::default();
    opts.collection = col.into();
    opts.format = ImportFormat::Ndjson;
    opts.json.array_mode = true;
    let rep = import_file(&engine, &data, &opts).unwrap();
    assert_eq!(rep.inserted, 2);
    let c = engine.get_collection(col).unwrap();
    assert_eq!(c.get_all_documents().len(), 2);
}

#[test]
fn test_import_bson_and_export_bson_roundtrip() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("imp_bson_wal.bin");
    let engine = nexus_lite::engine::Engine::new(wal).unwrap();
    let col = "bsoncol";
    let path = dir.path().join("in.bson");
    {
        let mut f = std::fs::File::create(&path).unwrap();
        let d1 = bson::to_document(&serde_json::json!({"x":1})).unwrap();
        let d2 = bson::to_document(&serde_json::json!({"x":2})).unwrap();
        let mut b1 = Vec::new(); d1.to_writer(&mut b1).unwrap();
        let mut b2 = Vec::new(); d2.to_writer(&mut b2).unwrap();
        f.write_all(&b1).unwrap(); f.write_all(&b2).unwrap();
    }
    let mut iopts = ImportOptions::default();
    iopts.collection = col.into();
    iopts.format = ImportFormat::Bson;
    let rep = import_file(&engine, &path, &iopts).unwrap();
    assert_eq!(rep.inserted, 2);
    // Export
    let out = dir.path().join("out.bson");
    let mut eopts = ExportOptions::default(); eopts.format = ExportFormat::Bson;
    let erep = export_file(&engine, col, &out, &eopts).unwrap();
    assert_eq!(erep.written, 2);
    // Re-read
    let mut f = std::fs::File::open(&out).unwrap();
    let mut buf = Vec::new(); f.read_to_end(&mut buf).unwrap();
    let mut off = 0usize; let mut cnt = 0;
    while off + 4 <= buf.len() { let sz = i32::from_le_bytes(buf[off..off+4].try_into().unwrap()) as usize; if off+sz > buf.len() { break; } off += sz; cnt += 1; }
    assert_eq!(cnt, 2);
}

#[test]
fn test_import_csv_type_infer() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("imp_csv_wal.bin");
    let engine = nexus_lite::engine::Engine::new(wal).unwrap();
    let col = "csv";
    let csvp = dir.path().join("data.csv");
    {
        let mut f = std::fs::File::create(&csvp).unwrap();
        writeln!(f, "a,b").unwrap();
        writeln!(f, "1,true").unwrap();
    }
    let mut opts = ImportOptions::default();
    opts.collection = col.into();
    opts.format = ImportFormat::Csv;
    opts.csv.type_infer = true;
    let rep = import_file(&engine, &csvp, &opts).unwrap();
    assert_eq!(rep.inserted, 1);
    let c = engine.get_collection(col).unwrap();
    let docs = c.get_all_documents();
    assert_eq!(docs[0].data.0.get_i64("a").unwrap(), 1);
    assert_eq!(docs[0].data.0.get_bool("b").unwrap(), true);
}
