use bson::doc;
use nexuslite::document::{Document, DocumentType};
use nexuslite::{Database, errors::DbError};
use std::fs;
use std::path::PathBuf;
use tempfile::tempdir;

#[test]
fn open_with_custom_filename_and_extension_creates_matching_wasp() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("foo.nexuslite");
    assert!(!db_path.exists());
    let wasp_path = db_path.with_extension("wasp");
    assert!(!wasp_path.exists());

    let db = Database::new(db_path.to_str()).unwrap();
    assert!(db_path.exists());
    assert!(wasp_path.exists());

    let _ = db.create_collection("t");
    let _ =
        db.insert_document("t", Document::new(doc! {"k": 1}, DocumentType::Persistent)).unwrap();
}

#[test]
fn open_missing_returns_database_not_found() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("missing.db");
    match Database::open(db_path.to_str().unwrap()) {
        Ok(_) => panic!("expected DatabaseNotFound error"),
        Err(DbError::DatabaseNotFound) => {}
        Err(other) => panic!("expected DatabaseNotFound, got {other:?}"),
    }
}

#[test]
fn close_behaves_as_expected() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("closeme.db");
    let _db = Database::new(db_path.to_str()).unwrap();
    Database::close(db_path.to_str()).expect("close ok");
    let err = Database::close(db_path.to_str()).unwrap_err();
    match err {
        DbError::DatabaseNotFound => {}
        other => panic!("expected DatabaseNotFound, got {other:?}"),
    }
    let _db2 = Database::open(db_path.to_str().unwrap()).unwrap();
}

#[test]
fn open_default_name_creates_nexuslite_db_and_wasp_and_logs() {
    let dir = tempdir().unwrap();
    let stem = dir.path().join("nexuslite");
    let _db = Database::new(stem.to_str()).unwrap();
    assert!(dir.path().join("nexuslite.db").exists());
    assert!(dir.path().join("nexuslite.wasp").exists());
    let logs_dir = dir.path().join("nexuslite_logs");
    assert!(logs_dir.join("nexuslite.log").exists());
    // New rolling categories should also exist
    assert!(logs_dir.join("nexuslite_audit.log").exists());
    assert!(logs_dir.join("nexuslite_metrics.log").exists());
}

#[test]
fn open_with_stem_foo_creates_foo_db_wasp_and_foo_logs() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("foo");
    let _db = Database::new(db_path.to_str()).unwrap();
    let _db2 = Database::open(db_path.to_str().unwrap()).unwrap();
    assert!(db_path.with_extension("db").exists());
    assert!(db_path.with_extension("wasp").exists());
    let logs_dir = dir.path().join("foo_logs");
    assert!(logs_dir.exists());
    assert!(logs_dir.join("foo.log").exists());
    // New rolling categories should also exist
    assert!(logs_dir.join("foo_audit.log").exists());
    assert!(logs_dir.join("foo_metrics.log").exists());
}

#[test]
fn open_with_extension_nexuslite_creates_db_nexuslite_and_wasp() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db.nexuslite");
    let _db = Database::new(db_path.to_str()).unwrap();
    let _db2 = Database::open(db_path.to_str().unwrap()).unwrap();
    assert!(db_path.exists());
    assert!(db_path.with_extension("wasp").exists());
}

#[test]
fn visible_custom_files_in_target_dir() {
    let base = PathBuf::from("target").join("custom_naming");
    fs::create_dir_all(&base).unwrap();
    let db_path = base.join("visible_test.nexuslite");
    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_file(db_path.with_extension("wasp"));
    let _db = Database::new(db_path.to_str()).unwrap();
    assert!(db_path.exists());
    assert!(db_path.with_extension("wasp").exists());
}
