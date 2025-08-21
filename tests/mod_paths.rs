use nexus_lite::{Database, errors::DbError};
use nexus_lite::document::{Document, DocumentType};
use bson::doc;
use tempfile::tempdir;
use std::fs;
use std::path::PathBuf;

#[test]
fn open_with_custom_filename_and_extension_creates_matching_wasp() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("foo.nexuslite");
    assert!(!db_path.exists());
    let wasp_path = db_path.with_extension("wasp");
    assert!(!wasp_path.exists());

    // Create the database at a custom path and extension
    let db = Database::new(db_path.to_str()).unwrap();

    // Files should be created
    assert!(db_path.exists());
    assert!(wasp_path.exists());

    // Basic op to ensure WASP path actually used
    db.create_collection("t");
    let _ = db.insert_document("t", Document::new(doc!{"k": 1}, DocumentType::Persistent)).unwrap();
}

#[test]
fn open_missing_returns_database_not_found() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("missing.db");
    match Database::open(db_path.to_str().unwrap()) {
        Ok(_) => panic!("expected DatabaseNotFound error"),
        Err(DbError::DatabaseNotFound) => {},
        Err(other) => panic!("expected DatabaseNotFound, got {:?}", other),
    }
}

#[test]
fn close_behaves_as_expected() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("closeme.db");
    // Create
    let _db = Database::new(db_path.to_str()).unwrap();
    // Close should succeed
    Database::close(db_path.to_str()).expect("close ok");
    // Second close should error
    let err = Database::close(db_path.to_str()).unwrap_err();
    match err { 
        DbError::DatabaseNotFound => {}, 
        other => panic!("expected DatabaseNotFound, got {:?}", other) 
    }
    // Open should succeed (files exist)
    let _db2 = Database::open(db_path.to_str().unwrap()).unwrap();
}

#[test]
fn open_default_name_creates_nexuslite_db_and_wasp_and_logs() {
    let dir = tempdir().unwrap();
    // Instead of changing CWD, explicitly provide the stem path and let .db be appended
    let stem = dir.path().join("nexuslite");
    let _db = Database::new(stem.to_str()).unwrap();
    assert!(dir.path().join("nexuslite.db").exists());
    assert!(dir.path().join("nexuslite.wasp").exists());
    // Logs should be under nexuslite_logs/nexuslite.log inside the same dir
    assert!(dir.path().join("nexuslite_logs").join("nexuslite.log").exists());
}

#[test]
fn open_with_stem_foo_creates_foo_db_wasp_and_foo_logs() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("foo"); // no extension provided
    // Create
    let _db = Database::new(db_path.to_str()).unwrap();
    // Open should succeed now
    let _db2 = Database::open(db_path.to_str().unwrap()).unwrap();
    assert!(db_path.with_extension("db").exists());
    assert!(db_path.with_extension("wasp").exists());
    // Log dir and file named after db stem in same dir as DB
    assert!(dir.path().join("foo_logs").exists());
    assert!(dir.path().join("foo_logs").join("foo.log").exists());
}

#[test]
fn open_with_extension_nexuslite_creates_db_nexuslite_and_wasp() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db.nexuslite");
    // Create
    let _db = Database::new(db_path.to_str()).unwrap();
    // Open should succeed now
    let _db2 = Database::open(db_path.to_str().unwrap()).unwrap();
    assert!(db_path.exists());
    assert!(db_path.with_extension("wasp").exists());
}

// This test writes to target/custom_naming so you can visually confirm the files exist after tests.
#[test]
fn visible_custom_files_in_target_dir() {
    let base = PathBuf::from("target").join("custom_naming");
    fs::create_dir_all(&base).unwrap();
    let db_path = base.join("visible_test.nexuslite");
    // Clean up any leftovers from previous runs
    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_file(db_path.with_extension("wasp"));
    // Create
    let _db = Database::new(db_path.to_str()).unwrap();
    // Assert files exist and will remain after test
    assert!(db_path.exists());
    assert!(db_path.with_extension("wasp").exists());
}
