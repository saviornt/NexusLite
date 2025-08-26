use nexuslite::engine::Engine;
use nexuslite::{api, export, import};

#[test]
fn import_invalid_path_returns_io_error() {
    let dir = tempfile::tempdir().unwrap();
    let tmp = dir.path().join("api_import_io.wal");
    let engine = Engine::new(tmp).unwrap();
    let mut opts = import::ImportOptions::default();
    let bad = std::path::PathBuf::from("does_not_exist.xyz");
    let err = api::import(&engine, bad, &mut opts).unwrap_err();
    matches!(err, nexuslite::errors::DbError::Io(_));
}

#[test]
fn export_invalid_collection_returns_no_such_collection() {
    let dir = tempfile::tempdir().unwrap();
    let tmp = dir.path().join("api_export.wal");
    let engine = Engine::new(tmp).unwrap();
    let mut opts = export::ExportOptions::default();
    let out = tempfile::tempdir().unwrap().path().join("out.jsonl");
    let err = api::export(&engine, "nope", out, &mut opts).unwrap_err();
    matches!(err, nexuslite::errors::DbError::NoSuchCollection(_));
}
