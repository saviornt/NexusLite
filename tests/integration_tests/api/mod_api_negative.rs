use nexus_lite::engine::Engine;
use nexus_lite::{api, query};

#[test]
fn count_on_missing_collection_returns_no_such_collection() {
    let tmp = std::env::temp_dir().join("api_missing_col.wal");
    let engine = Engine::new(tmp).unwrap();
    let filter = query::Filter::True;
    let err = api::count(&engine, "does_not_exist", &filter).unwrap_err();
    match err {
        nexus_lite::errors::DbError::NoSuchCollection(name) => {
            assert_eq!(name, "does_not_exist")
        }
        e => panic!("unexpected: {e:?}"),
    }
}

#[test]
fn find_on_missing_collection_returns_no_such_collection() {
    let tmp = std::env::temp_dir().join("api_find_missing.wal");
    let engine = Engine::new(tmp).unwrap();
    let opts = query::FindOptions::default();
    let e = api::find(&engine, "none", &query::Filter::True, &opts).unwrap_err();
    matches!(e, nexus_lite::errors::DbError::NoSuchCollection(_));
}

#[test]
fn update_delete_on_missing_collection() {
    let tmp = std::env::temp_dir().join("api_update_missing.wal");
    let engine = Engine::new(tmp).unwrap();
    let upd = query::UpdateDoc { set: vec![("a".into(), bson::Bson::Int32(1))], inc: vec![], unset: vec![] };
    let f = query::Filter::True;
    let e1 = api::update_one(&engine, "nope", &f, &upd).unwrap_err();
    let e2 = api::delete_one(&engine, "nope", &f).unwrap_err();
    assert!(matches!(e1, nexus_lite::errors::DbError::NoSuchCollection(_)));
    assert!(matches!(e2, nexus_lite::errors::DbError::NoSuchCollection(_)));
}

#[test]
fn parse_filter_update_json_errors() {
    let bad_filter = api::parse_filter_json("not json");
    assert!(bad_filter.is_err());
    let bad_update = api::parse_update_json("not json");
    assert!(bad_update.is_err());
}

#[test]
fn db_open_encrypted_without_env_fails() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("neg_open.db");
    // Create db then encrypt
    let _db = nexus_lite::Database::new(Some(db_path.to_str().unwrap())).unwrap();
    api::encrypt_db_with_password(&db_path, "u", "p").unwrap();
    // Clear env
    unsafe {
        std::env::remove_var("NEXUSLITE_USERNAME");
        std::env::remove_var("NEXUSLITE_PASSWORD");
    }
    // Try to open programmatic API which expects env if not TTY
    match api::db_open(db_path.to_str().unwrap()) {
        Ok(_) => panic!("expected error"),
        Err(err) => assert!(matches!(err, nexus_lite::errors::DbError::Io(_))),
    }
}

#[test]
fn db_close_missing_returns_database_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("missing_close.db");
    let e = api::db_close(db_path.to_str());
    assert!(matches!(e, Err(nexus_lite::errors::DbError::DatabaseNotFound)));
}
