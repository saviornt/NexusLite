use nexuslite::api;
use nexuslite::engine::Engine;

#[test]
fn create_document_persistent_creates_collection_if_missing() {
    let dir = tempfile::tempdir().unwrap();
    let tmp = dir.path().join("api_create_persistent.wal");
    let engine = Engine::new(tmp).unwrap();
    let id =
        api::create_document(&engine, Some("users"), &"{\"name\":\"a\"}".to_string(), false, None)
            .expect("ok");
    assert!(!id.0.is_nil());
    // Verify doc exists via find
    let found =
        api::find(&engine, "users", &nexuslite::query::Filter::True, &Default::default()).unwrap();
    assert_eq!(found.len(), 1);
}

#[test]
fn create_document_ephemeral_always_targets_temp_collection() {
    let dir = tempfile::tempdir().unwrap();
    let tmp = dir.path().join("api_create_ephemeral.wal");
    let engine = Engine::new(tmp).unwrap();
    let id =
        api::create_document(&engine, None, &"{\"x\":1}".to_string(), true, Some(1)).expect("ok");
    assert!(!id.0.is_nil());
    let found =
        api::find(&engine, "_tempDocuments", &nexuslite::query::Filter::True, &Default::default())
            .unwrap();
    assert_eq!(found.len(), 1);
}
