use nexus_lite::cli::{run, Command};
use nexus_lite::engine::Engine;
use serde_json::json;
use tempfile::tempdir;

#[test]
fn create_persistent_and_ephemeral_via_cli() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let engine = Engine::new(wal_path).unwrap();

    // Create a persistent document in collection "users"
    let persistent = json!({"name":"alice","role":"admin"}).to_string();
    run(&engine, Command::CreateDocument { collection: Some("users".into()), json: persistent, ephemeral: false, ttl_secs: None }).unwrap();

    // Create an ephemeral document (no collection provided)
    let ephemeral = json!({"task":"temp","state":"pending"}).to_string();
    run(&engine, Command::CreateDocument { collection: None, json: ephemeral, ephemeral: true, ttl_secs: Some(60) }).unwrap();

    // Verify persistent doc appears under users
    let users = engine.get_collection("users").expect("users collection");
    let docs_users = users.get_all_documents();
    assert_eq!(docs_users.len(), 1);
    assert_eq!(docs_users[0].metadata.document_type, nexus_lite::document::DocumentType::Persistent);

    // Verify ephemeral doc appears under _tempDocuments
    let temp = engine.get_collection("_tempDocuments").expect("_tempDocuments collection");
    let docs_temp = temp.get_all_documents();
    assert_eq!(docs_temp.len(), 1);
    assert_eq!(docs_temp[0].metadata.document_type, nexus_lite::document::DocumentType::Ephemeral);
}
