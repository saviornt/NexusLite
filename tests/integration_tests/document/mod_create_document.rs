use nexuslite::cli::{Command, run};
use nexuslite::engine::Engine;
use serde_json::json;
use tempfile::tempdir;

#[test]
fn create_persistent_and_ephemeral_via_cli() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("create.wasp");
    let engine = Engine::new(wal_path).unwrap();

    let persistent = json!({"name":"alice","role":"admin"}).to_string();
    run(
        &engine,
        Command::CreateDocument {
            collection: Some("users".into()),
            json: persistent,
            ephemeral: false,
            ttl_secs: None,
        },
    )
    .unwrap();

    let ephemeral = json!({"task":"temp","state":"pending"}).to_string();
    run(
        &engine,
        Command::CreateDocument {
            collection: None,
            json: ephemeral,
            ephemeral: true,
            ttl_secs: Some(60),
        },
    )
    .unwrap();

    let users = engine.get_collection("users").expect("users collection");
    let docs_users = users.get_all_documents();
    assert_eq!(docs_users.len(), 1);
    assert_eq!(docs_users[0].metadata.document_type, nexuslite::document::DocumentType::Persistent);

    let temp = engine.get_collection("_tempDocuments").expect("_tempDocuments collection");
    let docs_temp = temp.get_all_documents();
    assert_eq!(docs_temp.len(), 1);
    assert_eq!(docs_temp[0].metadata.document_type, nexuslite::document::DocumentType::Ephemeral);
}
