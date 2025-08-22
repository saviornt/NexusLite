use nexus_lite::cli::{run, Command};
use nexus_lite::engine::Engine;
use serde_json::json;
use tempfile::tempdir;

#[test]
fn purge_ephemeral_behaviour() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal_ephem.bin");
    let engine = Engine::new(wal_path).unwrap();

    // Insert two ephemeral docs (no TTL -> not expired)
    for i in 0..2 {
        let payload = json!({"k": i}).to_string();
        run(&engine, Command::CreateDocument { collection: None, json: payload, ephemeral: true, ttl_secs: None }).unwrap();
    }
    // Purge default (only expired) should keep both
    run(&engine, Command::PurgeEphemeral { all: false }).unwrap();
    let temp = engine.get_collection("_tempDocuments").unwrap();
    assert_eq!(temp.get_all_documents().len(), 2);

    // Purge all should remove both
    run(&engine, Command::PurgeEphemeral { all: true }).unwrap();
    assert_eq!(temp.get_all_documents().len(), 0);
}
