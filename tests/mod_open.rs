use nexus_lite::cli::{run, Command};
use nexus_lite::engine::Engine;
use tempfile::tempdir;

#[test]
fn test_open_pbe_env_missing() {
    let dir = tempdir().unwrap();
    let wasp_path = dir.path().join("wasp.log");
    let engine = Engine::new(wasp_path).unwrap();

    // Create DB files and then PBE-encrypt them
    let db_path = dir.path().join("test.db");
    {
        // Create database to ensure files exist
        let db = nexus_lite::Database::new(Some(db_path.to_str().unwrap())).unwrap();
        // Insert a collection to cause some content
        db.create_collection("c1");
    }
    // Encrypt with username/password
    unsafe { std::env::set_var("NEXUSLITE_PASSWORD", "secret"); }
    nexus_lite::api::encrypt_db_with_password(db_path.as_path(), "user", "secret").unwrap();
    unsafe { std::env::remove_var("NEXUSLITE_PASSWORD"); }

    // Now try to open via CLI programmatic without env; should error
    let r = run(&engine, Command::DbOpen { db_path: db_path.clone() });
    assert!(r.is_err());

    // Provide env and try again
    unsafe { std::env::set_var("NEXUSLITE_USERNAME", "user"); }
    unsafe { std::env::set_var("NEXUSLITE_PASSWORD", "secret"); }
    let r2 = run(&engine, Command::DbOpen { db_path: db_path.clone() });
    assert!(r2.is_ok());
}
