use nexus_lite::cli::{run, Command};
use nexus_lite::engine::Engine;
use tempfile::tempdir;
use std::io::Write;

#[test]
fn test_verify_db_signatures_flow() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let engine = Engine::new(wal_path).unwrap();

    // Create a DB and write some content to .db
    let db_path = dir.path().join("v.db");
    let db = nexus_lite::Database::new(Some(db_path.to_str().unwrap())).unwrap();
    let _ = db.create_collection("c1");

    // Generate keys via API
    let (priv_pem, pub_pem) = nexus_lite::api::crypto_generate_p256();

    // Sign the .db and .wasp
    let db_sig = nexus_lite::api::crypto_sign_file(&priv_pem, &db_path).unwrap();
    let wasp_path = db_path.with_extension("wasp");
    let wasp_sig = nexus_lite::api::crypto_sign_file(&priv_pem, &wasp_path).unwrap();

    // Write sig files
    std::fs::write(db_path.with_extension("db.sig"), &db_sig).unwrap();
    std::fs::write(wasp_path.with_extension("wasp.sig"), &wasp_sig).unwrap();

    // Verify using programmatic CLI command
    let res = run(&engine, Command::VerifyDbSigs { db_path: db_path.clone(), key_pub_pem: pub_pem.clone() });
    assert!(res.is_ok());

    // Tamper .db to force failure
    {
        let mut f = std::fs::OpenOptions::new().append(true).open(&db_path).unwrap();
        writeln!(f, "tamper").unwrap();
    }
    let res2 = run(&engine, Command::VerifyDbSigs { db_path: db_path.clone(), key_pub_pem: pub_pem.clone() });
    assert!(res2.is_err());
}
