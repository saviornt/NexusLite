use nexuslite::api;

#[test]
fn checkpoint_and_restore_encrypted_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("crypto.db");
    // Create DB and some content
    let db = nexuslite::Database::new(Some(db_path.to_str().unwrap())).unwrap();
    let _ = db.create_collection("c1");

    // Generate keys
    let (priv_pem, pub_pem) = api::crypto_generate_p256();

    // Encrypted checkpoint
    let out = dir.path().join("snapshot.enc");
    api::checkpoint_encrypted(&db, &out, &pub_pem).unwrap();
    assert!(out.exists());

    // Restore into a new path
    let restore_path = dir.path().join("restored.db");
    api::restore_encrypted(&restore_path, &out, &priv_pem).unwrap();
    assert!(restore_path.exists());
}
