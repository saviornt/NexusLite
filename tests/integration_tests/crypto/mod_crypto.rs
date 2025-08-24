use nexus_lite::cli::{Command, run};
use nexus_lite::engine::Engine;
use std::fs;

#[test]
fn p256_keygen_sign_verify_roundtrip() {
    let (priv_pem, pub_pem) = nexus_lite::crypto::generate_p256_keypair_pem();
    let dir = tempfile::tempdir().unwrap();
    let msg_path = dir.path().join("msg.bin");
    fs::write(&msg_path, b"hello world").unwrap();
    let sig = nexus_lite::crypto::sign_file_p256(&priv_pem, &msg_path).unwrap();
    let ok = nexus_lite::crypto::verify_file_p256(&pub_pem, &msg_path, &sig).unwrap();
    assert!(ok);
}

#[test]
fn p256_verify_rejects_tampered_signature() {
    let (priv_pem, pub_pem) = nexus_lite::crypto::generate_p256_keypair_pem();
    let dir = tempfile::tempdir().unwrap();
    let msg_path = dir.path().join("msg.bin");
    fs::write(&msg_path, b"hello world").unwrap();
    let mut sig = nexus_lite::crypto::sign_file_p256(&priv_pem, &msg_path).unwrap();
    // Flip a bit in the signature to corrupt it
    if let Some(b) = sig.get_mut(0) {
        *b ^= 0b0000_0001;
    }
    match nexus_lite::crypto::verify_file_p256(&pub_pem, &msg_path, &sig) {
        Ok(false) => {}
        Ok(true) => panic!("expected verification failure for tampered signature"),
        Err(_) => {}
    }
}

#[test]
fn p256_encrypt_decrypt_roundtrip() {
    let (priv_pem, pub_pem) = nexus_lite::crypto::generate_p256_keypair_pem();
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("plain.txt");
    let enc = dir.path().join("enc.bin");
    let out = dir.path().join("out.txt");
    fs::write(&src, b"secret data").unwrap();
    nexus_lite::crypto::encrypt_file_p256(&pub_pem, &src, &enc).unwrap();
    nexus_lite::crypto::decrypt_file_p256(&priv_pem, &enc, &out).unwrap();
    let out_bytes = fs::read(&out).unwrap();
    assert_eq!(out_bytes, b"secret data");
}

#[test]
fn cli_crypto_encrypt_decrypt_and_sign_verify() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path().join("crypto.wasp")).unwrap();
    // Prepare input file
    let input = dir.path().join("msg.bin");
    fs::write(&input, b"hello world").unwrap();
    // Keygen
    let priv_p = dir.path().join("priv.pem");
    let pub_p = dir.path().join("pub.pem");
    run(
        &engine,
        Command::CryptoKeygenP256 { out_priv: Some(priv_p.clone()), out_pub: Some(pub_p.clone()) },
    )
    .unwrap();
    // Encrypt/Decrypt
    let enc = dir.path().join("msg.enc");
    let dec = dir.path().join("msg.dec");
    run(
        &engine,
        Command::CryptoEncryptFile {
            key_pub: pub_p.clone(),
            input: input.clone(),
            output: enc.clone(),
        },
    )
    .unwrap();
    run(
        &engine,
        Command::CryptoDecryptFile {
            key_priv: priv_p.clone(),
            input: enc.clone(),
            output: dec.clone(),
        },
    )
    .unwrap();
    let dec_bytes = fs::read(&dec).unwrap();
    assert_eq!(dec_bytes, b"hello world");
    // Sign/Verify
    let sig = dir.path().join("sig.der");
    run(
        &engine,
        Command::CryptoSignFile {
            key_priv: priv_p.clone(),
            input: input.clone(),
            out_sig: Some(sig.clone()),
        },
    )
    .unwrap();
    run(
        &engine,
        Command::CryptoVerifyFile {
            key_pub: pub_p.clone(),
            input: input.clone(),
            sig: sig.clone(),
        },
    )
    .unwrap();
}

#[test]
fn cli_encrypted_checkpoint_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("mydb.db");
    // Create/open DB via API
    let db = nexus_lite::Database::new(Some(db_path.to_str().unwrap())).unwrap();
    let _ = db.create_collection("users");
    drop(db);
    let engine = Engine::new(dir.path().join("crypto2.wasp")).unwrap();
    // Keygen and checkpoint-encrypted
    let priv_p = dir.path().join("priv.pem");
    let pub_p = dir.path().join("pub.pem");
    run(
        &engine,
        Command::CryptoKeygenP256 { out_priv: Some(priv_p.clone()), out_pub: Some(pub_p.clone()) },
    )
    .unwrap();
    let enc_snap = dir.path().join("snap.enc");
    run(
        &engine,
        Command::CheckpointEncrypted {
            db_path: db_path.clone(),
            key_pub: pub_p.clone(),
            output: enc_snap.clone(),
        },
    )
    .unwrap();
    // Restore into a fresh path
    let db_restored = dir.path().join("restored.db");
    run(
        &engine,
        Command::RestoreEncrypted {
            db_path: db_restored.clone(),
            key_priv: priv_p.clone(),
            input: enc_snap.clone(),
        },
    )
    .unwrap();
    assert!(db_restored.exists());
}

#[test]
fn hash_secret_fields_argon2() {
    let mut doc = bson::doc! { "username": "alice", "password": "p@ss" };
    nexus_lite::crypto::hash_secret_fields(&mut doc, &["password"]).unwrap();
    assert!(matches!(doc.get("password"), Some(bson::Bson::Binary(_))));
}
