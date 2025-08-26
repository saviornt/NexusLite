use crate::errors::DbError;
use std::path::Path;

pub fn crypto_generate_p256() -> (String, String) {
    crate::crypto::generate_p256_keypair_pem()
}

pub fn crypto_sign_file<P: AsRef<Path>>(priv_pem: &str, input: P) -> Result<Vec<u8>, DbError> {
    crate::crypto::sign_file_p256(priv_pem, input.as_ref()).map_err(|e| DbError::Io(e.to_string()))
}

pub fn crypto_verify_file<P: AsRef<Path>>(
    pub_pem: &str,
    input: P,
    sig_der: &[u8],
) -> Result<bool, DbError> {
    crate::crypto::verify_file_p256(pub_pem, input.as_ref(), sig_der)
        .map_err(|e| DbError::Io(e.to_string()))
}

pub fn crypto_encrypt_file<P: AsRef<Path>, Q: AsRef<Path>>(
    pub_pem: &str,
    input: P,
    output: Q,
) -> Result<(), DbError> {
    crate::crypto::encrypt_file_p256(pub_pem, input.as_ref(), output.as_ref())
        .map_err(|e| DbError::Io(e.to_string()))
}

pub fn crypto_decrypt_file<P: AsRef<Path>, Q: AsRef<Path>>(
    priv_pem: &str,
    input: P,
    output: Q,
) -> Result<(), DbError> {
    crate::crypto::decrypt_file_p256(priv_pem, input.as_ref(), output.as_ref())
        .map_err(|e| DbError::Io(e.to_string()))
}

/// Write an encrypted checkpoint of the database .db snapshot.
pub fn checkpoint_encrypted<P: AsRef<Path>>(
    db: &crate::Database,
    output: P,
    recipient_pub_pem: &str,
) -> Result<(), DbError> {
    // 1) Snapshot to temp
    let outp = output.as_ref();
    let tmp = outp.with_extension("tmp.db");
    db.checkpoint(&tmp)?;
    // 2) Encrypt to requested output
    crate::crypto::encrypt_file_p256(recipient_pub_pem, &tmp, outp)
        .map_err(|e| DbError::Io(e.to_string()))?;
    // 3) Cleanup temp
    let _ = std::fs::remove_file(tmp);
    Ok(())
}

/// Restore database snapshot from an encrypted file.
pub fn restore_encrypted<P: AsRef<Path>, Q: AsRef<Path>>(
    db_path: P,
    encrypted: Q,
    recipient_priv_pem: &str,
) -> Result<(), DbError> {
    // 1) Decrypt to a temp snapshot
    let encp = encrypted.as_ref();
    let dbp = db_path.as_ref();
    let tmp = encp.with_extension("dec.db");
    crate::crypto::decrypt_file_p256(recipient_priv_pem, encp, &tmp)
        .map_err(|e| DbError::Io(e.to_string()))?;
    // 2) Move into place as the .db snapshot
    std::fs::copy(&tmp, dbp).map_err(|e| DbError::Io(e.to_string()))?;
    let _ = std::fs::remove_file(tmp);
    Ok(())
}

// --- Password-based DB encryption helpers ---

/// Encrypt both the .db (snapshot) and .wasp files using username+password.
/// If files are already PBE-encrypted, this will overwrite them with new params.
pub fn encrypt_db_with_password<P: AsRef<Path>>(
    db_path: P,
    username: &str,
    password: &str,
) -> Result<(), DbError> {
    let dbp = db_path.as_ref();
    let wasp_path = dbp.with_extension("wasp");
    // .db: ensure checkpoint snapshot exists, then wrap with PBE
    let tmp_plain = dbp.with_extension("tmp.plain.db");
    if dbp.exists() {
        std::fs::copy(dbp, &tmp_plain).map_err(|e| DbError::Io(e.to_string()))?;
    } else {
        // If no db yet, create empty
        std::fs::File::create(&tmp_plain).map_err(|e| DbError::Io(e.to_string()))?;
    }
    let tmp_enc = dbp.with_extension("tmp.enc.db");
    crate::crypto::pbe_encrypt_file(username, password, &tmp_plain, &tmp_enc, None)
        .map_err(|e| DbError::Io(e.to_string()))?;
    std::fs::rename(&tmp_enc, dbp).map_err(|e| DbError::Io(e.to_string()))?;
    let _ = std::fs::remove_file(&tmp_plain);

    // .wasp: wrap existing file if present, else create empty encrypted
    if wasp_path.exists() {
        let tmp_plain_w = wasp_path.with_extension("tmp.plain.wasp");
        std::fs::copy(&wasp_path, &tmp_plain_w).map_err(|e| DbError::Io(e.to_string()))?;
        let tmp_enc_w = wasp_path.with_extension("tmp.enc.wasp");
        crate::crypto::pbe_encrypt_file(username, password, &tmp_plain_w, &tmp_enc_w, None)
            .map_err(|e| DbError::Io(e.to_string()))?;
        std::fs::rename(&tmp_enc_w, &wasp_path).map_err(|e| DbError::Io(e.to_string()))?;
        let _ = std::fs::remove_file(&tmp_plain_w);
    } else {
        let tmp_plain_w = wasp_path.with_extension("tmp.plain.wasp");
        std::fs::File::create(&tmp_plain_w).map_err(|e| DbError::Io(e.to_string()))?;
        let tmp_enc_w = wasp_path.with_extension("tmp.enc.wasp");
        crate::crypto::pbe_encrypt_file(username, password, &tmp_plain_w, &tmp_enc_w, None)
            .map_err(|e| DbError::Io(e.to_string()))?;
        std::fs::rename(&tmp_enc_w, &wasp_path).map_err(|e| DbError::Io(e.to_string()))?;
        let _ = std::fs::remove_file(&tmp_plain_w);
    }
    Ok(())
}

/// Decrypt both the .db and .wasp files using username+password. This removes PBE encryption.
pub fn decrypt_db_with_password<P: AsRef<Path>>(
    db_path: P,
    username: &str,
    password: &str,
) -> Result<(), DbError> {
    let dbp = db_path.as_ref();
    let wasp_path = dbp.with_extension("wasp");
    if crate::crypto::pbe_is_encrypted(dbp) {
        let tmp_out = dbp.with_extension("tmp.dec.db");
        crate::crypto::pbe_decrypt_file(username, password, dbp, &tmp_out)
            .map_err(|e| DbError::Io(e.to_string()))?;
        std::fs::rename(&tmp_out, dbp).map_err(|e| DbError::Io(e.to_string()))?;
    }
    if wasp_path.exists() && crate::crypto::pbe_is_encrypted(&wasp_path) {
        let tmp_out_w = wasp_path.with_extension("tmp.dec.wasp");
        crate::crypto::pbe_decrypt_file(username, password, &wasp_path, &tmp_out_w)
            .map_err(|e| DbError::Io(e.to_string()))?;
        std::fs::rename(&tmp_out_w, &wasp_path).map_err(|e| DbError::Io(e.to_string()))?;
    }
    Ok(())
}
