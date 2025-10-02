//! File encryption algorithms and helpers.
//! - ECC (P-256 ECDH + HKDF-SHA256 -> AES-256-GCM)
//! - PQC (Kyber/ML-KEM) stub
//! - Password-based encryption (PBE) helpers

pub mod ecc;
pub mod kyber;

// Re-export stable API
pub use ecc::{decrypt_file_p256, encrypt_file_p256};
pub use kyber::kem_derive_shared_secret;

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use p256::elliptic_curve::rand_core::{OsRng, RngCore};
use std::fs;
use std::io::{Read, Write};
use zeroize::Zeroizing;

// ECC (P-256) lives in ecc.rs

// --- Password-based encryption (PBE) for files ---

const PBE_MAGIC: &[u8; 4] = b"NLPB"; // NexusLite Password-Based Encryption

#[derive(Clone, Debug)]
pub struct PbeKdfParams {
    pub t_cost: u32,     // iterations
    pub m_cost_kib: u32, // memory in KiB
    pub lanes: u32,
}

impl Default for PbeKdfParams {
    fn default() -> Self {
        Self { t_cost: 3, m_cost_kib: 64 * 1024, lanes: 1 }
    }
}

fn derive_pbe_key(
    username: &str,
    password: &str,
    salt: &[u8],
    params: &PbeKdfParams,
) -> Result<Zeroizing<[u8; 32]>, Box<dyn std::error::Error>> {
    use argon2::{Algorithm, Argon2, Params, Version};
    // Params::new expects memory cost in KiB; pass m_cost_kib directly.
    let p = Params::new(params.m_cost_kib, params.t_cost, params.lanes, Some(32))
        .map_err(|e| std::io::Error::other(format!("argon2 params: {e}")))?;
    let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, p);
    // Hold key material in a Zeroizing buffer so it's wiped on drop.
    let mut out: Zeroizing<[u8; 32]> = Zeroizing::new([0u8; 32]);
    let material = format!("{}:{}", username, password);
    argon
        .hash_password_into(material.as_bytes(), salt, &mut *out)
        .map_err(|e| std::io::Error::other(format!("argon2: {e}")))?;
    Ok(out)
}

fn sha256_bytes(input: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    let mut h: sha2::Sha256 = Default::default();
    h.update(input);
    let r = h.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&r);
    out
}

/// Encrypt a file using username+password (PBE). Writes header + nonce + ciphertext to output.
pub fn pbe_encrypt_file(
    username: &str,
    password: &str,
    input: &std::path::Path,
    output: &std::path::Path,
    kdf: Option<PbeKdfParams>,
) -> Result<(), Box<dyn std::error::Error>> {
    let params = kdf.unwrap_or_default();
    let mut pt = Vec::new();
    fs::File::open(input)?.read_to_end(&mut pt)?;
    let mut salt = [0u8; 16];
    OsRng.fill_bytes(&mut salt);
    let key = derive_pbe_key(username, password, &salt, &params)?;
    let mut nonce_bytes = [0u8; 12];
    OsRng.fill_bytes(&mut nonce_bytes);
    let cipher = Aes256Gcm::new_from_slice(&*key)
        .map_err(|e| std::io::Error::other(format!("aes key: {e}")))?;
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ct = cipher
        .encrypt(nonce, pt.as_ref())
        .map_err(|e| std::io::Error::other(format!("encrypt: {e}")))?;
    let uname_hash = sha256_bytes(username.as_bytes());
    let mut f = fs::File::create(output)?;
    // header: MAGIC PBE | VERSION | salt(16) | t_cost(u32) | m_cost_kib(u32) | lanes(u32) | username_hash(32) | nonce(12)
    f.write_all(PBE_MAGIC)?;
    f.write_all(&[crate::crypto::VERSION])?;
    f.write_all(&salt)?;
    f.write_all(&params.t_cost.to_be_bytes())?;
    f.write_all(&params.m_cost_kib.to_be_bytes())?;
    f.write_all(&params.lanes.to_be_bytes())?;
    f.write_all(&uname_hash)?;
    f.write_all(&nonce_bytes)?;
    f.write_all(&ct)?;
    Ok(())
}

/// Decrypt a file using username+password (PBE). Validates username hash and version.
///
/// # Errors
/// Returns I/O errors on read failures or decryption/authentication failures.
pub fn pbe_decrypt_file(
    username: &str,
    password: &str,
    input: &std::path::Path,
    output: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut f = fs::File::open(input)?;
    let mut magic = [0u8; 4];
    f.read_exact(&mut magic)?;
    if &magic != PBE_MAGIC {
        return Err("bad pbe magic".into());
    }
    let mut ver = [0u8; 1];
    f.read_exact(&mut ver)?;
    if ver[0] != crate::crypto::VERSION {
        return Err("bad pbe version".into());
    }
    let mut salt = [0u8; 16];
    f.read_exact(&mut salt)?;
    let mut t_cost = [0u8; 4];
    f.read_exact(&mut t_cost)?;
    let t_cost = u32::from_be_bytes(t_cost);
    let mut m_cost = [0u8; 4];
    f.read_exact(&mut m_cost)?;
    let m_cost = u32::from_be_bytes(m_cost);
    let mut lanes = [0u8; 4];
    f.read_exact(&mut lanes)?;
    let lanes = u32::from_be_bytes(lanes);
    let mut uname_hash = [0u8; 32];
    f.read_exact(&mut uname_hash)?;
    let expected = sha256_bytes(username.as_bytes());
    if uname_hash != expected {
        return Err("username mismatch".into());
    }
    let mut nonce_bytes = [0u8; 12];
    f.read_exact(&mut nonce_bytes)?;
    let params = PbeKdfParams { t_cost, m_cost_kib: m_cost, lanes };
    let key = derive_pbe_key(username, password, &salt, &params)?;
    let cipher = Aes256Gcm::new_from_slice(&*key)
        .map_err(|e| std::io::Error::other(format!("aes key: {e}")))?;
    let mut ct = Vec::new();
    f.read_to_end(&mut ct)?;
    let nonce = Nonce::from_slice(&nonce_bytes);
    let pt = cipher
        .decrypt(nonce, ct.as_ref())
        .map_err(|e| std::io::Error::other(format!("decrypt: {e}")))?;
    fs::File::create(output)?.write_all(&pt)?;
    Ok(())
}

/// Quick probe to detect if a file is PBE-encrypted by checking the magic.
#[must_use]
pub fn pbe_is_encrypted(input: &std::path::Path) -> bool {
    if let Ok(mut f) = fs::File::open(input) {
        let mut magic = [0u8; 4];
        if f.read_exact(&mut magic).is_ok() {
            return &magic == PBE_MAGIC;
        }
    }
    false
}

// Kyber/ML-KEM stub lives in kyber.rs
