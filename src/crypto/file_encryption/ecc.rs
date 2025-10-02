use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use hkdf::Hkdf;
use p256::{
    PublicKey, SecretKey, ecdh,
    elliptic_curve::rand_core::{OsRng, RngCore},
    pkcs8::{DecodePrivateKey, DecodePublicKey},
};
use sha2::Sha256;
use std::fs;
use std::io::{Read, Write};
use zeroize::Zeroizing;

const MAGIC: &[u8; 4] = b"NLEX"; // NexusLite Encrypted eXport

/// Encrypt a file for a recipient public key using ECDH(P-256)+HKDF-SHA256 -> AES-256-GCM.
pub fn encrypt_file_p256(
    recipient_pub_pem: &str,
    input: &std::path::Path,
    output: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let recipient_pub = PublicKey::from_public_key_pem(recipient_pub_pem)?;
    // Read plaintext
    let mut pt = Vec::new();
    fs::File::open(input)?.read_to_end(&mut pt)?;
    // Ephemeral secret/public
    let eph = ecdh::EphemeralSecret::random(&mut OsRng);
    let eph_pub = PublicKey::from(&eph);
    // Shared secret
    let shared = eph.diffie_hellman(&recipient_pub);
    let ikm = shared.raw_secret_bytes();
    // Derive AES-256 key
    let hk = Hkdf::<Sha256>::new(None, ikm.as_slice());
    let mut key: Zeroizing<[u8; 32]> = Zeroizing::new([0u8; 32]);
    hk.expand(b"nexuslite:file:enc", &mut *key)
        .map_err(|e| std::io::Error::other(format!("hkdf: {e}")))?;
    let cipher = Aes256Gcm::new_from_slice(&*key)
        .map_err(|e| std::io::Error::other(format!("aes key: {e}")))?;
    let mut nonce_bytes = [0u8; 12];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ct = cipher
        .encrypt(nonce, pt.as_ref())
        .map_err(|e| std::io::Error::other(format!("encrypt: {e}")))?;
    // Write header + eph pubkey (SEC1 uncompressed) + nonce + ct
    let mut f = fs::File::create(output)?;
    f.write_all(MAGIC)?;
    f.write_all(&[crate::crypto::VERSION])?;
    let eph_bytes = eph_pub.to_sec1_bytes();
    f.write_all(&(eph_bytes.len() as u16).to_be_bytes())?;
    f.write_all(&eph_bytes)?;
    f.write_all(&nonce_bytes)?;
    f.write_all(&ct)?;
    Ok(())
}

/// Decrypt a file using recipient private key. Writes plaintext to output path.
pub fn decrypt_file_p256(
    recipient_priv_pem: &str,
    input: &std::path::Path,
    output: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let sk = SecretKey::from_pkcs8_pem(recipient_priv_pem)?;
    let mut f = fs::File::open(input)?;
    let mut magic = [0u8; 4];
    f.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err("bad magic".into());
    }
    let mut ver = [0u8; 1];
    f.read_exact(&mut ver)?;
    if ver[0] != crate::crypto::VERSION {
        return Err("bad version".into());
    }
    let mut len_buf = [0u8; 2];
    f.read_exact(&mut len_buf)?;
    let eph_len = crate::utils::num::u16_to_usize(u16::from_be_bytes(len_buf));
    let mut eph_bytes = vec![0u8; eph_len];
    f.read_exact(&mut eph_bytes)?;
    let eph_pub = PublicKey::from_sec1_bytes(&eph_bytes)?;
    let mut nonce_bytes = [0u8; 12];
    f.read_exact(&mut nonce_bytes)?;
    let mut ct = Vec::new();
    f.read_to_end(&mut ct)?;
    let shared = ecdh::diffie_hellman(sk.to_nonzero_scalar(), eph_pub.as_affine());
    let ikm = shared.raw_secret_bytes();
    let hk = Hkdf::<Sha256>::new(None, ikm.as_slice());
    let mut key: Zeroizing<[u8; 32]> = Zeroizing::new([0u8; 32]);
    hk.expand(b"nexuslite:file:enc", &mut *key)
        .map_err(|e| std::io::Error::other(format!("hkdf: {e}")))?;
    let cipher = Aes256Gcm::new_from_slice(&*key)
        .map_err(|e| std::io::Error::other(format!("aes key: {e}")))?;
    let nonce = Nonce::from_slice(&nonce_bytes);
    let pt = cipher
        .decrypt(nonce, ct.as_ref())
        .map_err(|e| std::io::Error::other(format!("decrypt: {e}")))?;
    fs::File::create(output)?.write_all(&pt)?;
    Ok(())
}
