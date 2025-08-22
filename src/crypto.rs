//! Crypto utilities: ECC P-256 keypair/sign/verify, ECDH file encryption, and secret-field hashing.

use aes_gcm::{aead::{Aead, KeyInit}, Aes256Gcm, Nonce};
use hkdf::Hkdf;
use p256::{ecdsa::{signature::{Signer, Verifier}, Signature, SigningKey, VerifyingKey}, pkcs8::{DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey}, PublicKey, SecretKey};
use rand_core::OsRng;
use sha2::Sha256;
use std::fs;
use std::io::{Read, Write};

const MAGIC: &[u8; 4] = b"NLEX"; // NexusLite Encrypted eXport
const VERSION: u8 = 1;
const PBE_MAGIC: &[u8; 4] = b"NLPB"; // NexusLite Password-Based Encryption

pub fn generate_p256_keypair_pem() -> (String, String) {
	let sk = SigningKey::random(&mut OsRng);
	let vk = sk.verifying_key();
	let priv_pem = sk.to_pkcs8_pem(Default::default()).expect("PEM encode").to_string();
	let pub_pem = vk.to_public_key_pem(Default::default()).expect("PEM encode");
	(priv_pem, pub_pem)
}

/// Encrypt a file for a recipient public key using ECDH(P-256)+HKDF-SHA256 -> AES-256-GCM.
pub fn encrypt_file_p256(recipient_pub_pem: &str, input: &std::path::Path, output: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
	let recipient_pub = PublicKey::from_public_key_pem(recipient_pub_pem)?;
	// Read plaintext
	let mut pt = Vec::new();
	fs::File::open(input)?.read_to_end(&mut pt)?;
	// Ephemeral secret/public
	let eph = p256::ecdh::EphemeralSecret::random(&mut OsRng);
	let eph_pub = PublicKey::from(&eph);
	// Shared secret
	let shared = eph.diffie_hellman(&recipient_pub);
	let ikm = shared.raw_secret_bytes();
	// Derive AES-256 key
	let hk = Hkdf::<Sha256>::new(None, ikm.as_slice());
	let mut key = [0u8; 32];
	hk.expand(b"nexuslite:file:enc", &mut key).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("hkdf: {e}")))?;
	let cipher = Aes256Gcm::new_from_slice(&key).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("aes key: {e}")))?;
	let mut nonce_bytes = [0u8; 12];
	getrandom::getrandom(&mut nonce_bytes)?;
	let nonce = Nonce::from_slice(&nonce_bytes);
	let ct = cipher.encrypt(nonce, pt.as_ref()).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("encrypt: {e}")))?;
	// Write header + eph pubkey (SEC1 uncompressed) + nonce + ct
	let mut f = fs::File::create(output)?;
	f.write_all(MAGIC)?;
	f.write_all(&[VERSION])?;
	let eph_bytes = eph_pub.to_sec1_bytes();
	f.write_all(&(eph_bytes.len() as u16).to_be_bytes())?;
	f.write_all(&eph_bytes)?;
	f.write_all(&nonce_bytes)?;
	f.write_all(&ct)?;
	Ok(())
}

/// Decrypt a file using recipient private key. Writes plaintext to output path.
pub fn decrypt_file_p256(recipient_priv_pem: &str, input: &std::path::Path, output: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
	let sk = SecretKey::from_pkcs8_pem(recipient_priv_pem)?;
	let mut f = fs::File::open(input)?;
	let mut magic = [0u8; 4]; f.read_exact(&mut magic)?; if &magic != MAGIC { return Err("bad magic".into()); }
	let mut ver = [0u8;1]; f.read_exact(&mut ver)?; if ver[0] != VERSION { return Err("bad version".into()); }
	let mut len_buf = [0u8;2]; f.read_exact(&mut len_buf)?; let eph_len = u16::from_be_bytes(len_buf) as usize;
	let mut eph_bytes = vec![0u8; eph_len]; f.read_exact(&mut eph_bytes)?;
	let eph_pub = PublicKey::from_sec1_bytes(&eph_bytes)?;
	let mut nonce_bytes = [0u8; 12]; f.read_exact(&mut nonce_bytes)?;
	let mut ct = Vec::new(); f.read_to_end(&mut ct)?;
	let shared = p256::ecdh::diffie_hellman(sk.to_nonzero_scalar(), eph_pub.as_affine());
	let ikm = shared.raw_secret_bytes();
	let hk = Hkdf::<Sha256>::new(None, ikm.as_slice());
	let mut key = [0u8; 32];
	hk.expand(b"nexuslite:file:enc", &mut key).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("hkdf: {e}")))?;
	let cipher = Aes256Gcm::new_from_slice(&key).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("aes key: {e}")))?;
	let nonce = Nonce::from_slice(&nonce_bytes);
	let pt = cipher.decrypt(nonce, ct.as_ref()).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("decrypt: {e}")))?;
	fs::File::create(output)?.write_all(&pt)?;
	Ok(())
}

pub fn sign_file_p256(priv_pem: &str, input: &std::path::Path) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
	let sk = SigningKey::from_pkcs8_pem(priv_pem)?;
	let mut bytes = Vec::new(); fs::File::open(input)?.read_to_end(&mut bytes)?;
	let sig: Signature = sk.sign(&bytes);
	Ok(sig.to_der().as_bytes().to_vec())
}

pub fn verify_file_p256(pub_pem: &str, input: &std::path::Path, sig_der: &[u8]) -> Result<bool, Box<dyn std::error::Error>> {
	let vk = VerifyingKey::from_public_key_pem(pub_pem)?;
	let mut bytes = Vec::new(); fs::File::open(input)?.read_to_end(&mut bytes)?;
	let sig = Signature::from_der(sig_der)?;
	Ok(vk.verify(&bytes, &sig).is_ok())
}

/// Hash the specified top-level fields in a BSON document using Argon2id.
pub fn hash_secret_fields(doc: &mut bson::Document, fields: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
	let salt: [u8; 16] = {
		let mut s = [0u8; 16]; getrandom::getrandom(&mut s)?; s
	};
	let argon = argon2::Argon2::default();
	for &field in fields {
		if let Some(bson::Bson::String(s)) = doc.get(field) {
			let mut out = [0u8; 32];
			argon.hash_password_into(s.as_bytes(), &salt, &mut out).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("argon2: {e}")))?;
			doc.insert(field, bson::Bson::Binary(bson::Binary{subtype: bson::spec::BinarySubtype::Generic, bytes: out.to_vec()}));
		}
	}
	Ok(())
}

// --- Password-based encryption (PBE) for files ---

#[derive(Clone, Debug)]
pub struct PbeKdfParams {
	pub t_cost: u32, // iterations
	pub m_cost_kib: u32, // memory in KiB
	pub lanes: u32,
}

impl Default for PbeKdfParams {
	fn default() -> Self { Self { t_cost: 3, m_cost_kib: 64 * 1024, lanes: 1 } }
}

fn derive_pbe_key(username: &str, password: &str, salt: &[u8], params: &PbeKdfParams) -> Result<[u8;32], Box<dyn std::error::Error>> {
	use argon2::{Argon2, Params, Algorithm, Version};
	// Params::new expects memory cost in KiB; pass m_cost_kib directly.
	let p = Params::new(params.m_cost_kib, params.t_cost, params.lanes as u32, Some(32))
		.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("argon2 params: {e}")))?;
	let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, p);
	let mut out = [0u8; 32];
	let material = format!("{}:{}", username, password);
	argon.hash_password_into(material.as_bytes(), salt, &mut out)
		.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("argon2: {e}")))?;
	Ok(out)
}

fn sha256_bytes(input: &[u8]) -> [u8;32] {
	use sha2::Digest;
	let mut h: sha2::Sha256 = Default::default();
	h.update(input);
	let r = h.finalize();
	let mut out = [0u8;32];
	out.copy_from_slice(&r);
	out
}

/// Encrypt a file using username+password (PBE). Writes header + nonce + ciphertext to output.
pub fn pbe_encrypt_file(username: &str, password: &str, input: &std::path::Path, output: &std::path::Path, kdf: Option<PbeKdfParams>) -> Result<(), Box<dyn std::error::Error>> {
	let params = kdf.unwrap_or_default();
	let mut pt = Vec::new(); fs::File::open(input)?.read_to_end(&mut pt)?;
	let mut salt = [0u8;16]; getrandom::getrandom(&mut salt)?;
	let key = derive_pbe_key(username, password, &salt, &params)?;
	let mut nonce_bytes = [0u8; 12]; getrandom::getrandom(&mut nonce_bytes)?;
	let cipher = Aes256Gcm::new_from_slice(&key).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("aes key: {e}")))?;
	let nonce = Nonce::from_slice(&nonce_bytes);
	let ct = cipher.encrypt(nonce, pt.as_ref()).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("encrypt: {e}")))?;
	let uname_hash = sha256_bytes(username.as_bytes());
	let mut f = fs::File::create(output)?;
	// header: MAGIC PBE | VERSION | salt(16) | t_cost(u32) | m_cost_kib(u32) | lanes(u32) | username_hash(32) | nonce(12)
	f.write_all(PBE_MAGIC)?;
	f.write_all(&[VERSION])?;
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
pub fn pbe_decrypt_file(username: &str, password: &str, input: &std::path::Path, output: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
	let mut f = fs::File::open(input)?;
	let mut magic = [0u8;4]; f.read_exact(&mut magic)?; if &magic != PBE_MAGIC { return Err("bad pbe magic".into()); }
	let mut ver = [0u8;1]; f.read_exact(&mut ver)?; if ver[0] != VERSION { return Err("bad pbe version".into()); }
	let mut salt = [0u8;16]; f.read_exact(&mut salt)?;
	let mut t_cost = [0u8;4]; f.read_exact(&mut t_cost)?; let t_cost = u32::from_be_bytes(t_cost);
	let mut m_cost = [0u8;4]; f.read_exact(&mut m_cost)?; let m_cost = u32::from_be_bytes(m_cost);
	let mut lanes = [0u8;4]; f.read_exact(&mut lanes)?; let lanes = u32::from_be_bytes(lanes);
	let mut uname_hash = [0u8;32]; f.read_exact(&mut uname_hash)?;
	let expected = sha256_bytes(username.as_bytes()); if uname_hash != expected { return Err("username mismatch".into()); }
	let mut nonce_bytes = [0u8;12]; f.read_exact(&mut nonce_bytes)?;
	let params = PbeKdfParams { t_cost, m_cost_kib: m_cost, lanes };
	let key = derive_pbe_key(username, password, &salt, &params)?;
	let cipher = Aes256Gcm::new_from_slice(&key).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("aes key: {e}")))?;
	let mut ct = Vec::new(); f.read_to_end(&mut ct)?;
	let nonce = Nonce::from_slice(&nonce_bytes);
	let pt = cipher.decrypt(nonce, ct.as_ref()).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("decrypt: {e}")))?;
	fs::File::create(output)?.write_all(&pt)?;
	Ok(())
}

/// Quick probe to detect if a file is PBE-encrypted by checking the magic.
pub fn pbe_is_encrypted(input: &std::path::Path) -> bool {
	if let Ok(mut f) = fs::File::open(input) {
		let mut magic = [0u8;4];
		if f.read_exact(&mut magic).is_ok() { return &magic == PBE_MAGIC; }
	}
	false
}

/// PQC stubs for future work (ML-KEM, SPHINCS+)
pub mod pqc {
	/// Placeholder for ML-KEM key exchange
	pub fn kem_derive_shared_secret() -> Result<(), &'static str> { Err("not implemented") }
	/// Placeholder for SPHINCS+ signature verify
	pub fn sphincs_verify(_msg: &[u8], _sig: &[u8]) -> Result<bool, &'static str> { Err("not implemented") }
}
