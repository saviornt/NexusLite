//! Crypto utilities: ECC P-256 keypair/sign/verify, ECDH file encryption, secret-field hashing,
//! and password-based encryption (PBE). Public API is preserved via re-exports.

// Shared format version for crypto file headers
pub(crate) const VERSION: u8 = 1;

// New folder layout
pub mod file_encryption;
pub mod signature_verification;

// Keep original public API via re-exports
pub use file_encryption::{
    PbeKdfParams, decrypt_file_p256, encrypt_file_p256, pbe_decrypt_file, pbe_encrypt_file,
    pbe_is_encrypted,
};
pub use signature_verification::{generate_p256_keypair_pem, sign_file_p256, verify_file_p256};

// Secret-field hashing via hash_secrets module
pub mod hash_secrets;
pub use hash_secrets::hash_secret_fields;

// Back-compat logical module for pqc APIs without a physical folder
pub mod pqc {
    pub use crate::crypto::file_encryption::kyber::kem_derive_shared_secret;
    pub use crate::crypto::signature_verification::sphincs::sphincs_verify;
}
