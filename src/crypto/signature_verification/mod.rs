//! Signature algorithms: ECDSA (P-256) and SPHINCS+ (stub)
pub mod ecdsa;
pub mod sphincs;

// Re-export stable API
pub use ecdsa::{generate_p256_keypair_pem, sign_file_p256, verify_file_p256};
pub use sphincs::sphincs_verify;
