//! Secret field hashing implementations.
//! - Argon2id-based hashing for selected fields
//! - Dilithium (ML-DSA) stub for future PQC support

pub mod argon2;
pub mod dilithium;

// Keep the original public API surface
pub use argon2::hash_secret_fields;
