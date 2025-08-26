/// Placeholder for ML-KEM (Kyber) key exchange used in file encryption flows.
///
/// # Errors
/// Always returns `FeatureNotImplemented` until PQC is implemented.
pub fn kem_derive_shared_secret() -> Result<(), crate::errors::DbError> {
    // PQC removed from runtime features; always not implemented for now
    Err(crate::errors::DbError::FeatureNotImplemented("crypto-pqc".into()))
}
