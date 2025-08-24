/// Placeholder for ML-KEM (Kyber) key exchange used in file encryption flows.
///
/// # Errors
/// Always returns `FeatureNotImplemented` until PQC is implemented.
pub fn kem_derive_shared_secret() -> Result<(), crate::errors::DbError> {
    if crate::feature_flags::is_enabled("crypto-pqc") {
        return Err(crate::errors::DbError::FeatureNotImplemented("crypto-pqc".into()));
    }
    Err(crate::errors::DbError::FeatureNotImplemented("crypto-pqc".into()))
}
