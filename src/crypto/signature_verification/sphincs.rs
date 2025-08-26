/// Placeholder for SPHINCS+ signature verify.
///
/// # Errors
/// Always returns `FeatureNotImplemented` until PQC is implemented.
pub fn sphincs_verify(_msg: &[u8], _sig: &[u8]) -> Result<bool, crate::errors::DbError> {
    // PQC removed from runtime features; always not implemented for now
    Err(crate::errors::DbError::FeatureNotImplemented("crypto-pqc".into()))
}
