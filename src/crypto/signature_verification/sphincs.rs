/// Placeholder for SPHINCS+ signature verify.
///
/// # Errors
/// Always returns `FeatureNotImplemented` until PQC is implemented.
pub fn sphincs_verify(_msg: &[u8], _sig: &[u8]) -> Result<bool, crate::errors::DbError> {
    if crate::feature_flags::is_enabled("crypto-pqc") {
        return Err(crate::errors::DbError::FeatureNotImplemented("crypto-pqc".into()));
    }
    Err(crate::errors::DbError::FeatureNotImplemented("crypto-pqc".into()))
}
