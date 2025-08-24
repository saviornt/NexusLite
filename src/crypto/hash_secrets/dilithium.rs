/// Placeholder for PQC hashing (ML-DSA / Dilithium family) of secret fields.
/// For now, this is a stub that returns FeatureNotImplemented.
pub fn hash_secret_fields_dilithium(
    _doc: &mut bson::Document,
    _fields: &[&str],
) -> Result<(), crate::errors::DbError> {
    if crate::feature_flags::is_enabled("crypto-pqc") {
        return Err(crate::errors::DbError::FeatureNotImplemented("crypto-pqc".into()));
    }
    Err(crate::errors::DbError::FeatureNotImplemented("crypto-pqc".into()))
}
