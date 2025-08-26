use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("I/O error: {0}")]
    Io(String),

    #[error("Decode error: {0}")]
    Decode(#[from] bincode::error::DecodeError),

    #[error("Encode error: {0}")]
    Encode(#[from] bincode::error::EncodeError),

    #[error("Serde JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("BSON: {0}")]
    Bson(#[from] bson::error::Error),

    #[error("Collection not found: {0}")]
    NoSuchCollection(String),

    #[error("Collection already exists: {0}")]
    CollectionAlreadyExists(String),

    #[error("Document not found: {0}")]
    NoSuchDocument(String),

    #[error("Invalid document ID: {0}")]
    InvalidDocumentId(String),

    #[error("WAL error: {0}")]
    WalError(String),

    #[error("WASP shadow paging error: {0}")]
    ShadowPagingError(String),

    #[error("WASP snapshot error: {0}")]
    SnapshotError(String),

    #[error("WASP page map error: {0}")]
    PageMapError(String),

    #[error("Cache error: {0}")]
    CacheError(String),

    #[error("Encryption error: {0}")]
    EncryptionError(String),

    #[error("Decryption error: {0}")]
    DecryptionError(String),

    #[error("Signature verification error: {0}")]
    SignatureVerificationError(String),

    #[error("Query error: {0}")]
    QueryError(String),

    #[error("Database Not Found")]
    DatabaseNotFound,

    #[error("rate-limited")]
    RateLimited,

    #[error("rate-limited; retry-after-ms: {retry_after_ms}")]
    RateLimitedWithRetry { retry_after_ms: u64 },

    #[error("feature not implemented: {0}")]
    FeatureNotImplemented(String),
}
