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
    Bson(#[from] bson::de::Error),

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
}
