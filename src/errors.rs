use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Serialization error: {0}")]
    Codec(#[from] Box<bincode::ErrorKind>),
    
    #[error("Serde JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Serde BSON: {0}")]
    Bson(#[from] serde_bson::Error),
    
    #[error("Collection not found: {0}")]
    NoSuchCollection(String),
}
