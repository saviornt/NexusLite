use crate::types::SerializableDateTime;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentType {
    Persistent,
    Ephemeral,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Metadata {
    pub document_type: DocumentType,
    pub created_at: SerializableDateTime,
    pub updated_at: SerializableDateTime,
    pub ttl: Option<Duration>,
}

impl Metadata {
    #[must_use]
    pub fn new(document_type: DocumentType) -> Self {
        let now = SerializableDateTime(Utc::now());
        Self { document_type, created_at: now.clone(), updated_at: now, ttl: None }
    }
}
