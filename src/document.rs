use crate::types::{DocumentId, SerializableBsonDocument, SerializableDateTime};
use bson::Document as BsonDocument;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum DocumentType {
    Persistent,
    Ephemeral,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Metadata {
    pub created_at: SerializableDateTime,
    pub updated_at: SerializableDateTime,
    pub document_type: DocumentType,
    pub ttl: Option<Duration>,
}

impl Metadata {
    pub fn new(document_type: DocumentType) -> Self {
        Self {
            created_at: SerializableDateTime(Utc::now()),
            updated_at: SerializableDateTime(Utc::now()),
            document_type,
            ttl: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Document {
    pub id: DocumentId,
    pub data: SerializableBsonDocument,
    pub metadata: Metadata,
}

impl Document {
    pub fn new(data: BsonDocument, document_type: DocumentType) -> Self {
        Self {
            id: DocumentId::new(),
            data: SerializableBsonDocument(data),
            metadata: Metadata::new(document_type),
        }
    }

    pub fn set_ttl(&mut self, ttl: Duration) {
        if self.metadata.document_type == DocumentType::Ephemeral {
            self.metadata.ttl = Some(ttl);
        }
    }

    pub fn get_ttl(&self) -> Option<Duration> {
        self.metadata.ttl
    }

    pub fn is_expired(&self) -> bool {
        if let Some(ttl) = self.metadata.ttl {
            let elapsed = Utc::now().signed_duration_since(self.metadata.updated_at.0);
            match chrono::Duration::from_std(ttl) {
                Ok(d) => elapsed > d,
                Err(_) => false,
            }
        } else {
            false
        }
    }

    pub fn update(&mut self, new_data: BsonDocument) {
        self.data = SerializableBsonDocument(new_data);
        self.metadata.updated_at = SerializableDateTime(Utc::now());
    }
}