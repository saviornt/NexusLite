use crate::document::types::{DocumentType, Metadata};
use crate::types::{DocumentId, SerializableBsonDocument, SerializableDateTime};
use bson::Document as BsonDocument;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Document {
    pub id: DocumentId,
    pub data: SerializableBsonDocument,
    pub metadata: Metadata,
}

impl Document {
    #[must_use]
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

    #[must_use]
    pub const fn get_ttl(&self) -> Option<Duration> {
        self.metadata.ttl
    }

    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.metadata.ttl.is_some_and(|ttl| {
            let elapsed = Utc::now().signed_duration_since(self.metadata.updated_at.0);
            chrono::Duration::from_std(ttl).is_ok_and(|d| elapsed > d)
        })
    }

    pub fn update(&mut self, new_data: BsonDocument) {
        self.data = SerializableBsonDocument(new_data);
        self.metadata.updated_at = SerializableDateTime(Utc::now());
    }
}
