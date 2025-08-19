use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use bson::{Document as BsonDocument, Bson};
use std::time::Duration;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum DocumentType {
    Persistent,
    Ephemeral,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Metadata {
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub document_type: DocumentType,
    pub ttl: Option<Duration>,
}

impl Metadata {
    pub fn new(document_type: DocumentType) -> Self {
        Self {
            created_at: Utc::now(),
            updated_at: Utc::now(),
            document_type,
            ttl: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Document {
    pub id: Uuid,
    pub data: BsonDocument,
    pub metadata: Metadata,
}

impl Document {
    pub fn new(data: BsonDocument, document_type: DocumentType) -> Self {
        Self {
            id: Uuid::new_v4(),
            data,
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
            let elapsed = Utc::now().signed_duration_since(self.metadata.updated_at);
            elapsed > chrono::Duration::from_std(ttl).unwrap()
        } else {
            false
        }
    }

    pub fn update(&mut self, new_data: BsonDocument) {
        self.data = new_data;
        self.metadata.updated_at = Utc::now();
    }

    pub fn find(&self, path: &str) -> Option<&Bson> {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current_doc = &self.data;

        for (i, part) in parts.iter().enumerate() {
            if i == parts.len() - 1 {
                // Last part, try to get the Bson value
                return current_doc.get(*part);
            } else {
                // Not the last part, try to get a nested document
                current_doc = current_doc.get_document(*part).ok()?;
            }
        }
        None // Should not be reached for valid paths
    }
}
