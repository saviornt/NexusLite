/// This module is responsibe for the creation, finding, updating and deleting BSON documents
/// and document metadata.

use chrono::{DateTime, TimeZone};
use serde::{Serialize, Deserialize};
use serde_json::Value;
use uuid::Uuid;

pub type DocumentId = Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    pub id: DocumentId,
    pub data: Value,
    pub metadata: Value,
}

pub struct MetaData {
    // pub created_at:
    // pub created_by:
    // pub modified_at:
    // pub modified_by:
    // pub last_accessed_at:
    // pub last_accessed_by:
    // pub document_size:
}

impl MetaData {
    /// Create metadata for document
    pub fn created_at() -> Self {
        // Get current datetime
    }
    
    pub fn created_by() -> Self {
        // Get current user name
    }

    pub fn modified_at() -> Self {
        // Get current datetime
    }

    pub fn modified_by() -> Self {
        // Get current user name
    }

    pub fn last_accessed_at() -> Self {
        // Get current datetime
    }

    pub fn last_accessed_by() -> Self {
        // Get current user name
    }

    pub fn document_size() -> Self {
        // Calculate estimated document size in MB
    }

}

impl Document {
    // Create a new document with generated UUID
    pub fn create(data: Value, metadata: Value) -> Self {
        Self {
            id: Uuid::new_v4(),
            data,
            metadata: MetaData
        }
    }

    // Find a document based on document UUID
    pub fn find(document_id: Value) -> Self {
        
    }

    // Update the document data
    pub fn update(&mut self, new_data: Value, update_metadata: Value) {
        self.data = new_data;
        self.metadata = update_metadata;
    }

    // Delete the document
    pub fn delete() {

    }
}
