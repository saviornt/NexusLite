use serde::{Serialize, Deserialize};
use serde_json::Value;
use uuid::Uuid;

pub type DocumentId = Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    pub id: DocumentId,
    pub data: Value,
}

impl Document {
    /// Create a new document with generated UUID
    pub fn new(data: Value) -> Self {
        Self {
            id: Uuid::new_v4(),
            data,
        }
    }

    /// Update the whole document data
    pub fn update(&mut self, new_data: Value) {
        self.data = new_data;
    }
}
