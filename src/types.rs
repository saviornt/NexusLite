use serde_json::Value;

pub type CollectionName = String;
pub type DocumentId = String;

/// A document is any valid JSON value. Top-level is expected to be an object.
pub type Document = Value;
