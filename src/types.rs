use serde_json::Value;
use std::vec::Vec;

pub type CollectionName = String;
pub type DocumentId = String;
pub type DocumentVectorIndex = Vec<T>;

/// A document is any valid BSON value. Top-level is expected to be an object.
pub type Document = Value;
