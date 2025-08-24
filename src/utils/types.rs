use crate::document::Document;
use bson::Document as BsonDocument;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

/// A wrapper around `uuid::Uuid` to ensure Bincode serialization compatibility.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DocumentId(pub Uuid);

impl DocumentId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for DocumentId {
    fn default() -> Self {
        Self::new()
    }
}

// Serde handles (de)serialization of DocumentId via uuid's serde feature.

/// A wrapper for `bson::Document` that implements `Encode` and `Decode`.
#[derive(Debug, Clone, PartialEq)]
pub struct SerializableBsonDocument(pub BsonDocument);

impl Serialize for SerializableBsonDocument {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let bytes = bson::to_vec(&self.0).map_err(serde::ser::Error::custom)?;
        serializer.serialize_bytes(&bytes)
    }
}

impl<'de> Deserialize<'de> for SerializableBsonDocument {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes: Vec<u8> = <Vec<u8>>::deserialize(deserializer)?;
        let doc = bson::from_slice(&bytes).map_err(serde::de::Error::custom)?;
        Ok(Self(doc))
    }
}

/// A wrapper for `chrono::DateTime<Utc>` that implements `Encode` and `Decode`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SerializableDateTime(pub DateTime<Utc>);

impl Serialize for SerializableDateTime {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_rfc3339())
    }
}

impl<'de> Deserialize<'de> for SerializableDateTime {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        let dt =
            DateTime::parse_from_rfc3339(&s).map_err(serde::de::Error::custom)?.with_timezone(&Utc);
        Ok(Self(dt))
    }
}


/// Represents operations that can be logged in the WAL.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Operation {
    Insert { document: Document },
    Update { document_id: DocumentId, new_document: Document },
    Delete { document_id: DocumentId },
}
// WAL uses bincode::serde to serialize/deserialize Operation.
