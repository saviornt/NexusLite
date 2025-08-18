//! NexusLite: embeddable NoSQL engine with Mongo-like collections/docs,
//! Redis-like TTL+LRU cache, and WAL durability.

mod engine;
mod errors;
mod types;
mod collection;
mod document;
mod cache;
mod wal;

use std::collections::HashMap;
use collection::Collection;

pub use document::{Document, DocumentId};

#[derive(Debug)]
pub struct Database {
    pub collections: HashMap<String, Collection>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            collections: HashMap::new(),
        }
    }

    /// Create a new collection
    pub fn create_collection(&mut self, name: &str) -> bool {
        if self.collections.contains_key(name) {
            return false; // already exists
        }
        self.collections.insert(name.to_string(), Collection::new(name));
        true
    }

    /// Drop an existing collection
    pub fn drop_collection(&mut self, name: &str) -> bool {
        self.collections.remove(name).is_some()
    }

    /// Get a mutable reference to a collection
    pub fn get_collection_mut(&mut self, name: &str) -> Option<&mut Collection> {
        self.collections.get_mut(name)
    }

    /// Get an immutable reference to a collection
    pub fn get_collection(&self, name: &str) -> Option<&Collection> {
        self.collections.get(name)
    }
}
