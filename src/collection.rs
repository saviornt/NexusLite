use crate::cache::Cache;
use crate::document::Document;
use crate::types::{DocumentId, Operation};
use crate::wasp::StorageEngine;
use parking_lot::RwLock;
use std::sync::Arc;

pub struct Collection {
    pub name: Arc<RwLock<String>>,
    pub cache: Cache,
    storage: Arc<RwLock<Box<dyn StorageEngine>>>,
}

impl Collection {
    pub fn new(name: String, storage: Arc<RwLock<Box<dyn StorageEngine>>>, cache_capacity: usize) -> Self {
        Collection {
            name: Arc::new(RwLock::new(name)),
            cache: Cache::new(cache_capacity),
            storage,
        }
    }

    pub fn new_with_config(name: String, storage: Arc<RwLock<Box<dyn StorageEngine>>>, config: crate::cache::CacheConfig) -> Self {
        Collection {
            name: Arc::new(RwLock::new(name)),
            cache: Cache::new_with_config(config),
            storage,
        }
    }

    pub fn insert_document(&self, document: Document) -> DocumentId {
        let doc_id = document.id.clone();
    self.cache.insert(document.clone());
    let operation = Operation::Insert { document };
    self.storage.write().append(&operation).expect("Failed to append insert operation to storage");
        doc_id
    }

    pub fn find_document(&self, id: &DocumentId) -> Option<Document> {
        self.cache.get(id)
    }

    pub fn update_document(&self, id: &DocumentId, new_document: Document) -> bool {
        if self.cache.get(id).is_some() {
            self.cache.insert(new_document.clone());
            let operation = Operation::Update { document_id: id.clone(), new_document };
            self.storage.write().append(&operation).expect("Failed to append update operation to storage");
            true
        } else {
            false
        }
    }

    pub fn delete_document(&self, id: &DocumentId) -> bool {
        if self.cache.remove(id).is_some() {
            let operation = Operation::Delete { document_id: id.clone() };
            self.storage.write().append(&operation).expect("Failed to append delete operation to storage");
            true
        } else {
            false
        }
    }

    pub fn get_all_documents(&self) -> Vec<Document> {
        let mut documents = Vec::new();
        // This is not efficient and should be used carefully.
        // It clones all documents in the cache.
        // A better implementation would be to have an iterator.
        let cache = self.cache.clone();
        let store = cache.store.read();
        for (_, doc) in store.iter() {
            documents.push(doc.clone());
        }
        documents
    }

    pub fn cache_metrics(&self) -> crate::cache::CacheMetricsSnapshot {
        self.cache.metrics_snapshot()
    }

    pub fn set_name(&self, new_name: String) {
        *self.name.write() = new_name;
    }

    /// Returns the collection's name as a String (cloned), hiding the RwLock.
    pub fn name_str(&self) -> String {
        self.name.read().clone()
    }
}
