use crate::cache::Cache;
use crate::document::Document;
use crate::types::{DocumentId, Operation};
use crate::wasp::{StorageEngine, IndexDelta, DeltaOp, DeltaKey};
use crate::index::{IndexManager, IndexKind, index_insert_all, index_remove_all, IndexImpl};
use parking_lot::RwLock;
use std::sync::Arc;

pub struct Collection {
    pub name: Arc<RwLock<String>>,
    pub cache: Cache,
    storage: Arc<RwLock<Box<dyn StorageEngine>>>,
    pub indexes: RwLock<IndexManager>,
    build_lock: RwLock<()>,
}

impl Collection {
    pub fn new(name: String, storage: Arc<RwLock<Box<dyn StorageEngine>>>, cache_capacity: usize) -> Self {
        Collection {
            name: Arc::new(RwLock::new(name)),
            cache: Cache::new(cache_capacity),
            storage,
            indexes: RwLock::new(IndexManager::new()),
            build_lock: RwLock::new(()),
        }
    }

    pub fn new_with_config(name: String, storage: Arc<RwLock<Box<dyn StorageEngine>>>, config: crate::cache::CacheConfig) -> Self {
        Collection {
            name: Arc::new(RwLock::new(name)),
            cache: Cache::new_with_config(config),
            storage,
            indexes: RwLock::new(IndexManager::new()),
            build_lock: RwLock::new(()),
        }
    }

    pub fn insert_document(&self, document: Document) -> DocumentId {
    let _guard = self.build_lock.read();
    let doc_id = document.id.clone();
    // First, persist operation
    let operation = Operation::Insert { document: document.clone() };
    if let Err(e) = self.storage.write().append(&operation) {
        log::error!("storage append(insert) failed: {e}");
    }
    // Then apply to cache and indexes
    self.cache.insert(document.clone());
    index_insert_all(&mut self.indexes.write(), &document.data.0, &doc_id);
    // Emit index deltas for WASP overlay
    let mut st = self.storage.write();
    for (field, idx) in self.indexes.read().indexes.iter() {
        // For simplicity, we only support top-level equality key extraction for Hash index
        if let Some(v) = document.data.0.get(field) {
            let key = match v { bson::Bson::String(s) => Some(DeltaKey::Str(s.clone())), bson::Bson::Int32(i) => Some(DeltaKey::I64(*i as i64)), bson::Bson::Int64(i) => Some(DeltaKey::I64(*i)), bson::Bson::Double(f) => Some(DeltaKey::F64(*f)), bson::Bson::Boolean(b) => Some(DeltaKey::Bool(*b)), _ => None };
            if let Some(k) = key {
                let kind = match idx { IndexImpl::Hash(_) => crate::index::IndexKind::Hash, IndexImpl::BTree(_) => crate::index::IndexKind::BTree };
                let delta = IndexDelta { collection: self.name_str(), field: field.clone(), kind, op: DeltaOp::Add, key: k, id: doc_id.clone() };
                let _ = st.append_index_delta(delta);
            }
        }
    }
    doc_id
    }

    pub fn find_document(&self, id: &DocumentId) -> Option<Document> {
        self.cache.get(id)
    }

    pub fn update_document(&self, id: &DocumentId, new_document: Document) -> bool {
        let _guard = self.build_lock.read();
        if let Some(old) = self.cache.get(id) {
            // Prepare updated document with same ID
            let mut new_doc_same_id = new_document.clone();
            new_doc_same_id.id = id.clone();
            // Persist update first
            let operation = Operation::Update { document_id: id.clone(), new_document: new_doc_same_id.clone() };
            if let Err(e) = self.storage.write().append(&operation) {
                log::error!("storage append(update) failed: {e}");
            }
            // Then mutate cache and indexes
            index_remove_all(&mut self.indexes.write(), &old.data.0, id);
            self.cache.insert(new_doc_same_id.clone());
            index_insert_all(&mut self.indexes.write(), &new_doc_same_id.data.0, id);
            // Emit remove deltas for old keys and add deltas for new keys
            let mut st = self.storage.write();
            for (field, idx) in self.indexes.read().indexes.iter() {
                if let Some(v) = old.data.0.get(field) {
                    let key = match v { bson::Bson::String(s) => Some(DeltaKey::Str(s.clone())), bson::Bson::Int32(i) => Some(DeltaKey::I64(*i as i64)), bson::Bson::Int64(i) => Some(DeltaKey::I64(*i)), bson::Bson::Double(f) => Some(DeltaKey::F64(*f)), bson::Bson::Boolean(b) => Some(DeltaKey::Bool(*b)), _ => None };
                    if let Some(k) = key {
                        let kind = match idx { IndexImpl::Hash(_) => crate::index::IndexKind::Hash, IndexImpl::BTree(_) => crate::index::IndexKind::BTree };
                        let delta = IndexDelta { collection: self.name_str(), field: field.clone(), kind, op: DeltaOp::Remove, key: k, id: id.clone() };
                        let _ = st.append_index_delta(delta);
                    }
                }
                if let Some(v) = new_doc_same_id.data.0.get(field) {
                    let key = match v { bson::Bson::String(s) => Some(DeltaKey::Str(s.clone())), bson::Bson::Int32(i) => Some(DeltaKey::I64(*i as i64)), bson::Bson::Int64(i) => Some(DeltaKey::I64(*i)), bson::Bson::Double(f) => Some(DeltaKey::F64(*f)), bson::Bson::Boolean(b) => Some(DeltaKey::Bool(*b)), _ => None };
                    if let Some(k) = key {
                        let kind = match idx { IndexImpl::Hash(_) => crate::index::IndexKind::Hash, IndexImpl::BTree(_) => crate::index::IndexKind::BTree };
                        let delta = IndexDelta { collection: self.name_str(), field: field.clone(), kind, op: DeltaOp::Add, key: k, id: id.clone() };
                        let _ = st.append_index_delta(delta);
                    }
                }
            }
            true
        } else {
            false
        }
    }

    pub fn delete_document(&self, id: &DocumentId) -> bool {
        let _guard = self.build_lock.read();
        if let Some(old) = self.cache.get(id) {
            // Persist delete first
            let operation = Operation::Delete { document_id: id.clone() };
            if let Err(e) = self.storage.write().append(&operation) {
                log::error!("storage append(delete) failed: {e}");
            }
            // Then remove from cache and indexes
            let _ = self.cache.remove(id);
            index_remove_all(&mut self.indexes.write(), &old.data.0, id);
            // Emit remove deltas
            let mut st = self.storage.write();
            for (field, idx) in self.indexes.read().indexes.iter() {
                if let Some(v) = old.data.0.get(field) {
                    let key = match v { bson::Bson::String(s) => Some(DeltaKey::Str(s.clone())), bson::Bson::Int32(i) => Some(DeltaKey::I64(*i as i64)), bson::Bson::Int64(i) => Some(DeltaKey::I64(*i)), bson::Bson::Double(f) => Some(DeltaKey::F64(*f)), bson::Bson::Boolean(b) => Some(DeltaKey::Bool(*b)), _ => None };
                    if let Some(k) = key {
                        let kind = match idx { IndexImpl::Hash(_) => crate::index::IndexKind::Hash, IndexImpl::BTree(_) => crate::index::IndexKind::BTree };
                        let delta = IndexDelta { collection: self.name_str(), field: field.clone(), kind, op: DeltaOp::Remove, key: k, id: id.clone() };
                        let _ = st.append_index_delta(delta);
                    }
                }
            }
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

    /// Return only the IDs of all documents without cloning each document.
    pub fn list_ids(&self) -> Vec<DocumentId> {
        let cache = self.cache.clone();
        let store = cache.store.read();
        store.iter().map(|(id, _)| id.clone()).collect()
    }

    pub fn cache_metrics(&self) -> crate::cache::CacheMetricsSnapshot {
        self.cache.metrics_snapshot()
    }

    /// Read index deltas from the underlying storage (WASP). Returns empty if unsupported.
    pub fn index_deltas(&self) -> Vec<IndexDelta> {
        self.storage.read().read_index_deltas().unwrap_or_default()
    }

    pub fn set_name(&self, new_name: String) {
        *self.name.write() = new_name;
    }

    /// Returns the collection's name as a String (cloned), hiding the RwLock.
    pub fn name_str(&self) -> String {
        self.name.read().clone()
    }

    // --- Index admin helpers ---
    pub fn create_index(&self, field: &str, kind: IndexKind) {
    let _wguard = self.build_lock.write();
        let mut mgr = self.indexes.write();
        mgr.create_index(field, kind);
        // offline build: rebuild from current cache
        let start = std::time::Instant::now();
        let ids_docs: Vec<(DocumentId, Document)> = {
            let cache = self.cache.clone();
            let store = cache.store.read();
            store.iter().map(|(id, doc)| (id.clone(), doc.clone())).collect()
        };
        for (id, doc) in ids_docs {
            index_insert_all(&mut mgr, &doc.data.0, &id);
        }
        // record build time on the created index only
        if let Some(idx) = mgr.indexes.get_mut(field) {
            let elapsed = start.elapsed().as_millis();
            match idx {
                IndexImpl::Hash(h) => h.stats.build_time_ms = elapsed,
                IndexImpl::BTree(b) => b.stats.build_time_ms = elapsed,
            }
        }
    }
    pub fn drop_index(&self, field: &str) { let _wguard = self.build_lock.write(); self.indexes.write().drop_index(field); }
}
