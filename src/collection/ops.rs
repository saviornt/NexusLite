use super::core::Collection;
use crate::document::Document;
use crate::index::{IndexImpl, index_insert_all, index_remove_all};
use crate::telemetry;
use crate::types::{DocumentId, Operation};
use crate::wasp::{DeltaKey, DeltaOp, IndexDelta};

impl Collection {
    pub fn insert_document(&self, document: Document) -> DocumentId {
        let _guard = self.build_lock.read();
        let doc_id = document.id.clone();
        // First, persist operation
        let operation = Operation::Insert { document: document.clone() };
        let res = {
            let mut st = self.storage.write();
            st.append(&operation)
        };
        if let Err(e) = res {
            log::error!("storage append(insert) failed: {e}");
        }
        // Then apply to cache and indexes
        self.cache.insert(document.clone());
        telemetry::log_audit("insert", &self.name_str(), &doc_id.0.to_string(), None);
        index_insert_all(&mut self.indexes.write(), &document.data.0, &doc_id);
        // Emit index deltas for WASP overlay
        let mut st = self.storage.write();
        for (field, idx) in &self.indexes.read().indexes {
            if let Some(v) = document.data.0.get(field) {
                let key = match v {
                    bson::Bson::String(s) => Some(DeltaKey::Str(s.clone())),
                    bson::Bson::Int32(i) => Some(DeltaKey::I64(i64::from(*i))),
                    bson::Bson::Int64(i) => Some(DeltaKey::I64(*i)),
                    bson::Bson::Double(f) => Some(DeltaKey::F64(*f)),
                    bson::Bson::Boolean(b) => Some(DeltaKey::Bool(*b)),
                    _ => None,
                };
                if let Some(k) = key {
                    let kind = match idx {
                        IndexImpl::Hash(_) => crate::index::IndexKind::Hash,
                        IndexImpl::BTree(_) => crate::index::IndexKind::BTree,
                    };
                    let delta = IndexDelta {
                        collection: self.name_str(),
                        field: field.clone(),
                        kind,
                        op: DeltaOp::Add,
                        key: k,
                        id: doc_id.clone(),
                    };
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
            let mut new_doc_same_id = new_document;
            new_doc_same_id.id = id.clone();
            // Persist update first
            let operation = Operation::Update {
                document_id: id.clone(),
                new_document: new_doc_same_id.clone(),
            };
            let res = {
                let mut st = self.storage.write();
                st.append(&operation)
            };
            if let Err(e) = res {
                log::error!("storage append(update) failed: {e}");
            }
            // Then mutate cache and indexes
            index_remove_all(&mut self.indexes.write(), &old.data.0, id);
            self.cache.insert(new_doc_same_id.clone());
            index_insert_all(&mut self.indexes.write(), &new_doc_same_id.data.0, id);
            telemetry::log_audit("update", &self.name_str(), &id.0.to_string(), None);
            // Emit deltas
            let mut st = self.storage.write();
            for (field, idx) in &self.indexes.read().indexes {
                if let Some(v) = old.data.0.get(field) {
                    let key = match v {
                        bson::Bson::String(s) => Some(DeltaKey::Str(s.clone())),
                        bson::Bson::Int32(i) => Some(DeltaKey::I64(i64::from(*i))),
                        bson::Bson::Int64(i) => Some(DeltaKey::I64(*i)),
                        bson::Bson::Double(f) => Some(DeltaKey::F64(*f)),
                        bson::Bson::Boolean(b) => Some(DeltaKey::Bool(*b)),
                        _ => None,
                    };
                    if let Some(k) = key {
                        let kind = match idx {
                            IndexImpl::Hash(_) => crate::index::IndexKind::Hash,
                            IndexImpl::BTree(_) => crate::index::IndexKind::BTree,
                        };
                        let delta = IndexDelta {
                            collection: self.name_str(),
                            field: field.clone(),
                            kind,
                            op: DeltaOp::Remove,
                            key: k,
                            id: id.clone(),
                        };
                        let _ = st.append_index_delta(delta);
                    }
                }
                if let Some(v) = new_doc_same_id.data.0.get(field) {
                    let key = match v {
                        bson::Bson::String(s) => Some(DeltaKey::Str(s.clone())),
                        bson::Bson::Int32(i) => Some(DeltaKey::I64(i64::from(*i))),
                        bson::Bson::Int64(i) => Some(DeltaKey::I64(*i)),
                        bson::Bson::Double(f) => Some(DeltaKey::F64(*f)),
                        bson::Bson::Boolean(b) => Some(DeltaKey::Bool(*b)),
                        _ => None,
                    };
                    if let Some(k) = key {
                        let kind = match idx {
                            IndexImpl::Hash(_) => crate::index::IndexKind::Hash,
                            IndexImpl::BTree(_) => crate::index::IndexKind::BTree,
                        };
                        let delta = IndexDelta {
                            collection: self.name_str(),
                            field: field.clone(),
                            kind,
                            op: DeltaOp::Add,
                            key: k,
                            id: id.clone(),
                        };
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
            let res = {
                let mut st = self.storage.write();
                st.append(&operation)
            };
            if let Err(e) = res {
                log::error!("storage append(delete) failed: {e}");
            }
            // Then remove from cache and indexes
            let _ = self.cache.remove(id);
            index_remove_all(&mut self.indexes.write(), &old.data.0, id);
            telemetry::log_audit("delete", &self.name_str(), &id.0.to_string(), None);
            // Emit remove deltas
            let mut st = self.storage.write();
            for (field, idx) in &self.indexes.read().indexes {
                if let Some(v) = old.data.0.get(field) {
                    let key = match v {
                        bson::Bson::String(s) => Some(DeltaKey::Str(s.clone())),
                        bson::Bson::Int32(i) => Some(DeltaKey::I64(i64::from(*i))),
                        bson::Bson::Int64(i) => Some(DeltaKey::I64(*i)),
                        bson::Bson::Double(f) => Some(DeltaKey::F64(*f)),
                        bson::Bson::Boolean(b) => Some(DeltaKey::Bool(*b)),
                        _ => None,
                    };
                    if let Some(k) = key {
                        let kind = match idx {
                            IndexImpl::Hash(_) => crate::index::IndexKind::Hash,
                            IndexImpl::BTree(_) => crate::index::IndexKind::BTree,
                        };
                        let delta = IndexDelta {
                            collection: self.name_str(),
                            field: field.clone(),
                            kind,
                            op: DeltaOp::Remove,
                            key: k,
                            id: id.clone(),
                        };
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
        let cache = self.cache.clone();
        let store = cache.store.read();
        store.iter().map(|(_, doc)| doc.clone()).collect()
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
}
