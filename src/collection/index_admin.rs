use super::core::Collection;
use crate::document::Document;
use crate::index::{index_insert_all, IndexImpl, IndexKind};
use crate::types::DocumentId;

impl Collection {
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

    pub fn drop_index(&self, field: &str) {
        let _wguard = self.build_lock.write();
        self.indexes.write().drop_index(field);
    }
}
