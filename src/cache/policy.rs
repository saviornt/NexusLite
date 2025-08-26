use crate::cache::metrics::CacheMetrics;
use crate::document::Document;
use crate::types::DocumentId;
use lru::LruCache;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// Removes expired documents from the cache. Returns number evicted.
pub fn purge_expired(
    store: &Arc<RwLock<LruCache<DocumentId, Document>>>,
    metrics: &Arc<CacheMetrics>,
    sizes: &Arc<RwLock<HashMap<DocumentId, usize>>>,
    freq: &Arc<RwLock<HashMap<DocumentId, u64>>>,
) -> usize {
    let mut cache = store.write();
    let expired_keys: Vec<DocumentId> =
        cache.iter().filter(|(_, doc)| doc.is_expired()).map(|(id, _)| id.clone()).collect();

    let count = expired_keys.len();
    let mut freed_bytes: u64 = 0;
    for key in expired_keys {
        cache.pop(&key);
        if let Some(sz) = sizes.write().remove(&key) {
            let sz64 = crate::utils::num::usize_to_u64(sz);
            metrics.memory_bytes.fetch_sub(sz64, Ordering::Relaxed);
            freed_bytes = freed_bytes.saturating_add(sz64);
        }
        freq.write().remove(&key);
    }
    if count > 0 {
        metrics
            .ttl_evictions
            .fetch_add(crate::utils::num::usize_to_u64(count), Ordering::Relaxed);
        // Developer benchmark log: TTL purge summary
        crate::dev6!(
            "{{\"bench\":\"cache\",\"op\":\"ttl_purge\",\"evicted\":{},\"freed_bytes\":{}}}",
            count,
            freed_bytes
        );
    }
    count
}
