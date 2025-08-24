use std::sync::atomic::{AtomicU64, Ordering};

/// Simple metrics for observing cache behavior.
#[derive(Default)]
pub struct CacheMetrics {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub inserts: AtomicU64,
    pub removes: AtomicU64,
    pub ttl_evictions: AtomicU64,
    pub lru_evictions: AtomicU64,
    pub memory_bytes: AtomicU64,
    pub total_get_ns: AtomicU64,
    pub total_insert_ns: AtomicU64,
    pub total_remove_ns: AtomicU64,
}

impl CacheMetrics {
    pub fn snapshot(&self) -> CacheMetricsSnapshot {
        CacheMetricsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            inserts: self.inserts.load(Ordering::Relaxed),
            removes: self.removes.load(Ordering::Relaxed),
            ttl_evictions: self.ttl_evictions.load(Ordering::Relaxed),
            lru_evictions: self.lru_evictions.load(Ordering::Relaxed),
            memory_bytes: self.memory_bytes.load(Ordering::Relaxed),
            total_get_ns: self.total_get_ns.load(Ordering::Relaxed),
            total_insert_ns: self.total_insert_ns.load(Ordering::Relaxed),
            total_remove_ns: self.total_remove_ns.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CacheMetricsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub removes: u64,
    pub ttl_evictions: u64,
    pub lru_evictions: u64,
    pub memory_bytes: u64,
    pub total_get_ns: u64,
    pub total_insert_ns: u64,
    pub total_remove_ns: u64,
}
