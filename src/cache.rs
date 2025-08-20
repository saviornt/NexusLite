use crate::document::Document;
use crate::types::DocumentId;
use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::time;

/// Eviction modes to control cache behavior.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvictionMode {
    TtlFirst,
    LruOnly,
    TtlOnly,
    Hybrid,
}

/// Configuration for the cache.
#[derive(Clone, Debug)]
pub struct CacheConfig {
    pub capacity: usize,
    pub max_samples: usize,  // reserved for future sampling strategy
    pub batch_size: usize,
    pub eviction_mode: EvictionMode,
    pub purge_interval_secs: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            capacity: 1024,
            max_samples: 5,
            batch_size: 5,
            eviction_mode: EvictionMode::TtlFirst,
            purge_interval_secs: 5,
        }
    }
}

/// Simple metrics for observing cache behavior.
#[derive(Default)]
pub struct CacheMetrics {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub inserts: AtomicU64,
    pub removes: AtomicU64,
    pub ttl_evictions: AtomicU64,
    pub lru_evictions: AtomicU64,
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
}

/// A thread-safe, in-memory cache with TTL-first + LRU fallback eviction.
#[derive(Clone)]
pub struct Cache {
    pub store: Arc<RwLock<LruCache<DocumentId, Document>>>,
    pub config: Arc<RwLock<CacheConfig>>, // runtime adjustable
    pub metrics: Arc<CacheMetrics>,
    eviction_lock: Arc<Mutex<()>>,
}

impl Cache {
    /// Creates a new cache with a given capacity and starts the TTL eviction task.
    pub fn new(capacity: usize) -> Self {
        Self::new_with_config(CacheConfig { capacity, ..Default::default() })
    }

    /// Creates a new cache with the provided configuration.
    pub fn new_with_config(config: CacheConfig) -> Self {
        let cache = Cache {
            store: Arc::new(RwLock::new(LruCache::new(NonZeroUsize::new(config.capacity).unwrap()))),
            config: Arc::new(RwLock::new(config)),
            metrics: Arc::new(CacheMetrics::default()),
            eviction_lock: Arc::new(Mutex::new(())),
        };

        // Spawn a background task for TTL eviction
        let store_clone = cache.store.clone();
        let metrics_clone = cache.metrics.clone();
        let config_clone = cache.config.clone();
        tokio::spawn(async move {
            loop {
                let secs = config_clone.read().purge_interval_secs;
                time::sleep(Duration::from_secs(secs)).await;
                purge_expired(&store_clone, &metrics_clone);
            }
        });

        cache
    }

    /// Inserts a document into the cache.
    pub fn insert(&self, document: Document) {
        // Evict as needed before insert to honor TTL-first policy
        self.enforce_capacity();

        self.store.write().put(document.id.clone(), document);
        self.metrics.inserts.fetch_add(1, Ordering::Relaxed);
    }

    /// Retrieves a document from the cache.
    pub fn get(&self, id: &DocumentId) -> Option<Document> {
        let mut guard = self.store.write();
        if let Some(doc) = guard.get(id) {
            if doc.is_expired() {
                // Lazy eviction on access
                guard.pop(id);
                self.metrics.ttl_evictions.fetch_add(1, Ordering::Relaxed);
                self.metrics.misses.fetch_add(1, Ordering::Relaxed);
                None
            } else {
                self.metrics.hits.fetch_add(1, Ordering::Relaxed);
                Some(doc.clone())
            }
        } else {
            self.metrics.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Removes a document from the cache.
    pub fn remove(&self, id: &DocumentId) -> Option<Document> {
        let removed = self.store.write().pop(id);
        if removed.is_some() {
            self.metrics.removes.fetch_add(1, Ordering::Relaxed);
        }
        removed
    }

    /// Clears the cache.
    pub fn clear(&self) {
        self.store.write().clear();
    }

    /// Force a TTL purge now. Returns number evicted.
    pub fn purge_expired_now(&self) -> usize {
        purge_expired(&self.store, &self.metrics)
    }

    /// Get a snapshot of metrics.
    pub fn metrics_snapshot(&self) -> CacheMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Runtime config updates
    pub fn set_eviction_mode(&self, mode: EvictionMode) {
        self.config.write().eviction_mode = mode;
    }

    pub fn set_max_samples(&self, samples: usize) {
        self.config.write().max_samples = samples.max(1);
    }

    pub fn set_batch_size(&self, batch: usize) {
        self.config.write().batch_size = batch.max(1);
    }

    pub fn set_capacity(&self, capacity: usize) {
        let nz = NonZeroUsize::new(capacity.max(1)).unwrap();
        self.config.write().capacity = nz.get();
        self.store.write().resize(nz);
    }

    pub fn set_purge_interval_secs(&self, secs: u64) {
        self.config.write().purge_interval_secs = secs.max(1);
    }
}

/// Removes expired documents from the cache. Returns number evicted.
fn purge_expired(store: &Arc<RwLock<LruCache<DocumentId, Document>>>, metrics: &Arc<CacheMetrics>) -> usize {
    let mut cache = store.write();
    let expired_keys: Vec<DocumentId> = cache
        .iter()
        .filter(|(_, doc)| doc.is_expired())
        .map(|(id, _)| id.clone())
        .collect();

    let count = expired_keys.len();
    for key in expired_keys {
        cache.pop(&key);
    }
    if count > 0 {
        metrics.ttl_evictions.fetch_add(count as u64, Ordering::Relaxed);
    }
    count
}

impl Cache {
    /// Ensures capacity by evicting TTL-expired entries first, then LRU as fallback.
    fn enforce_capacity(&self) {
        let _lock = self.eviction_lock.lock(); // prevent concurrent eviction cycles

        // Calculate how many evictions we need to make room for one insert
        let (mut needed, mode);
        {
            let guard = self.store.read();
            let cap = guard.cap().get();
            let len = guard.len();
            mode = self.config.read().eviction_mode;
            if len < cap { return; }
            needed = (len + 1).saturating_sub(cap);
        }

        // Try TTL evictions first if enabled
        {
            if needed > 0 && (mode == EvictionMode::TtlFirst || mode == EvictionMode::TtlOnly || mode == EvictionMode::Hybrid) {
                let batch_limit = self.config.read().batch_size;
                let mut evicted_total = 0usize;
                while evicted_total < needed && evicted_total < batch_limit {
                    let evicted = purge_expired(&self.store, &self.metrics);
                    if evicted == 0 { break; }
                    evicted_total += evicted;
                }
                needed = needed.saturating_sub(evicted_total);
            }
        }

        // If still need space, evict via LRU sampling fallback
        {
            let mut cache = self.store.write();
            let cap = cache.cap().get();
            let mode = self.config.read().eviction_mode;
            if mode == EvictionMode::TtlOnly { return; }

            while needed > 0 && cache.len() >= cap && cache.len() > 0 {
                let batch_size = self.config.read().batch_size.min(needed);
                let max_samples = self.config.read().max_samples;

                // Build a stable list of keys in recency order
                let keys: Vec<DocumentId> = cache.iter().map(|(k, _)| k.clone()).collect();
                if keys.is_empty() { break; }

                // Take from the tail (least-recently used end) up to max_samples
                let sample_count = keys.len().min(max_samples);
                let mut evicted_this_round = 0usize;
                for i in 0..sample_count.min(batch_size) {
                    let key = &keys[keys.len() - 1 - i];
                    if cache.pop(key).is_some() {
                        self.metrics.lru_evictions.fetch_add(1, Ordering::Relaxed);
                        evicted_this_round += 1;
                        needed -= 1;
                        if needed == 0 { break; }
                    }
                }
                if evicted_this_round == 0 { break; }
            }
        }
    }
}
