use crate::cache::config::{CacheConfig, EvictionMode};
use crate::cache::metrics::{CacheMetrics, CacheMetricsSnapshot};
use crate::cache::policy::purge_expired;
use crate::cache::size::approximate_doc_size;
use crate::document::Document;
use crate::types::DocumentId;
use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

/// A thread-safe, in-memory cache with TTL-first + LRU fallback eviction.
#[derive(Clone)]
pub struct Cache {
    pub store: Arc<RwLock<LruCache<DocumentId, Document>>>,
    pub config: Arc<RwLock<CacheConfig>>, // runtime adjustable
    pub metrics: Arc<CacheMetrics>,
    eviction_lock: Arc<Mutex<()>>,
    pub(crate) freq: Arc<RwLock<HashMap<DocumentId, u64>>>,
    pub(crate) sizes: Arc<RwLock<HashMap<DocumentId, usize>>>,
}

impl Cache {
    /// Creates a new cache with a given capacity and starts the TTL eviction task.
    pub fn new(capacity: usize) -> Self {
        Self::new_with_config(CacheConfig { capacity, ..Default::default() })
    }

    /// Creates a new cache with the provided configuration.
    pub fn new_with_config(config: CacheConfig) -> Self {
        let cache = Cache {
            store: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(config.capacity.max(1))
                    .unwrap_or_else(|| NonZeroUsize::new(1).expect("NonZeroUsize(1) must exist")),
            ))),
            config: Arc::new(RwLock::new(config)),
            metrics: Arc::new(CacheMetrics::default()),
            eviction_lock: Arc::new(Mutex::new(())),
            freq: Arc::new(RwLock::new(HashMap::new())),
            sizes: Arc::new(RwLock::new(HashMap::new())),
        };

        // Spawn a background thread for TTL eviction
        let store_clone = cache.store.clone();
        let metrics_clone = cache.metrics.clone();
        let config_clone = cache.config.clone();
        let sizes_clone = cache.sizes.clone();
        let freq_clone = cache.freq.clone();
        std::thread::spawn(move || {
            loop {
                let secs = config_clone.read().purge_interval_secs;
                std::thread::sleep(Duration::from_secs(secs));
                purge_expired(&store_clone, &metrics_clone, &sizes_clone, &freq_clone);
            }
        });

        cache
    }

    /// Inserts a document into the cache.
    pub fn insert(&self, document: Document) {
        let start = std::time::Instant::now();
        // Evict as needed before insert to honor TTL-first policy
        self.enforce_capacity();

        // Update memory tracking
        let approx = approximate_doc_size(&document);
        {
            let mut sizes = self.sizes.write();
            if let Some(prev) = sizes.insert(document.id.clone(), approx) {
                self.metrics
                    .memory_bytes
                    .fetch_sub(crate::utils::num::usize_to_u64(prev), Ordering::Relaxed);
            }
            self.metrics
                .memory_bytes
                .fetch_add(crate::utils::num::usize_to_u64(approx), Ordering::Relaxed);
            // Developer benchmark log: memory added for insert
            crate::dev6!(
                "{{\"bench\":\"cache\",\"op\":\"mem_add\",\"bytes\":{}}}",
                crate::utils::num::usize_to_u64(approx)
            );
        }

        let id_clone = document.id.clone();
        self.store.write().put(id_clone.clone(), document);
        self.freq.write().insert(id_clone, 1);
        self.metrics.inserts.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .total_insert_ns
            .fetch_add(
                crate::utils::num::usize_to_u64(start.elapsed().as_nanos() as usize),
                Ordering::Relaxed,
            );
    }

    /// Retrieves a document from the cache.
    pub fn get(&self, id: &DocumentId) -> Option<Document> {
        let start = std::time::Instant::now();
        let mut guard = self.store.write();
        if let Some(doc) = guard.get(id) {
            if doc.is_expired() {
                // Lazy eviction on access
                guard.pop(id);
                self.metrics.ttl_evictions.fetch_add(1, Ordering::Relaxed);
                self.metrics.misses.fetch_add(1, Ordering::Relaxed);
                if let Some(sz) = self.sizes.write().remove(id) {
                    self.metrics
                        .memory_bytes
                        .fetch_sub(crate::utils::num::usize_to_u64(sz), Ordering::Relaxed);
                }
                self.freq.write().remove(id);
                self.metrics
                    .total_get_ns
                    .fetch_add(
                        crate::utils::num::usize_to_u64(start.elapsed().as_nanos() as usize),
                        Ordering::Relaxed,
                    );
                None
            } else {
                self.metrics.hits.fetch_add(1, Ordering::Relaxed);
                let mut f = self.freq.write();
                *f.entry(id.clone()).or_insert(0) += 1;
                self.metrics
                    .total_get_ns
                    .fetch_add(
                        crate::utils::num::usize_to_u64(start.elapsed().as_nanos() as usize),
                        Ordering::Relaxed,
                    );
                Some(doc.clone())
            }
        } else {
            self.metrics.misses.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .total_get_ns
                .fetch_add(
                    crate::utils::num::usize_to_u64(start.elapsed().as_nanos() as usize),
                    Ordering::Relaxed,
                );
            None
        }
    }

    /// Removes a document from the cache.
    pub fn remove(&self, id: &DocumentId) -> Option<Document> {
        let start = std::time::Instant::now();
        let removed = self.store.write().pop(id);
        if removed.is_some() {
            self.metrics.removes.fetch_add(1, Ordering::Relaxed);
            if let Some(sz) = self.sizes.write().remove(id) {
                let sz64 = crate::utils::num::usize_to_u64(sz);
                self.metrics.memory_bytes.fetch_sub(sz64, Ordering::Relaxed);
                // Developer benchmark log: memory freed by remove
                crate::dev6!(
                    "{{\"bench\":\"cache\",\"op\":\"mem_free\",\"bytes\":{}}}",
                    sz64
                );
            }
            self.freq.write().remove(id);
        }
        self.metrics
            .total_remove_ns
            .fetch_add(
                crate::utils::num::usize_to_u64(start.elapsed().as_nanos() as usize),
                Ordering::Relaxed,
            );
        removed
    }

    /// Clears the cache.
    pub fn clear(&self) {
        self.store.write().clear();
    }

    /// Force a TTL purge now. Returns number evicted.
    pub fn purge_expired_now(&self) -> usize {
        purge_expired(&self.store, &self.metrics, &self.sizes, &self.freq)
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
        let nz = NonZeroUsize::new(capacity.max(1))
            .unwrap_or_else(|| NonZeroUsize::new(1).expect("NonZeroUsize(1) must exist"));
        self.config.write().capacity = nz.get();
        self.store.write().resize(nz);
    }

    pub fn set_purge_interval_secs(&self, secs: u64) {
        self.config.write().purge_interval_secs = secs.max(1);
    }

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
            if len < cap {
                return;
            }
            needed = (len + 1).saturating_sub(cap);
        }

        // Try TTL evictions first if enabled
        {
            if needed > 0
                && (mode == EvictionMode::TtlFirst
                    || mode == EvictionMode::TtlOnly
                    || mode == EvictionMode::Hybrid)
            {
                let batch_limit = self.config.read().batch_size;
                let mut evicted_total = 0usize;
                while evicted_total < needed && evicted_total < batch_limit {
                    let evicted =
                        purge_expired(&self.store, &self.metrics, &self.sizes, &self.freq);
                    if evicted == 0 {
                        break;
                    }
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
            if mode == EvictionMode::TtlOnly {
                return;
            }

            let mut freed_lru_bytes_total: u64 = 0;
            while needed > 0 && cache.len() >= cap && !cache.is_empty() {
                let batch_size = self.config.read().batch_size.min(needed);
                let max_samples = self.config.read().max_samples;

                // Build a stable list of keys in recency order
                let keys: Vec<DocumentId> = cache.iter().map(|(k, _)| k.clone()).collect();
                if keys.is_empty() {
                    break;
                }

                // Sample from tail and choose victims by LFU (for Hybrid) or LRU (for LruOnly)
                let sample_count = keys.len().min(max_samples);
                let mut candidates: Vec<DocumentId> = Vec::with_capacity(sample_count);
                for i in 0..sample_count {
                    candidates.push(keys[keys.len() - 1 - i].clone());
                }

                let victims: Vec<DocumentId> = match mode {
                    EvictionMode::LruOnly => candidates.into_iter().take(batch_size).collect(),
                    EvictionMode::Hybrid | EvictionMode::TtlFirst => {
                        let freq_map = self.freq.read();
                        let mut scored: Vec<(u64, DocumentId)> = candidates
                            .into_iter()
                            .map(|k| (*freq_map.get(&k).unwrap_or(&0), k))
                            .collect();
                        scored.sort_by_key(|(f, _)| *f);
                        scored.into_iter().take(batch_size).map(|(_, k)| k).collect()
                    }
                    EvictionMode::TtlOnly => Vec::new(),
                    EvictionMode::LfuOnly => {
                        let freq_map = self.freq.read();
                        let mut scored: Vec<(u64, DocumentId)> = candidates
                            .into_iter()
                            .map(|k| (*freq_map.get(&k).unwrap_or(&0), k))
                            .collect();
                        scored.sort_by_key(|(f, _)| *f);
                        scored.into_iter().take(batch_size).map(|(_, k)| k).collect()
                    }
                };

                let mut evicted_this_round = 0usize;
                for key in victims {
                    if cache.pop(&key).is_some() {
                        self.metrics.lru_evictions.fetch_add(1, Ordering::Relaxed);
                        if let Some(sz) = self.sizes.write().remove(&key) {
                            let sz64 = crate::utils::num::usize_to_u64(sz);
                            self.metrics.memory_bytes.fetch_sub(sz64, Ordering::Relaxed);
                            freed_lru_bytes_total = freed_lru_bytes_total.saturating_add(sz64);
                            // Per-eviction developer log (optional granularity)
                            crate::dev6!(
                                "{{\"bench\":\"cache\",\"op\":\"lru_evict\",\"freed_bytes\":{}}}",
                                sz64
                            );
                        }
                        self.freq.write().remove(&key);
                        evicted_this_round += 1;
                        needed = needed.saturating_sub(1);
                        if needed == 0 {
                            break;
                        }
                    }
                }
                if evicted_this_round == 0 {
                    break;
                }
            }
            if freed_lru_bytes_total > 0 {
                crate::dev6!(
                    "{{\"bench\":\"cache\",\"op\":\"lru_summary\",\"freed_bytes\":{}}}",
                    freed_lru_bytes_total
                );
            }
        }
    }
}

/// Represents an entry in the cache.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct CacheEntry {
    pub document: Document,
    pub lru_timestamp: Instant,
}

#[allow(dead_code)]
impl CacheEntry {
    #[must_use]
    pub fn new(document: Document) -> Self {
        Self { document, lru_timestamp: Instant::now() }
    }
}
