/// Eviction modes to control cache behavior.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvictionMode {
    TtlFirst,
    LruOnly,
    TtlOnly,
    Hybrid,
    LfuOnly,
}

/// Configuration for the cache.
#[derive(Clone, Debug)]
pub struct CacheConfig {
    pub capacity: usize,
    pub max_samples: usize, // reserved for future sampling strategy
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
