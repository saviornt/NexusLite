use bson::doc;
use nexus_lite::cache::{Cache, CacheConfig, EvictionMode};
use nexus_lite::document::{Document, DocumentType};

// Validate that Hybrid (LFU among LRU-tail samples) can choose a more recent but lower-frequency victim,
// whereas pure LRU evicts the least recent regardless of frequency.
#[tokio::test]
async fn lfu_vs_lru_victim_selection_differs() {
    // We need at least 5 to build a clear tail sample of two keys with different frequencies.
    let base_cfg = CacheConfig { capacity: 5, max_samples: 2, batch_size: 5, ..Default::default() };

    // Build a workload where the two least recent keys end up as [B (LRU, freq 3), C (2nd LRU, freq 2)].
    // In LRU mode, B will be evicted; in Hybrid mode, C (lower freq) will be evicted.
    let build_sequence = |cache: &Cache| {
        let a = Document::new(doc! {"k": "A"}, DocumentType::Persistent);
        let b = Document::new(doc! {"k": "B"}, DocumentType::Persistent);
        let c = Document::new(doc! {"k": "C"}, DocumentType::Persistent);
        let d = Document::new(doc! {"k": "D"}, DocumentType::Persistent);
        let e = Document::new(doc! {"k": "E"}, DocumentType::Persistent);
        let f = Document::new(doc! {"k": "F"}, DocumentType::Persistent);

        // Insert A..E
        cache.insert(a.clone());
        cache.insert(b.clone());
        cache.insert(c.clone());
        cache.insert(d.clone());
        cache.insert(e.clone());

        // Access pattern to set frequencies and recency:
        // Start order (MRU->LRU): E, D, C, B, A
        // get B twice -> B MRU, freq(B)=3 (insert+2 gets)
        let _ = cache.get(&b.id);
        let _ = cache.get(&b.id);

        // get C once -> C MRU, freq(C)=2 (insert+1 get)
        let _ = cache.get(&c.id);

        // get E then D then A to push them ahead of B and C
        let _ = cache.get(&e.id);
        let _ = cache.get(&d.id);
        let _ = cache.get(&a.id);

        // At this point, the LRU tail order should be [B (LRU, freq 3), C (2nd LRU, freq 2)]

        (a, b, c, d, e, f)
    };

    // LRU-only baseline
    let mut cfg_lru = base_cfg.clone();
    cfg_lru.eviction_mode = EvictionMode::LruOnly;
    let cache_lru = Cache::new_with_config(cfg_lru);
    let (_a, b_lru, c_lru, _d, _e, f_lru) = build_sequence(&cache_lru);
    // Insert F to force eviction; LRU should evict B (the true LRU)
    cache_lru.insert(f_lru.clone());
    assert!(cache_lru.get(&b_lru.id).is_none(), "LRU should evict the least recent (B)");
    assert!(cache_lru.get(&c_lru.id).is_some(), "Second least recent (C) should remain under LRU");

    // Hybrid (LFU within tail samples) should evict the lower-frequency among the tail: C
    let mut cfg_hybrid = base_cfg;
    cfg_hybrid.eviction_mode = EvictionMode::Hybrid;
    let cache_hybrid = Cache::new_with_config(cfg_hybrid);
    let (_a, b_h, c_h, _d, _e, f_h) = build_sequence(&cache_hybrid);
    cache_hybrid.insert(f_h.clone());
    assert!(cache_hybrid.get(&c_h.id).is_none(), "Hybrid (LFU) should evict lower-frequency C among the tail");
    assert!(cache_hybrid.get(&b_h.id).is_some(), "Hybrid should keep higher-frequency B even if older than C");
}

#[tokio::test]
async fn metrics_memory_and_latency_update() {
    let cfg = CacheConfig { capacity: 8, ..Default::default() };
    let cache = Cache::new_with_config(cfg);

    let d1 = Document::new(doc!{"k": 1}, DocumentType::Persistent);
    let id = d1.id.clone();
    cache.insert(d1);

    let snap_after_insert = cache.metrics_snapshot();
    assert!(snap_after_insert.inserts >= 1);
    assert!(snap_after_insert.memory_bytes > 0);
    assert!(snap_after_insert.total_insert_ns > 0);

    let _ = cache.get(&id);
    let snap_after_get = cache.metrics_snapshot();
    assert!(snap_after_get.hits + snap_after_get.misses >= 1);
    assert!(snap_after_get.total_get_ns > 0);

    let _removed = cache.remove(&id);
    let snap_after_remove = cache.metrics_snapshot();
    assert!(snap_after_remove.removes >= 1);
    assert!(snap_after_remove.total_remove_ns > 0);
    // Memory should not increase after removal; it should go down or stay the same if concurrent ops happened
    assert!(snap_after_remove.memory_bytes <= snap_after_get.memory_bytes);
}
