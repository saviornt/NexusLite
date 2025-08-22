use nexus_lite::cache::{Cache, CacheConfig, EvictionMode};
use nexus_lite::document::{Document, DocumentType};
use bson::doc;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_cache_insert_and_get() {
    let cache = Cache::new(10);
    let doc = Document::new(doc! { "key": "value" }, DocumentType::Persistent);
    cache.insert(doc.clone());

    let retrieved = cache.get(&doc.id).unwrap();
    assert_eq!(retrieved, doc);
}

#[tokio::test]
async fn test_lru_eviction() {
    let cache = Cache::new(2);
    let doc1 = Document::new(doc! { "key": "value1" }, DocumentType::Persistent);
    let doc2 = Document::new(doc! { "key": "value2" }, DocumentType::Persistent);
    let doc3 = Document::new(doc! { "key": "value3" }, DocumentType::Persistent);

    cache.insert(doc1.clone());
    cache.insert(doc2.clone());
    cache.insert(doc3.clone());

    assert!(cache.get(&doc1.id).is_none());
    assert!(cache.get(&doc2.id).is_some());
    assert!(cache.get(&doc3.id).is_some());
}

#[tokio::test]
async fn test_ttl_eviction() {
    let cache = Cache::new(10);
    let mut doc = Document::new(doc! { "key": "value" }, DocumentType::Ephemeral);
    doc.set_ttl(Duration::from_millis(100));

    cache.insert(doc.clone());
    assert!(cache.get(&doc.id).is_some());

    sleep(Duration::from_millis(200)).await;

    // The purge task runs every 5 seconds, so we might need to wait for it.
    // For a more deterministic test, we could expose the purge function and call it manually.
    // For now, we'll just sleep and assume the purge has run.
    sleep(Duration::from_secs(6)).await;

    assert!(cache.get(&doc.id).is_none());
}

#[tokio::test]
async fn ttl_first_over_lru_when_full() {
    let cfg = CacheConfig { capacity: 2, batch_size: 10, ..Default::default() };
    let cache = Cache::new_with_config(cfg);

    // doc1 has TTL and will expire, doc2 no TTL
    let mut doc1 = Document::new(doc!{"k": 1}, DocumentType::Ephemeral);
    doc1.set_ttl(Duration::from_millis(50));
    let doc2 = Document::new(doc!{"k": 2}, DocumentType::Persistent);

    cache.insert(doc1.clone());
    cache.insert(doc2.clone());

    sleep(Duration::from_millis(80)).await; // allow doc1 to expire
    // Trigger capacity enforcement by inserting doc3. TTL-first should evict doc1 (expired), not doc2.
    let doc3 = Document::new(doc!{"k": 3}, DocumentType::Persistent);
    cache.insert(doc3.clone());

    assert!(cache.get(&doc2.id).is_some(), "Persistent doc should remain");
    assert!(cache.get(&doc1.id).is_none(), "Expired doc should be evicted first");
}

#[tokio::test]
async fn lru_sampling_correctness_when_no_ttls() {
    let mut cfg = CacheConfig { capacity: 3, batch_size: 2, max_samples: 2, ..Default::default() };
    cfg.eviction_mode = EvictionMode::LruOnly;
    let cache = Cache::new_with_config(cfg);

    let d1 = Document::new(doc!{"k": 1}, DocumentType::Persistent);
    let d2 = Document::new(doc!{"k": 2}, DocumentType::Persistent);
    let d3 = Document::new(doc!{"k": 3}, DocumentType::Persistent);
    cache.insert(d1.clone());
    cache.insert(d2.clone());
    cache.insert(d3.clone());

    // Touch d1 to make it more recent than d2/d3
    let _ = cache.get(&d1.id);

    // Insert d4; capacity=3, must evict via sampling (2 from tail). d2 or d3 should be gone; d1 should remain.
    let d4 = Document::new(doc!{"k": 4}, DocumentType::Persistent);
    cache.insert(d4.clone());

    assert!(cache.get(&d1.id).is_some(), "Most recently used should remain");
    let d2_present = cache.get(&d2.id).is_some();
    let d3_present = cache.get(&d3.id).is_some();
    assert!(!(d2_present && d3_present), "At least one of the two LRU candidates should be evicted by sampling");
}

#[tokio::test]
async fn batching_and_locking_under_pressure() {
    let cfg = CacheConfig { capacity: 5, batch_size: 3, ..Default::default() };
    let cache = Cache::new_with_config(cfg);

    // Fill to capacity
    let docs: Vec<_> = (0..5).map(|i| Document::new(doc!{"k": i}, DocumentType::Persistent)).collect();
    for d in &docs { cache.insert(d.clone()); }

    // Concurrent inserts should not race evictions (eviction lock protects)
    let cache1 = cache.clone();
    let cache2 = cache.clone();
    let t1 = tokio::spawn(async move {
        for i in 100..110 { cache1.insert(Document::new(doc!{"k": i}, DocumentType::Persistent)); }
    });
    let t2 = tokio::spawn(async move {
        for i in 200..210 { cache2.insert(Document::new(doc!{"k": i}, DocumentType::Persistent)); }
    });
    let _ = tokio::join!(t1, t2);

    // If lock works, we won’t panic and cache remains consistent
    let _snap = cache.metrics_snapshot();
}

#[tokio::test]
async fn lazy_expiration_counts_as_miss() {
    let cfg = CacheConfig { capacity: 4, ..Default::default() };
    let cache = Cache::new_with_config(cfg);

    let mut d = Document::new(doc!{"k": 1}, DocumentType::Ephemeral);
    d.set_ttl(Duration::from_millis(30));
    cache.insert(d.clone());
    assert!(cache.get(&d.id).is_some());

    sleep(Duration::from_millis(60)).await;
    // Access after expiration triggers lazy eviction
    assert!(cache.get(&d.id).is_none());
    let snap = cache.metrics_snapshot();
    assert!(snap.misses >= 1, "Lazy expiration should increment miss count");
}

#[tokio::test]
async fn purge_trigger_and_interval_tuning() {
    let mut cfg = CacheConfig { capacity: 3, ..Default::default() };
    cfg.purge_interval_secs = 60; // set high; we will trigger manually
    let cache = Cache::new_with_config(cfg);

    let mut d = Document::new(doc!{"k": 1}, DocumentType::Ephemeral);
    d.set_ttl(Duration::from_millis(10));
    cache.insert(d.clone());
    sleep(Duration::from_millis(30)).await;

    // Without waiting for background, force purge
    let evicted = cache.purge_expired_now();
    assert!(evicted >= 1);
}

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
