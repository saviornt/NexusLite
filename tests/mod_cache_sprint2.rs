use nexus_lite::cache::{Cache, CacheConfig, EvictionMode};
use nexus_lite::document::{Document, DocumentType};
use bson::doc;
use std::time::Duration;
use tokio::time::sleep;

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
