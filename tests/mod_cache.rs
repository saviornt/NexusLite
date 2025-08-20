use nexus_lite::cache::Cache;
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
