use nexus_lite::engine::Engine;
use nexus_lite::{api, query};

#[test]
fn telemetry_default_rate_limit_is_set() {
    // Force recompute default cfg by setting env (optional); then check would_limit eventually after consuming tokens
    // Set a known default bucket for collections without explicit config
    nexus_lite::telemetry::set_default_rate_limit(5, 1);
    // Ensure a default bucket is created lazily
    let collection = "telemetry_test";
    let limited_before = nexus_lite::telemetry::would_limit(collection, 1);
    // Might be false (fresh bucket full); consume aggressively to trigger limit
    for _ in 0..10 { let _ = nexus_lite::telemetry::try_consume_token(collection, 1); }
    let limited_after = nexus_lite::telemetry::would_limit(collection, 1);
    assert!(!limited_before || limited_after);
}

#[test]
fn api_returns_rate_limited_error() {
    let tmp = std::env::temp_dir().join("nexus_telemetry_rate_api.wal");
    let engine = Engine::new(tmp).unwrap();
    let col = engine.create_collection("users".to_string());
    // Configure tight rate: capacity 1, refill 0
    nexus_lite::telemetry::configure_rate_limit(&col.name_str(), 1, 0);
    // First call should pass, second should error
    let filter = query::Filter::True;
    let res1 = api::count(&engine, &col.name_str(), &filter);
    assert!(res1.is_ok());
    let res2 = api::count(&engine, &col.name_str(), &filter);
    match res2 {
        Err(nexus_lite::errors::DbError::RateLimitedWithRetry { .. } | nexus_lite::errors::DbError::RateLimited) => {},
        other => panic!("unexpected: {other:?}"),
    }
}
