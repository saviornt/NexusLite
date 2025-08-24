use nexus_lite::engine::Engine;
use nexus_lite::{api, query};
use uuid::Uuid;

#[test]
fn telemetry_default_rate_limit_is_set() {
    nexus_lite::telemetry::set_default_rate_limit(5, 1);
    let collection = "telemetry_test";
    let limited_before = nexus_lite::telemetry::would_limit(collection, 1);
    for _ in 0..10 {
        let _ = nexus_lite::telemetry::try_consume_token(collection, 1);
    }
    let limited_after = nexus_lite::telemetry::would_limit(collection, 1);
    assert!(!limited_before || limited_after);
}

#[test]
fn api_returns_rate_limited_error() {
    let tmp = std::env::temp_dir().join("nexus_telemetry_rate_api.wal");
    let engine = Engine::new(tmp).unwrap();
    let cname = format!("users_{}", Uuid::new_v4());
    let col = engine.create_collection(cname.clone());
    nexus_lite::telemetry::remove_rate_limit(&col.name_str());
    nexus_lite::telemetry::configure_rate_limit(&col.name_str(), 1, 0);
    let filter = query::Filter::True;
    let res1 = api::count(&engine, &col.name_str(), &filter);
    assert!(res1.is_ok());
    let res2 = api::count(&engine, &col.name_str(), &filter);
    match res2 {
        Err(
            nexus_lite::errors::DbError::RateLimitedWithRetry { .. }
            | nexus_lite::errors::DbError::RateLimited,
        ) => {}
        other => panic!("unexpected: {other:?}"),
    }
}
