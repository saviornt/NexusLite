use nexuslite::engine::Engine;
use nexuslite::{api, query};
use uuid::Uuid;

#[test]
fn telemetry_default_rate_limit_is_set() {
    nexuslite::telemetry::set_default_rate_limit(5, 1);
    let collection = "telemetry_test";
    let limited_before = nexuslite::telemetry::would_limit(collection, 1);
    for _ in 0..10 {
        let _ = nexuslite::telemetry::try_consume_token(collection, 1);
    }
    let limited_after = nexuslite::telemetry::would_limit(collection, 1);
    assert!(!limited_before || limited_after);
}

#[test]
fn api_returns_rate_limited_error() {
    let dir = tempfile::tempdir().unwrap();
    let tmp = dir.path().join("nexus_telemetry_rate_api.wal");
    let engine = Engine::new(tmp).unwrap();
    let cname = format!("users_{}", Uuid::new_v4());
    let col = engine.create_collection(cname.clone());
    nexuslite::telemetry::remove_rate_limit(&col.name_str());
    nexuslite::telemetry::configure_rate_limit(&col.name_str(), 1, 0);
    let filter = query::Filter::True;
    let res1 = api::count(&engine, &col.name_str(), &filter);
    assert!(res1.is_ok());
    let res2 = api::count(&engine, &col.name_str(), &filter);
    match res2 {
        Err(
            nexuslite::errors::DbError::RateLimitedWithRetry { .. }
            | nexuslite::errors::DbError::RateLimited,
        ) => {}
        other => panic!("unexpected: {other:?}"),
    }
}
