use nexus_lite::engine::Engine;
use nexus_lite::{api, query};

#[test]
fn api_telemetry_config_and_rate_limit() {
    let tmp = std::env::temp_dir().join("nexus_api_telemetry.wal");
    let engine = Engine::new(tmp).unwrap();
    let col = engine.create_collection("tapi".to_string());
    // Set defaults via API
    api::telemetry_set_db_name("apitest");
    api::telemetry_set_max_results_global(5000);
    api::telemetry_set_max_results_for(&col.name_str(), 100);
    api::telemetry_configure_rate_limit(&col.name_str(), 1, 0);

    // First find/count ok, second should rate-limit
    let filter = query::Filter::True;
    let _ = api::count(&engine, &col.name_str(), &filter).unwrap();
    let e = api::count(&engine, &col.name_str(), &filter).unwrap_err();
    assert!(matches!(e, nexus_lite::errors::DbError::RateLimited));
}
