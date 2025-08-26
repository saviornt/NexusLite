use nexuslite::telemetry;
use std::thread;
use std::time::Duration;

#[test]
fn rate_limit_threshold_boundaries() {
    telemetry::configure_rate_limit("edges", 3, 0);
    assert!(!telemetry::would_limit("edges", 1));
    assert!(!telemetry::would_limit("edges", 3));
    assert!(telemetry::would_limit("edges", 4));
    assert!(telemetry::try_consume_token("edges", 1));
    assert!(telemetry::try_consume_token("edges", 1));
    assert!(telemetry::try_consume_token("edges", 1));
    assert!(telemetry::would_limit("edges", 1));
}

#[test]
fn retry_after_ms_reports_wait_time() {
    telemetry::configure_rate_limit("edges2", 2, 1);
    assert!(telemetry::try_consume_token("edges2", 2));
    assert!(telemetry::would_limit("edges2", 1));
    let t = telemetry::retry_after_ms("edges2", 1);
    assert!((900..=1100).contains(&t), "expected ~1s wait, got {}ms", t);
    thread::sleep(Duration::from_millis(1100));
    assert!(!telemetry::would_limit("edges2", 1));
}

#[test]
fn max_result_limit_per_collection_overrides_global() {
    telemetry::set_max_result_limit_global(1000);
    telemetry::set_max_result_limit_for("abc", 123);
    assert_eq!(telemetry::max_result_limit(), 1000);
    assert_eq!(telemetry::max_result_limit_for("abc"), 123);
    assert_eq!(telemetry::max_result_limit_for("other"), 1000);
}
