use nexus_lite::telemetry;
use std::thread;
use std::time::Duration;

#[test]
fn rate_limit_threshold_boundaries() {
    // Configure capacity=3, refill=0 to make behavior deterministic
    telemetry::configure_rate_limit("edges", 3, 0);

    // Initially should allow up to capacity; would_limit should reflect strictly-less-than logic
    assert!(!telemetry::would_limit("edges", 1));
    assert!(!telemetry::would_limit("edges", 3));
    assert!(telemetry::would_limit("edges", 4));

    // Consume exactly capacity and verify further requests are limited
    assert!(telemetry::try_consume_token("edges", 1));
    assert!(telemetry::try_consume_token("edges", 1));
    assert!(telemetry::try_consume_token("edges", 1));
    assert!(telemetry::would_limit("edges", 1));
}

#[test]
fn retry_after_ms_reports_wait_time() {
    telemetry::configure_rate_limit("edges2", 2, 1); // 2 capacity, 1 token/sec
    // Drain tokens
    assert!(telemetry::try_consume_token("edges2", 2));
    assert!(telemetry::would_limit("edges2", 1));
    let t = telemetry::retry_after_ms("edges2", 1);
    assert!((900..=1100).contains(&t), "expected ~1s wait, got {}ms", t);

    // After sleeping, should allow again
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
