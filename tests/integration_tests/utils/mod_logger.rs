use std::fs;
use tempfile::tempdir;

#[test]
fn configure_logging_writes_app_audit_metrics_in_dir() {
    let dir = tempdir().unwrap();
    let base = dir.path().join("proc_logs");
    fs::create_dir_all(&base).unwrap();
    nexuslite::logger::configure_logging(Some(&base), Some("debug"), Some(3));
    // Emit some logs into categories
    log::info!("hello app");
    log::info!(target: "nexuslite::audit", "audit event");
    log::info!(target: "nexuslite::metrics", "metric event");
    // Files should exist
    assert!(base.join("app.log").exists());
    assert!(base.join("audit.log").exists());
    assert!(base.join("metrics.log").exists());
}

#[test]
fn configure_logging_explicit_params() {
    let dir = tempdir().unwrap();
    let base = dir.path().join("explicit_logs");
    fs::create_dir_all(&base).unwrap();
    nexuslite::logger::configure_logging(Some(&base), Some("trace"), Some(2));
    log::trace!("trace app");
    log::info!(target: "nexuslite::audit", "u");
    log::info!(target: "nexuslite::metrics", "m");
    assert!(base.join("app.log").exists());
    assert!(base.join("audit.log").exists());
    assert!(base.join("metrics.log").exists());
}
