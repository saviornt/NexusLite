use nexuslite::feature_flags as ff;

#[test]
fn list_and_toggle_flags() {
    // PQC is not a runtime-toggleable feature anymore; ensure it is not present
    let list = ff::list();
    assert!(!list.iter().any(|f| f.name == "crypto-pqc"));
    // Renamed crypto flag should be present and enabled by default
    assert!(list.iter().any(|f| f.name == "crypto" && f.enabled));
    // Can toggle a known flag (open-metrics) without panic; restore state after
    let before = ff::get("open-metrics").unwrap();
    let _ = ff::set("open-metrics", !before.enabled);
    assert_eq!(ff::is_enabled("open-metrics"), !before.enabled);
    let _ = ff::set("open-metrics", before.enabled);
}

#[test]
fn pqc_calls_error_as_not_implemented() {
    // Regardless of flag state, PQC stubs must return FeatureNotImplemented
    let err = nexuslite::crypto::pqc::kem_derive_shared_secret().unwrap_err();
    match err {
        nexuslite::errors::DbError::FeatureNotImplemented(name) => assert_eq!(name, "crypto-pqc"),
        other => panic!("unexpected error: {:?}", other),
    }
    let err2 = nexuslite::crypto::pqc::sphincs_verify(b"m", b"s").unwrap_err();
    match err2 {
        nexuslite::errors::DbError::FeatureNotImplemented(name) => assert_eq!(name, "crypto-pqc"),
        other => panic!("unexpected error: {other:?}"),
    }
}
