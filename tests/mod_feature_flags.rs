use nexus_lite::feature_flags as ff;

#[test]
fn list_and_toggle_flags() {
    // crypto-pqc exists and is disabled by default
    let list = ff::list();
    assert!(list.iter().any(|f| f.name == "crypto-pqc" && !f.enabled));
    // Toggle on and off
    assert!(ff::set("crypto-pqc", true));
    assert!(ff::is_enabled("crypto-pqc"));
    assert!(ff::set("crypto-pqc", false));
    assert!(!ff::is_enabled("crypto-pqc"));
}

#[test]
fn pqc_calls_error_as_not_implemented() {
    // Regardless of flag state, PQC stubs must return FeatureNotImplemented
    let err = nexus_lite::crypto::pqc::kem_derive_shared_secret().unwrap_err();
    match err {
        nexus_lite::errors::DbError::FeatureNotImplemented(name) => assert_eq!(name, "crypto-pqc"),
        other => panic!("unexpected error: {:?}", other),
    }
    let err2 = nexus_lite::crypto::pqc::sphincs_verify(b"m", b"s").unwrap_err();
    match err2 {
        nexus_lite::errors::DbError::FeatureNotImplemented(name) => assert_eq!(name, "crypto-pqc"),
        other => panic!("unexpected error: {:?}", other),
    }
}
