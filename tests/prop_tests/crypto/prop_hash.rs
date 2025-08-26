use proptest::prelude::*;

proptest! {
    #![proptest_config(proptest::test_runner::Config {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions"))),
        cases: 24,
        .. proptest::test_runner::Config::default()
    })]
    #[test]
    fn prop_hash_secret_fields_converts_strings_to_binary_and_keeps_len(msg in "[a-zA-Z0-9]{0,64}") {
        use nexuslite::crypto::hash_secrets::argon2::hash_secret_fields;
        let mut d = bson::doc!{ "pw": msg.clone(), "nonsecret": 1 };
        hash_secret_fields(&mut d, &["pw"]).unwrap();
        // pw becomes Binary
        let v = d.get("pw").unwrap();
        match v { bson::Bson::Binary(b) => { prop_assert_eq!(b.bytes.len(), 32); }, _ => prop_assert!(false, "pw not binary") }
        // nonsecret remains untouched
        prop_assert_eq!(d.get_i32("nonsecret").unwrap(), 1);
    }

    #[test]
    fn prop_hash_secret_fields_ignores_missing_fields(k in "[a-z]{1,8}") {
        use nexuslite::crypto::hash_secrets::argon2::hash_secret_fields;
        let mut d = bson::doc!{ "a": 1, "b": 2 };
        hash_secret_fields(&mut d, &[&k]).unwrap();
        // No panic and no additional keys introduced
        prop_assert!(d.keys().all(|name| name == "a" || name == "b"));
    }
}
