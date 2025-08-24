use proptest::prelude::*;

proptest! {
    #![proptest_config(proptest::test_runner::Config {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions"))),
        cases: 20,
        .. proptest::test_runner::Config::default()
    })]
    #[test]
    fn prop_feature_flags_ensure_set_get_list(
        name in "[A-Za-z][A-Za-z0-9_-]{0,15}",
        default_enabled in any::<bool>(),
        set_enabled in any::<bool>()
    ) {
        use nexus_lite::utils::feature_flags as ff;
        // Use a unique name per case to avoid cross-test interference
        let unique = format!("propff_{}_{}", name, default_enabled as u8);
        ff::ensure(&unique, default_enabled, "prop test");
        let got = ff::get(&unique).expect("flag exists");
    prop_assert_eq!(got.name.as_str(), unique.as_str());
        prop_assert_eq!(got.enabled, default_enabled);

        let existed = ff::set(&unique, set_enabled);
        prop_assert!(existed);
        prop_assert_eq!(ff::is_enabled(&unique), set_enabled);

        let all = ff::list();
        prop_assert!(all.iter().any(|f| f.name == unique && f.enabled == set_enabled));
    }
}
