use proptest::prelude::*;

proptest! {
    #![proptest_config(proptest::test_runner::Config {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions"))),
        cases: 10,
        .. proptest::test_runner::Config::default()
    })]
    #[test]
    fn prop_db_init_creates_log_files(stem in "[A-Za-z0-9_-]{1,12}") {
        use tempfile::tempdir;
        // Avoid leading dots or invalid names
        let stem = stem.trim_matches('.');
        prop_assume!(!stem.is_empty());
        let dir = tempdir().unwrap();
        let db_path = dir.path().join(stem);
    let _db = nexuslite::Database::new(db_path.to_str()).unwrap();
        let logs = dir.path().join(format!("{}_logs", stem));
    let app_log = logs.join(format!("{}.log", stem));
    let usage_log = logs.join(format!("{}_audit.log", stem));
    let metrics_log = logs.join(format!("{}_metrics.log", stem));
    prop_assert!(app_log.exists());
    prop_assert!(usage_log.exists());
    prop_assert!(metrics_log.exists());
    }

    #[test]
    fn prop_dev6_thread_local_captures(messages in proptest::collection::vec("[A-Za-z0-9 _-]{0,32}", 0..10)) {
        // Capture with thread-local sink; avoid touching global logger
        let _sink = nexuslite::utils::devlog::enable_thread_sink();
        for m in &messages {
            nexuslite::dev6!("{}", m);
        }
        let captured = nexuslite::utils::devlog::drain();
        // All emitted messages should be present in some form (empty strings allowed)
        for m in &messages {
            prop_assert!(captured.iter().any(|s| s.contains(m)));
        }
    }
}
