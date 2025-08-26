use proptest::prelude::*;

proptest! {
    #![proptest_config(proptest::test_runner::Config {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions"))),
        cases: 24,
        .. proptest::test_runner::Config::default()
    })]
    #[test]
    fn prop_normalize_db_path_appends_db_ext_when_missing(mut name in "[A-Za-z0-9_-]{0,32}") {
    use nexuslite::utils::fsutil::normalize_db_path;
        if name.is_empty() { name = "".into(); }
        let p = if name.is_empty() { normalize_db_path(None) } else { normalize_db_path(Some(&name)) };
        let ext = p.extension().and_then(|s| s.to_str()).unwrap_or("");
        prop_assert_eq!(ext, "db");
        // Should be absolute
        prop_assert!(p.is_absolute());
    }

    #[test]
    fn prop_open_create_secure_files(temp_name in "[A-Za-z0-9_-]{1,16}") {
    use nexuslite::utils::fsutil::{create_secure, open_rw_no_trunc};
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join(format!("{temp_name}.bin"));
        // create_secure should succeed and not truncate on second open
        {
            let _ = create_secure(&p).unwrap();
        }
        // Write some bytes
        std::fs::write(&p, b"abc").unwrap();
        {
            let _ = open_rw_no_trunc(&p).unwrap();
        }
        // Ensure contents still present
        let contents = std::fs::read(&p).unwrap();
        prop_assert_eq!(contents, b"abc");
    }
}
