use proptest::prelude::*;

proptest! {
    #![proptest_config(proptest::test_runner::Config {
    failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions"))),
        .. proptest::test_runner::Config::default()
    })]
    #[test]
    #[ignore = "slow on CI; run in scheduled full builds"]
    fn prop_csv_infer_basic_ints(a in -100_000_i64..100_000, b in -100_000_i64..100_000) {
        use nexus_lite::engine::Engine;
        use std::io::Cursor;
    let csv = format!("x\n{a}\n{b}\n");
    let engine = Engine::new(std::env::temp_dir().join("prop_import.wasp")).unwrap();
        let mut opts = nexus_lite::import::ImportOptions::default();
        opts.csv.has_headers = true;
        opts.csv.type_infer = true;
        opts.collection = "prop".into();
        let cur = Cursor::new(csv.into_bytes());
        let rep = nexus_lite::import::import_from_reader(&engine, cur, nexus_lite::import::ImportFormat::Csv, &opts).unwrap();
        prop_assert_eq!(rep.inserted, 2);
    }
}
