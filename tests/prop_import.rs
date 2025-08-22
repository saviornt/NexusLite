use proptest::prelude::*;

proptest! {
    #[test]
    fn prop_csv_infer_basic_ints(a in -100000i64..100000, b in -100000i64..100000) {
        use nexus_lite::engine::Engine;
        use std::io::Cursor;
        let csv = format!("x\n{}\n{}\n", a, b);
        let engine = Engine::new(std::env::temp_dir().join("prop_import_wal.log")).unwrap();
        let mut opts = nexus_lite::import::ImportOptions::default();
        opts.csv.has_headers = true;
        opts.csv.type_infer = true;
        opts.collection = "prop".into();
        let cur = Cursor::new(csv.into_bytes());
        let rep = nexus_lite::import::import_from_reader(&engine, cur, nexus_lite::import::ImportFormat::Csv, &opts).unwrap();
        prop_assert_eq!(rep.inserted, 2);
    }
}
