use nexus_lite::document::{Document, DocumentType};
use nexus_lite::query::{FindOptions, Order, SortSpec};
use proptest::prelude::*;

proptest! {
    #![proptest_config(proptest::test_runner::Config {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions"))),
        // Reduce the number of cases to speed up this particular property test
        cases: 16,
    // Cap the total time in milliseconds
    timeout: 20_000,
        .. proptest::test_runner::Config::default()
    })]
    #[test]
    #[ignore = "slow on CI; run in scheduled full builds"]
    fn prop_multi_key_sort_non_decreasing(v in proptest::collection::vec((any::<i64>(), any::<i64>()), 0..15)) {
        use nexus_lite::engine::Engine;
    let engine = Engine::new(std::env::temp_dir().join("prop_sort.wasp")).unwrap();
        let col = engine.create_collection("srt".into());
        for (a,b) in &v {
            let d = Document::new(bson::doc!{"a": *a, "b": *b}, DocumentType::Persistent);
            col.insert_document(d);
        }
        let opts = FindOptions {
            projection: None,
            sort: Some(vec![SortSpec{ field: "a".into(), order: Order::Asc }, SortSpec{ field: "b".into(), order: Order::Asc }]),
            limit: None,
            skip: None,
            timeout_ms: None
        };
    let cur = nexus_lite::query::find_docs(&col, &nexus_lite::query::Filter::True, &opts);
        let docs = cur.to_vec();
        // Check non-decreasing (lexicographic) by (a,b)
        for w in docs.windows(2) {
            let d0 = &w[0].data.0;
            let d1 = &w[1].data.0;
            let a0 = d0.get_i64("a").unwrap();
            let b0 = d0.get_i64("b").unwrap();
            let a1 = d1.get_i64("a").unwrap();
            let b1 = d1.get_i64("b").unwrap();
            prop_assert!(a0 < a1 || (a0 == a1 && b0 <= b1));
        }
    }
}
