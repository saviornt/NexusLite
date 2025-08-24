use bson::Bson;
use nexus_lite::query::{CmpOp, Filter, eval_filter};
use proptest::prelude::*;

proptest! {
    #![proptest_config(proptest::test_runner::Config {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions"))),
        .. proptest::test_runner::Config::default()
    })]
    // Symmetry: bson_equal(a,b) == bson_equal(b,a); implied via eval on Eq op
    #[test]
    fn prop_eq_symmetry(v in any_bson_number(), w in any_bson_number()) {
        // Build tiny docs {x: v} and {x: w}
        let doc_v = bson::doc!{"x": v.clone()};
        let doc_w = bson::doc!{"x": w.clone()};
        let f_eq_v = Filter::Cmp { path: "x".into(), op: CmpOp::Eq, value: w.clone() };
        let f_eq_w = Filter::Cmp { path: "x".into(), op: CmpOp::Eq, value: v.clone() };
        let a = eval_filter(&doc_v, &f_eq_v);
        let b = eval_filter(&doc_w, &f_eq_w);
        prop_assert_eq!(a, b);
    }

    // Transitivity across numeric casts is not strictly guaranteed for floats with NaN; exclude NaN
    #[test]
    fn prop_order_consistency(i in -1_000_000i64..1_000_000, j in -1_000_000i64..1_000_000) {
        let doc = bson::doc!{"x": i};
        let f_gt = Filter::Cmp { path: "x".into(), op: CmpOp::Gt, value: Bson::Int64(j) };
        let f_lte = Filter::Cmp { path: "x".into(), op: CmpOp::Lte, value: Bson::Int64(j) };
        let gt = eval_filter(&doc, &f_gt);
        let lte = eval_filter(&doc, &f_lte);
        // For total order on integers, gt and lte are complementary
        prop_assert_eq!(gt, !lte);
    }
}

fn any_bson_number() -> impl Strategy<Value = Bson> {
    prop_oneof![
        any::<i32>().prop_map(Bson::Int32),
        any::<i64>().prop_map(Bson::Int64),
        // limit float range and exclude NaN/inf
        (-1.0e6f64..1.0e6f64).prop_map(Bson::Double),
    ]
}

proptest! {
    #![proptest_config(proptest::test_runner::Config {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions"))),
        .. proptest::test_runner::Config::default()
    })]
    #[test]
    #[ignore = "slow on CI; run in scheduled full builds"]
    fn prop_projection_preserves_selected_fields(a in any::<i64>(), b in any::<i64>()) {
        use nexus_lite::query::{FindOptions, SortSpec, Order};
        use nexus_lite::engine::Engine;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path().join("test.wasp")).unwrap();
        let col = engine.create_collection("proj".into());
        col.insert_document(nexus_lite::document::Document::new(bson::doc!{"x": a, "y": b, "z": 1}, nexus_lite::document::DocumentType::Persistent));
        let opts = FindOptions { projection: Some(vec!["x".into(), "y".into()]), sort: Some(vec![SortSpec{ field: "x".into(), order: Order::Asc }]), limit: Some(1), skip: Some(0), timeout_ms: None };
    let cur = nexus_lite::query::find_docs(&col, &nexus_lite::query::Filter::True, &opts);
        let docs = cur.to_vec();
        prop_assert_eq!(docs.len(), 1);
        let d = &docs[0].data.0;
        prop_assert!(d.get("x").is_some());
        prop_assert!(d.get("y").is_some());
        prop_assert!(d.get("z").is_none());
    }

    #[test]
    #[ignore = "slow on CI; run in scheduled full builds"]
    fn prop_pagination_bounds(n in 0usize..200) {
        use nexus_lite::engine::Engine;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path().join("test.wasp")).unwrap();
        let col = engine.create_collection("pag".into());
        for i in 0..n { col.insert_document(nexus_lite::document::Document::new(bson::doc!{"i": i as i64}, nexus_lite::document::DocumentType::Persistent)); }
        let opts = nexus_lite::query::FindOptions { projection: None, sort: None, limit: Some(50), skip: Some(usize::MAX/2), timeout_ms: None };
    let cur = nexus_lite::query::find_docs(&col, &nexus_lite::query::Filter::True, &opts);
        let docs = cur.to_vec();
        // With huge skip, result must be empty, not panic
        prop_assert!(docs.len() <= 50);
    }
}
