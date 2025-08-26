use bson::Bson;
use nexuslite::query::{CmpOp, Filter, eval_filter, parse_filter_json};
use proptest::prelude::*;

fn bump_numeric(b: Bson) -> Bson {
    match b {
        Bson::Int32(i) => Bson::Int32(i.saturating_add(1)),
        Bson::Int64(i) => Bson::Int64(i.saturating_add(1)),
        Bson::Double(f) => Bson::Double(f + 1.0),
        _ => Bson::Int32(1),
    }
}

proptest! {
    #![proptest_config(proptest::test_runner::Config {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions"))),
        cases: 32,
        .. proptest::test_runner::Config::default()
    })]
        #[test]
        fn prop_parse_roundtrip_cmp_eq_for_numbers(v in any_bson_number()) {
            let json = format!("{{\"field\":\"x\",\"$eq\":{}}}", to_json_number(&v));
            let f = parse_filter_json(&json).unwrap();
            match f {
                Filter::Cmp { ref path, op: CmpOp::Eq, ref value } => {
                    prop_assert_eq!(path.as_str(), "x");
                    // Evaluate equality against a doc that contains exactly the parsed value
                    let d_ok = bson::doc!{"x": value.clone()};
                    prop_assert!(eval_filter(&d_ok, &f));
                    // And a doc with a different value should not match
                    let d_bad = bump_numeric(value.clone());
                    let d_bad = bson::doc!{"x": d_bad};
                    prop_assert!(!eval_filter(&d_bad, &f));
                }
                _ => prop_assert!(false, "not Cmp Eq"),
            }
    }

    #[test]
    fn prop_numeric_order_via_filters(a in any_bson_number(), b in any_bson_number()) {
        // Build docs
        let da = bson::doc!{"x": a.clone()};
        let db = bson::doc!{"x": b.clone()};
        // a > b implies not (b > a)
        let a_gt_b = eval_filter(&da, &Filter::Cmp { path: "x".into(), op: CmpOp::Gt, value: b.clone() });
        let b_gt_a = eval_filter(&db, &Filter::Cmp { path: "x".into(), op: CmpOp::Gt, value: a.clone() });
        prop_assert!(!(a_gt_b && b_gt_a));
        // a == a
        let a_eq_a = eval_filter(&da, &Filter::Cmp { path: "x".into(), op: CmpOp::Eq, value: a.clone() });
        prop_assert!(a_eq_a);
    }
}

fn any_bson_number() -> impl Strategy<Value = Bson> {
    prop_oneof![
        any::<i32>().prop_map(Bson::Int32),
        any::<i64>().prop_map(Bson::Int64),
        (-1.0e6f64..1.0e6f64).prop_map(Bson::Double),
    ]
}

fn to_json_number(b: &Bson) -> String {
    // helper bump_numeric is defined above
    match b {
        Bson::Int32(i) => i.to_string(),
        Bson::Int64(i) => i.to_string(),
        Bson::Double(f) => {
            if f.is_finite() {
                format!("{}", f)
            } else {
                "0".into()
            }
        }
        _ => "0".into(),
    }
}
