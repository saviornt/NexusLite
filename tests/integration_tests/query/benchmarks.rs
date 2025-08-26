use nexuslite::{engine::Engine, query};
use nexuslite::utils::devlog::{enable_thread_sink, drain};
use bson::doc;

#[test]
fn bench_logs_query_find_and_count() {
    let _g = enable_thread_sink();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bench_q.wasp");
    let e = Engine::new(path).unwrap();
    let col = e.create_collection("benchq".to_string());
    for i in 0..50i32 {
        let mut d = nexuslite::document::Document::new(
            doc! {"k": i, "x": i % 2, "payload": format!("{:08}", i)},
            nexuslite::document::DocumentType::Persistent,
        );
        if i % 10 == 0 { d.set_ttl(std::time::Duration::from_millis(1)); }
        col.insert_document(d);
    }
    // Build an index to exercise used_index=true path
    col.create_index("x", nexuslite::index::IndexKind::Hash);
    let filter = query::Filter::Cmp { path: "x".into(), op: query::CmpOp::Eq, value: bson::Bson::Int32(1) };
    let mut opts = query::FindOptions::default();
    opts.limit = Some(10);
    let _cursor = query::find_docs(&col, &filter, &opts);
    let _n = query::count_docs(&col, &filter);
    let logs = drain();
    assert!(logs.iter().any(|l| l.contains("\"bench\":\"query\"")));
    assert!(logs.iter().any(|l| l.contains("\"op\":\"find\"")));
    assert!(logs.iter().any(|l| l.contains("\"op\":\"count\"")));
}

#[test]
fn bench_logs_wasp_recovery_time() {
    let _g = enable_thread_sink();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bench_recover.wasp");
    let _e = Engine::new(path).unwrap();
    let logs = drain();
    assert!(logs.iter().any(|l| l.contains("\"bench\":\"wasp\"")));
    assert!(logs.iter().any(|l| l.contains("\"op\":\"recover_init\"")));
}
