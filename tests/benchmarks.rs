use bson::doc;
use nexuslite::{document::Document, document::DocumentType, engine::Engine, query};
use nexuslite::utils::devlog::{enable_thread_sink, drain};
use serde_json::Value;

#[test]
fn benchmark_database_and_metrics() {
    // Capture dev6 JSON benchmark lines deterministically
    let _g = enable_thread_sink();

    let dir = tempfile::tempdir().unwrap();
    let wasp_path = dir.path().join("bench_db.wasp");

    // Engine construction (emits WASP recovery time dev6)
    let engine = Engine::new(wasp_path).unwrap();
    let col = engine.create_collection("bm_col".to_string());

    // Seed documents (some with short TTL so we can force TTL evictions)
    for i in 0..200i32 {
        let mut d = Document::new(
            doc! {"k": i, "x": i % 3, "payload": format!("{:08}", i)},
            DocumentType::Persistent,
        );
        if i % 25 == 0 { d.set_ttl(std::time::Duration::from_millis(5)); }
        col.insert_document(d);
    }

    // Build an index to exercise used_index=true for find
    col.create_index("x", nexuslite::index::IndexKind::Hash);

    // Queries: find (indexed), count, update, delete
    let filter = query::Filter::Cmp {
        path: "x".into(),
        op: query::CmpOp::Eq,
        value: bson::Bson::Int32(1),
    };

    // find with options
    let mut opts = query::FindOptions::default();
    opts.limit = Some(50);
    opts.skip = Some(10);
    let cur = query::find_docs(&col, &filter, &opts);
    let found = cur.to_vec();
    assert!(!found.is_empty());

    // count
    let _count = query::count_docs(&col, &filter);

    // update many
    let upd = query::UpdateDoc { set: vec![("flag".into(), bson::Bson::Boolean(true))], ..Default::default() };
    let upd_r = query::update_many(&col, &filter, &upd);
    assert!(upd_r.matched >= upd_r.modified);

    // delete many (narrow filter)
    let del_filter = query::Filter::Cmp { path: "k".into(), op: query::CmpOp::Lt, value: bson::Bson::Int32(5) };
    let del_r = query::delete_many(&col, &del_filter);
    assert!(del_r.deleted > 0);

    // Give TTL items a moment to expire and purge once
    std::thread::sleep(std::time::Duration::from_millis(10));
    let _ = col.cache.purge_expired_now();

    // Access some docs: a mix of hits and misses
    // Hits: use existing IDs
    let ids = col.list_ids();
    for id in ids.iter().take(5) {
        let _ = col.cache.get(id);
    }
    // Misses: use fresh random IDs not present in cache
    for _ in 0..10 {
        let miss_id = nexuslite::types::DocumentId::new();
        let _ = col.cache.get(&miss_id);
    }

    // Cache metrics invariants
    let m = col.cache_metrics();
    assert!(m.inserts >= 190); // after deletes we still expect many inserts counted
    assert!(m.hits + m.misses >= 10);
    assert!(m.total_get_ns > 0);
    assert!(m.total_insert_ns > 0);
    // Evictions may be zero depending on timing; just record them in the saved report

    // Telemetry counters: force a rate-limited event and check JSON metrics
    nexuslite::telemetry::configure_rate_limit(&col.name_str(), 1, 0);
    let _ = nexuslite::telemetry::try_consume_token(&col.name_str(), 1); // consume one
    let _ = nexuslite::telemetry::try_consume_token(&col.name_str(), 2); // should increment rate_limited_total
    let telemetry_obj_now = nexuslite::telemetry::metrics_json();
    assert!(telemetry_obj_now.get("rate_limited_total").and_then(|v| v.as_u64()).unwrap_or(0) >= 1);

    // Collect benchmark logs (query and wasp recovery)
    let logs = drain();
    // WASP recovery init time present
    assert!(logs.iter().any(|l| l.contains("\"bench\":\"wasp\"")));
    assert!(logs.iter().any(|l| l.contains("\"op\":\"recover_init\"")));
    // Query operations present with result_count and used_index
    assert!(logs.iter().any(|l| l.contains("\"bench\":\"query\"")));
    assert!(logs.iter().any(|l| l.contains("\"op\":\"find\"")));
    assert!(logs.iter().any(|l| l.contains("\"result_count\"")));
    assert!(logs.iter().any(|l| l.contains("\"used_index\":true")));
    assert!(logs.iter().any(|l| l.contains("\"op\":\"count\"")));
    assert!(logs.iter().any(|l| l.contains("\"op\":\"update_many\"")));
    assert!(logs.iter().any(|l| l.contains("\"op\":\"delete_many\"")));

    // --- Persist a benchmarks.json for later inspection ---
    let mut parsed: Vec<Value> = Vec::new();
    for line in &logs {
        if let Ok(v) = serde_json::from_str::<Value>(line) {
            parsed.push(v);
        }
    }
    let mut wasp_logs: Vec<Value> = Vec::new();
    let mut query_logs: Vec<Value> = Vec::new();
    let mut cache_logs: Vec<Value> = Vec::new();
    for v in parsed.into_iter() {
        match v.get("bench").and_then(|b| b.as_str()) {
            Some("wasp") => wasp_logs.push(v),
            Some("query") => query_logs.push(v),
            Some("cache") => cache_logs.push(v),
            _ => {}
        }
    }

    // Structured telemetry metrics without `nexus_` prefix
    let telemetry_obj = nexuslite::telemetry::metrics_json();

    let report = serde_json::json!({
        "when": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        "cache_metrics": {
            "hits": m.hits,
            "misses": m.misses,
            "inserts": m.inserts,
            "removes": m.removes,
            "ttl_evictions": m.ttl_evictions,
            "lru_evictions": m.lru_evictions,
            "memory_bytes": m.memory_bytes,
            "total_get_ns": m.total_get_ns,
            "total_insert_ns": m.total_insert_ns,
            "total_remove_ns": m.total_remove_ns,
        },
    "telemetry_metrics": telemetry_obj,
        "wasp": wasp_logs,
    "query": query_logs,
    "cache": cache_logs,
    });

    let stamp = chrono::Utc::now().format("%Y%m%d_%H%M%S_%3fZ").to_string();
    let p1 = std::path::Path::new("./test_logs").join(&stamp);
    let p2 = std::path::Path::new("./benchmarks/results").join(&stamp);
    std::fs::create_dir_all(&p1).unwrap();
    std::fs::create_dir_all(&p2).unwrap();
    let f1 = p1.join("benchmarks.json");
    let f2 = p2.join("benchmarks.json");
    std::fs::write(&f1, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    std::fs::write(&f2, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    assert!(f1.exists());
    assert!(f2.exists());
}
