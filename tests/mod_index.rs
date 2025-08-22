use bson::{doc, Bson, Document as BsonDocument};
use nexus_lite::index::{IndexKind, IndexManager, index_insert_all, index_remove_all, lookup_eq, lookup_range};
use nexus_lite::types::DocumentId;
use nexus_lite::query::{self, Filter, CmpOp, FindOptions};
use nexus_lite::engine::Engine;
use std::fs;
use tempfile::tempdir;
use std::sync::{Mutex, OnceLock};

fn with_env_lock<F: FnOnce()>(f: F) {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let m = LOCK.get_or_init(|| Mutex::new(()));
    // Avoid poisoning panics: if poisoned, try to recover via into_inner
    match m.lock() {
        Ok(_g) => f(),
        Err(_poison) => {
            // Best-effort: execute anyway to ensure tests continue; env var isolation may still hold
            f();
        }
    }
}

fn make_doc_i32(k: i32) -> (BsonDocument, DocumentId) {
    (doc! { "k": k }, DocumentId::new())
}

fn make_doc_f64(k: f64) -> (BsonDocument, DocumentId) {
    (doc! { "k": k }, DocumentId::new())
}

#[test]
fn test_hash_index_equality_i32() {
    let mut mgr = IndexManager::new();
    mgr.create_index("k", IndexKind::Hash);

    // Insert 0..100
    let mut ids: Vec<(DocumentId, i32)> = Vec::new();
    for i in 0..100i32 {
        let (doc, id) = make_doc_i32(i);
        index_insert_all(&mut mgr, &doc, &id);
        ids.push((id, i));
    }

    // Lookup equality that exists
    let out = lookup_eq(&mut mgr, "k", &Bson::Int32(42)).expect("should find eq match");
    assert_eq!(out.len(), 1, "expect exactly one match for unique key");
    let found = &out[0];
    let expected = ids.iter().find(|(_, v)| *v == 42).unwrap().0.clone();
    assert_eq!(*found, expected);

    // Lookup equality that doesn't exist
    assert!(lookup_eq(&mut mgr, "k", &Bson::Int32(420)).is_none());
}

#[test]
fn test_hash_index_equality_f64() {
    let mut mgr = IndexManager::new();
    mgr.create_index("k", IndexKind::Hash);

    let (d1, id1) = make_doc_f64(1.5);
    let (d2, id2) = make_doc_f64(2.5);
    index_insert_all(&mut mgr, &d1, &id1);
    index_insert_all(&mut mgr, &d2, &id2);

    let out = lookup_eq(&mut mgr, "k", &Bson::Double(2.5)).expect("should find 2.5");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0], id2);
}

#[test]
fn test_btree_index_range_queries() {
    let mut mgr = IndexManager::new();
    mgr.create_index("k", IndexKind::BTree);

    // Insert 0..100
    let mut ids: Vec<(DocumentId, i32)> = Vec::new();
    for i in 0..100i32 {
        let (doc, id) = make_doc_i32(i);
        index_insert_all(&mut mgr, &doc, &id);
        ids.push((id, i));
    }

    // Inclusive range [10, 20]
    let out = lookup_range(&mut mgr, "k", Some(&Bson::Int32(10)), Some(&Bson::Int32(20)), true, true).expect("range should match");
    let expected_set: std::collections::HashSet<_> = ids.iter().filter(|(_, v)| (10..=20).contains(v)).map(|(id, _)| id.clone()).collect();
    let out_set: std::collections::HashSet<_> = out.into_iter().collect();
    assert_eq!(out_set, expected_set, "inclusive bounds should include 10 and 20");

    // Exclusive range (10, 20)
    let out2 = lookup_range(&mut mgr, "k", Some(&Bson::Int32(10)), Some(&Bson::Int32(20)), false, false).expect("range should match");
    let expected_set2: std::collections::HashSet<_> = ids.iter().filter(|(_, v)| (11..=19).contains(v)).map(|(id, _)| id.clone()).collect();
    let out_set2: std::collections::HashSet<_> = out2.into_iter().collect();
    assert_eq!(out_set2, expected_set2, "exclusive bounds should exclude 10 and 20");

    // Unbounded min ..<=5
    let out3 = lookup_range(&mut mgr, "k", None, Some(&Bson::Int32(5)), false, true).expect("range should match");
    let expected_set3: std::collections::HashSet<_> = ids.iter().filter(|(_, v)| *v <= 5).map(|(id, _)| id.clone()).collect();
    let out_set3: std::collections::HashSet<_> = out3.into_iter().collect();
    assert_eq!(out_set3, expected_set3);

    // No matches should return None
    assert!(lookup_range(&mut mgr, "k", Some(&Bson::Int32(200)), Some(&Bson::Int32(300)), true, true).is_none());
}

#[test]
fn test_index_remove_updates_views() {
    let mut mgr = IndexManager::new();
    mgr.create_index("k", IndexKind::Hash);

    let (d42, id42) = make_doc_i32(42);
    index_insert_all(&mut mgr, &d42, &id42);
    assert_eq!(lookup_eq(&mut mgr, "k", &Bson::Int32(42)).unwrap().len(), 1);

    // Remove, expect no result
    index_remove_all(&mut mgr, &d42, &id42);
    assert!(lookup_eq(&mut mgr, "k", &Bson::Int32(42)).is_none());
}

#[test]
fn test_planner_uses_index_equality_stats_hit() {
    // Engine/collection (use temp dir to avoid leftover files)
    let dir = tempdir().unwrap();
    let engine = Engine::new(dir.path().join("wal_planner.bin")).unwrap();
    let col = engine.create_collection("pidx".into());
    for i in 0..50i32 { col.insert_document(nexus_lite::document::Document::new(doc!{"k": i}, nexus_lite::document::DocumentType::Persistent)); }
    col.create_index("k", IndexKind::Hash);

    // Run query that should use the index
    let filter = Filter::Cmp { path: "k".into(), op: CmpOp::Eq, value: 21.into() };
    let cur = query::find_docs(&&col, &filter, &FindOptions::default());
    let docs = cur.to_vec();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].data.0.get_i32("k").unwrap(), 21);

    // Check index hit incremented
    let mut hits = 0u64;
    {
        let mut mgr = col.indexes.write();
        if let Some(idx) = mgr.indexes.get_mut("k") {
            match idx {
                nexus_lite::index::IndexImpl::Hash(h) => hits = h.stats.hits,
                nexus_lite::index::IndexImpl::BTree(b) => hits = b.stats.hits,
            }
        }
    }
    assert!(hits > 0, "expected index hits to increase after planner lookup");
}

#[test]
fn test_collection_index_invalidation_update_delete() {
    let dir = tempdir().unwrap();
    let engine = Engine::new(dir.path().join("wal_inval.bin")).unwrap();
    let col = engine.create_collection("cidx".into());
    let d = nexus_lite::document::Document::new(doc!{"k": 5, "v": "x"}, nexus_lite::document::DocumentType::Persistent);
    let id = d.id.clone();
    col.insert_document(d);
    col.create_index("k", IndexKind::Hash);

    // Query by old value
    let filter_old = Filter::Cmp { path: "k".into(), op: CmpOp::Eq, value: 5.into() };
    assert_eq!(query::count_docs(&&col, &filter_old), 1);

    // Update indexed field: k:5 -> k:7
    let d2 = nexus_lite::document::Document::new(doc!{"k": 7, "v": "x"}, nexus_lite::document::DocumentType::Persistent);
    let ok = col.update_document(&id, d2);
    assert!(ok);

    // Old value should not match; new value should
    let filter_new = Filter::Cmp { path: "k".into(), op: CmpOp::Eq, value: 7.into() };
    assert_eq!(query::count_docs(&&col, &filter_old), 0);
    assert_eq!(query::count_docs(&&col, &filter_new), 1);

    // Delete and ensure no match
    assert!(col.delete_document(&id));
    assert_eq!(query::count_docs(&&col, &filter_new), 0);
}

#[test]
fn test_index_metadata_persistence_and_rebuild() { with_env_lock(|| {
    // Use a temp dir for WAL and metadata
    let dir = tempdir().unwrap();
    let meta_path = dir.path().join("nexus_indexes_test.json");
    unsafe { std::env::set_var("NEXUS_INDEX_META", meta_path.to_string_lossy().to_string()); }
    let _ = fs::remove_file(&meta_path);

    // First run: create engine, create collection/index, then persist
    let engine = Engine::new(dir.path().join("wal_meta.bin")).unwrap();
    let col = engine.create_collection("meta_col".into());
    col.create_index("k", IndexKind::Hash);
    // Force save
    engine.save_indexes_metadata().expect("failed to save index metadata");
    assert!(meta_path.exists(), "metadata file should be written");

    // Second run: new engine should rebuild index automatically
    let engine2 = Engine::new(dir.path().join("wal_meta.bin")).unwrap();
    let col2 = engine2.get_collection("meta_col").unwrap();
    let mgr = col2.indexes.read();
    let descs = mgr.descriptors();
    assert_eq!(descs.len(), 1);
    assert_eq!(descs[0].field, "k");
}) }

#[test]
fn test_index_rebuild_ux_explicit_load() { with_env_lock(|| {
    let dir = tempdir().unwrap();
    let meta_path = dir.path().join("rebuild_ux_meta.json");
    unsafe { std::env::set_var("NEXUS_INDEX_META", meta_path.to_string_lossy().to_string()); }
    let _ = fs::remove_file(&meta_path);

    // create engine and collection and index, save metadata
    let engine = Engine::new(dir.path().join("wal_rebuild_ux.bin")).unwrap();
    let col = engine.create_collection("recol".into());
    col.create_index("k", IndexKind::Hash);
    engine.save_indexes_metadata().unwrap();

    // New engine auto-loads metadata; explicit call remains idempotent UX
    let engine2 = Engine::new(dir.path().join("wal_rebuild_ux.bin")).unwrap();
    let col2 = engine2.get_collection("recol").expect("recol present after auto-load");
    // user triggers explicit rebuild UX (no-op)
    engine2.load_indexes_metadata();
    let descs = col2.indexes.read().descriptors();
    assert_eq!(descs.len(), 1);
    assert_eq!(descs[0].field, "k");
}) }

#[test]
fn test_index_metadata_version_bump() { with_env_lock(|| {
    let dir = tempdir().unwrap();
    let meta_path = dir.path().join("nexus_indexes_version.json");
    unsafe { std::env::set_var("NEXUS_INDEX_META", meta_path.to_string_lossy().to_string()); }
    let _ = fs::remove_file(&meta_path);
    // Write an older version file
    let legacy = serde_json::json!({
        "version": 0,
        "collections": { "vcol": [ { "field": "k", "kind": "Hash" } ] }
    });
    fs::write(&meta_path, serde_json::to_vec_pretty(&legacy).unwrap()).unwrap();
    // Engine should load and rewrite to current version
    let engine = Engine::new(dir.path().join("wal_meta_ver.bin")).unwrap();
    let col = engine.get_collection("vcol").unwrap();
    let descs = col.indexes.read().descriptors();
    assert_eq!(descs.len(), 1);
    // File should now have current version
    let updated: serde_json::Value = serde_json::from_slice(&fs::read(&meta_path).unwrap()).unwrap();
    assert_eq!(updated["version"].as_u64().unwrap() as u32, nexus_lite::index::INDEX_METADATA_VERSION);
}) }

#[test]
fn test_index_build_mode_blocks_writes() {
    let dir = tempdir().unwrap();
    let engine = Engine::new(dir.path().join("wal_block.bin")).unwrap();
    let col = engine.create_collection("block_col".into());
    // Spawn a thread that holds write lock by building an index on many docs
    for i in 0..500i32 { col.insert_document(nexus_lite::document::Document::new(doc!{"k": i}, nexus_lite::document::DocumentType::Persistent)); }
    // Start index build in a thread (will take some time iterating cache)
    let col_clone = col.clone();
    let h = std::thread::spawn(move || { col_clone.create_index("k", IndexKind::BTree); });
    // Attempt writes while build lock held; they will block until build completes; just ensure no panic
    let d = nexus_lite::document::Document::new(doc!{"k": -1}, nexus_lite::document::DocumentType::Persistent);
    let _ = col.insert_document(d);
    let _ = h.join();
}

#[test]
fn test_index_wasp_overlay_compensates_index_miss() { with_env_lock(|| {
    // Use WASP-backed engine to ensure index deltas are persisted alongside ops
    let dir = tempdir().unwrap();
    let wasp_path = dir.path().join("wasp_overlay.bin");
    let engine = Engine::with_wasp(wasp_path).expect("WASP engine should initialize");
    let col = engine.create_collection("overlay_col".into());
    col.create_index("k", IndexKind::Hash);

    // Insert a document; this appends op + emits an Add delta and updates cache+indexes
    let doc = nexus_lite::document::Document::new(doc!{"k": 42, "v": "x"}, nexus_lite::document::DocumentType::Persistent);
    let id = doc.id.clone();
    col.insert_document(doc.clone());

    // Simulate an out-of-sync base index (e.g., crash window) by manually removing the entry
    {
        let mut mgr = col.indexes.write();
        index_remove_all(&mut mgr, &doc.data.0, &id);
    }

    // A query on k==42 should still find the doc because the planner merges WASP overlay deltas
    let filter = Filter::Cmp { path: "k".into(), op: CmpOp::Eq, value: 42.into() };
    let cur = query::find_docs(&&col, &filter, &FindOptions::default());
    let docs = cur.to_vec();
    assert_eq!(docs.len(), 1, "overlay should compensate for base index miss");
    assert_eq!(docs[0].id, id);
}) }

#[test]
fn test_index_wasp_overlay_persists_across_restart() { with_env_lock(|| {
    let dir = tempdir().unwrap();
    let wasp_path = dir.path().join("wasp_overlay_restart.bin");

    // First instance: write an insert and delta
    let engine = Engine::with_wasp(wasp_path.clone()).expect("WASP engine should initialize");
    let col_name = "overlay_restart_col";
    let col = engine.create_collection(col_name.into());
    col.create_index("k", IndexKind::Hash);
    let doc = nexus_lite::document::Document::new(doc!{"k": 7, "v": "y"}, nexus_lite::document::DocumentType::Persistent);
    let id = doc.id.clone();
    col.insert_document(doc);

    // Simulate restart by creating a fresh engine with the same WASP path
    let engine2 = Engine::with_wasp(wasp_path.clone()).expect("WASP engine should re-open");
    let col2 = engine2.create_collection(col_name.into());

    // Deltas should persist and be readable after restart
    let deltas = col2.index_deltas();
    assert!(deltas.iter().any(|d| d.collection == col_name && d.field == "k" && match d.op { nexus_lite::wasp::DeltaOp::Add => true, _ => false } && d.id == id),
        "expected Add delta for id to persist across restart");
}) }

#[test]
fn test_index_wasp_overlay_across_restart_compensates_index_miss() { with_env_lock(|| {
    let dir = tempdir().unwrap();
    let wasp_path = dir.path().join("wasp_overlay_restart2.bin");

    // First instance: insert one document and emit delta
    let engine = Engine::with_wasp(wasp_path.clone()).expect("WASP engine should initialize");
    let col_name = "overlay_restart_col2";
    let col = engine.create_collection(col_name.into());
    col.create_index("k", IndexKind::Hash);
    let doc = nexus_lite::document::Document::new(doc!{"k": 42, "v": "x"}, nexus_lite::document::DocumentType::Persistent);
    let id = doc.id.clone();
    let data = doc.data.0.clone();
    col.insert_document(doc.clone());

    // Restart: new engine, same WASP path, empty cache
    let engine2 = Engine::with_wasp(wasp_path.clone()).expect("WASP engine should re-open");
    let col2 = engine2.create_collection(col_name.into());
    // Do not rebuild base index to simulate out-of-sync, but rehydrate cache with the same id
    let mut doc2 = nexus_lite::document::Document::new(data.clone(), nexus_lite::document::DocumentType::Persistent);
    doc2.id = id.clone();
    col2.cache.insert(doc2);

    // Planner should use overlay deltas to return this id even if the base index is missing/stale
    let filter = Filter::Cmp { path: "k".into(), op: CmpOp::Eq, value: 42.into() };
    let cur = query::find_docs(&&col2, &filter, &FindOptions::default());
    let docs = cur.to_vec();
    assert_eq!(docs.len(), 1, "overlay should still guide query after restart");
    assert_eq!(docs[0].id, id);
}) }
