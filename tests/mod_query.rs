use bson::doc;
use nexus_lite::document::{Document, DocumentType};
use nexus_lite::query::{find_docs, count_docs, Filter, CmpOp, FindOptions, Order, SortSpec, UpdateDoc, apply_update};

#[test]
fn filter_eq_and_count() {
    let d1 = Document::new(doc!{"age": 30, "name": "alice"}, DocumentType::Persistent);
    let _d2 = Document::new(doc!{"age": 40, "name": "bob"}, DocumentType::Persistent);
    assert!(nexus_lite::query::eval_filter(&d1.data.0, &Filter::Cmp { path: "age".into(), op: CmpOp::Eq, value: 30.into() }));
    assert!(!nexus_lite::query::eval_filter(&d1.data.0, &Filter::Cmp { path: "age".into(), op: CmpOp::Gt, value: 45.into() }));
}

#[test]
fn update_set_inc_unset() {
    let mut d = Document::new(doc!{"age": 30, "info": {"visits": 1}}, DocumentType::Persistent);
    let upd = UpdateDoc {
        set: vec![("name".into(), "alice".into())],
        inc: vec![("age".into(), 1.0), ("info.visits".into(), 2.0)],
        unset: vec!["unused".into()],
    };
    let changed = apply_update(&mut d, &upd);
    assert!(changed);
    assert_eq!(d.data.0.get_str("name").unwrap(), "alice");
    assert_eq!(d.data.0.get_f64("age").unwrap(), 31.0);
    assert_eq!(d.data.0.get_document("info").unwrap().get_f64("visits").unwrap(), 3.0);
}

#[test]
fn update_inc_int64_and_cmp_lt_not() {
    // Int64 increment should be handled and result stored as Double
    let mut d = Document::new(doc!{"age": bson::Bson::Int64(30)}, DocumentType::Persistent);
    let upd = UpdateDoc { set: vec![], inc: vec![("age".into(), 2.0)], unset: vec![] };
    let changed = apply_update(&mut d, &upd);
    assert!(changed);
    assert_eq!(d.data.0.get_f64("age").unwrap(), 32.0);

    // Lt branch should work, and Not should invert
    let f_lt = Filter::Cmp { path: "age".into(), op: CmpOp::Lt, value: 40.into() };
    assert!(nexus_lite::query::eval_filter(&d.data.0, &f_lt));
    let f_not = Filter::Not(Box::new(f_lt));
    assert!(!nexus_lite::query::eval_filter(&d.data.0, &f_not));
}

#[test]
fn find_sort_project_paginate() {
    use nexus_lite::engine::Engine;
    use tempfile::tempdir;
    let dir = tempdir().unwrap();
    let db_name = "qtestdb";
    let wal_path = dir.path().join(format!("{db_name}_wasp_query.bin"));
    let engine = Engine::new(wal_path).unwrap();
    let col = engine.create_collection("qtest".into());
    col.insert_document(Document::new(doc!{"age": 30, "name": "alice"}, DocumentType::Persistent));
    col.insert_document(Document::new(doc!{"age": 40, "name": "bob"}, DocumentType::Persistent));
    col.insert_document(Document::new(doc!{"age": 35, "name": "carol"}, DocumentType::Persistent));

    let filter = Filter::Cmp { path: "age".into(), op: CmpOp::Gt, value: 30.into() };
    let opts = FindOptions { projection: Some(vec!["name".into()]), sort: Some(vec![SortSpec{ field: "age".into(), order: Order::Desc }]), limit: Some(2), skip: Some(0), timeout_ms: None };
    let cur = find_docs(&col, &filter, &opts);
    let docs = cur.to_vec();
    assert_eq!(docs.len(), 2);
    assert_eq!(docs[0].data.0.get_str("name").unwrap(), "bob");
    assert_eq!(docs[1].data.0.get_str("name").unwrap(), "carol");

    let cnt = count_docs(&col, &Filter::Cmp { path: "age".into(), op: CmpOp::Gt, value: 30.into() });
    assert_eq!(cnt, 2);
}

#[test]
fn test_query_find_redaction() {
    use nexus_lite::engine::Engine;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path().join("wal.log")).unwrap();
    let col = engine.create_collection("users".into());
    let id = col.insert_document(nexus_lite::document::Document::new(doc!{"user":"alice","password":"secret"}, nexus_lite::document::DocumentType::Persistent));
    assert!(col.find_document(&id).is_some());
    // Use programmatic CLI path to emit NDJSON with redaction
    let _ = nexus_lite::cli::run(&engine, nexus_lite::cli::Command::QueryFindR { collection: "users".into(), filter_json: "{}".into(), project: None, sort: None, limit: None, skip: None, redact_fields: Some(vec!["password".into()]) });
    // We don't capture stdout here, but ensure no panic and path compiles.
}
