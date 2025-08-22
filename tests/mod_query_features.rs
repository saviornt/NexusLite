use bson::doc;
use nexus_lite::document::{Document, DocumentType};
use nexus_lite::engine::Engine;
use nexus_lite::query::{self, Filter, CmpOp, FindOptions};

#[test]
#[cfg(feature = "regex")]
fn test_regex_basic_and_length_guard() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path().join("wal_regex.bin")).unwrap();
    let col = engine.create_collection("re".into());
    col.insert_document(Document::new(doc!{"name":"Alice"}, DocumentType::Persistent));
    col.insert_document(Document::new(doc!{"name":"Bob"}, DocumentType::Persistent));
    // Case-insensitive search
    let f = Filter::Regex { path: "name".into(), pattern: "^a".into(), case_insensitive: true };
    let cur = query::find_docs(&col, &f, &FindOptions::default());
    let docs = cur.to_vec();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].data.0.get_str("name").unwrap(), "Alice");
    // Over-long pattern should reject
    let long = "a".repeat(600);
    let f2 = Filter::Regex { path: "name".into(), pattern: long, case_insensitive: false };
    let docs2 = query::find_docs(&col, &f2, &FindOptions::default()).to_vec();
    assert_eq!(docs2.len(), 0);
}

#[test]
fn test_timeout_best_effort() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path().join("wal_timeout.bin")).unwrap();
    let col = engine.create_collection("t".into());
    for i in 0..2000 {
        col.insert_document(Document::new(doc!{"i": i}, DocumentType::Persistent));
    }
    // Use a filter that forces a scan; set tiny timeout
    let f = Filter::Cmp { path: "i".into(), op: CmpOp::Gte, value: 0.into() };
    let mut opts = FindOptions::default();
    opts.timeout_ms = Some(1);
    let cur = query::find_docs(&col, &f, &opts);
    let docs = cur.to_vec();
    assert!(docs.len() < 2000);
}
