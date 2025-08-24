// Re-exported from former concurrency suite; kept as-is.
use std::sync::Arc;
use std::thread;

#[test]
fn concurrent_insert_read_update_stress() {
    use nexus_lite::document::{Document, DocumentType};
    use nexus_lite::engine::Engine;

    let engine = Arc::new(Engine::new(std::env::temp_dir().join("concurrency.wasp")).unwrap());
    let col = engine.create_collection("conc".into());

    let threads = 4;
    let per_thread = 50;

    let mut handles = Vec::new();
    for t in 0..threads {
        let col = col.clone();
        let handle = thread::spawn(move || {
            for i in 0..per_thread {
                let val = (t as i64) * 1_000_000 + i as i64;
                let d = Document::new(bson::doc! {"k": val, "t": t}, DocumentType::Persistent);
                let id = d.id.clone();
                col.insert_document(d);
                let fetched = col.find_document(&id).expect("doc must exist");
                assert_eq!(fetched.data.0.get_i64("k").unwrap(), val);
                if i % 10 == 0 {
                    let mut upd = fetched.clone();
                    upd.data.0.insert("updated".to_string(), bson::Bson::Boolean(true));
                    col.update_document(&id, upd);
                    let fetched2 = col.find_document(&id).unwrap();
                    assert!(fetched2.data.0.get_bool("updated").unwrap());
                }
            }
        });
        handles.push(handle);
    }

    for h in handles { h.join().unwrap(); }

    let total = threads * per_thread;
    let all = col.get_all_documents();
    assert_eq!(all.len(), total as usize);
}
