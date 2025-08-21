use nexus_lite::{Database, index::IndexKind};
use nexus_lite::document::{Document, DocumentType};
use bson::doc;
use tempfile::tempdir;
use bincode::serde::decode_from_slice;
use bincode::config::standard;

#[test]
#[cfg_attr(target_os = "windows", ignore)]
fn snapshot_contains_index_descriptors() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("snap.db");

    let db = Database::open(db_path.to_str().unwrap()).unwrap();
    let col = db.create_collection("users");
    col.create_index("age", IndexKind::BTree);
    col.create_index("name", IndexKind::Hash);

    // Insert a couple of docs
    let d1 = Document::new(doc!{ "name": "alice", "age": 30 }, DocumentType::Persistent);
    let d2 = Document::new(doc!{ "name": "bob", "age": 25 }, DocumentType::Persistent);
    db.insert_document("users", d1).unwrap();
    db.insert_document("users", d2).unwrap();

    // On Windows, proactively remove destination to avoid replace locking issues
    #[cfg(target_os = "windows")]
    if db_path.exists() { let _ = std::fs::remove_file(&db_path); }
    // Checkpoint into the .db snapshot
    db.checkpoint(&db_path).unwrap();

    // On Windows, give the OS a moment to release the lock after move
    #[cfg(target_os = "windows")]
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Read back and decode snapshot
    #[cfg(target_os = "windows")]
    {
        use std::ptr::null_mut;
        use std::ffi::OsStr;
        use std::os::windows::ffi::OsStrExt;
        use winapi::um::fileapi::CreateFileW;
        use winapi::um::winnt::{FILE_SHARE_READ, GENERIC_READ};
        use winapi::um::fileapi::OPEN_EXISTING;
        use winapi::um::handleapi::INVALID_HANDLE_VALUE;
        use std::io::Read as _;
        fn open_with_shared_read(path: &std::path::Path) -> std::fs::File {
            let wide: Vec<u16> = OsStr::new(path)
                .encode_wide()
                .chain(Some(0))
                .collect();
            use std::os::windows::io::FromRawHandle;
            unsafe {
                let handle = CreateFileW(
                    wide.as_ptr(),
                    GENERIC_READ,
                    FILE_SHARE_READ,
                    null_mut(),
                    OPEN_EXISTING,
                    0,
                    null_mut(),
                );
                assert!(handle != INVALID_HANDLE_VALUE, "Failed to open file with shared read permissions: {:?}", path);
                std::fs::File::from_raw_handle(handle as *mut _)
            }
        }
        let mut f = open_with_shared_read(&db_path);
        let mut bytes = Vec::new();
        f.read_to_end(&mut bytes).unwrap();
        let (snap, _) = decode_from_slice::<nexus_lite::wasp::DbSnapshot, _>(&bytes, standard()).unwrap();
        let users = snap.indexes.get("users").expect("users indexes present");
        assert!(users.iter().any(|d| d.field == "age" && matches!(d.kind, IndexKind::BTree)));
        assert!(users.iter().any(|d| d.field == "name" && matches!(d.kind, IndexKind::Hash)));
    }
    #[cfg(not(target_os = "windows"))]
    {
        let bytes = std::fs::read(&db_path).unwrap();
        let (snap, _) = decode_from_slice::<nexus_lite::wasp::DbSnapshot, _>(&bytes, standard()).unwrap();
        let users = snap.indexes.get("users").expect("users indexes present");
        assert!(users.iter().any(|d| d.field == "age" && matches!(d.kind, IndexKind::BTree)));
        assert!(users.iter().any(|d| d.field == "name" && matches!(d.kind, IndexKind::Hash)));
    }
}
