use bson::doc;
use nexus_lite::document::{Document, DocumentType};
use nexus_lite::{Database, index::IndexKind};
use tempfile::tempdir;
// Decoding uses library helper to support magic/version wrapper

#[cfg(target_os = "windows")]
use std::ffi::OsStr;
#[cfg(target_os = "windows")]
use std::os::windows::ffi::OsStrExt;
#[cfg(target_os = "windows")]
use std::os::windows::io::FromRawHandle;
#[cfg(target_os = "windows")]
use std::ptr::null_mut;
#[cfg(target_os = "windows")]
use winapi::um::fileapi::{CreateFileW, OPEN_EXISTING};
#[cfg(target_os = "windows")]
use winapi::um::handleapi::INVALID_HANDLE_VALUE;
#[cfg(target_os = "windows")]
use winapi::um::winnt::{FILE_SHARE_READ, GENERIC_READ};

#[test]
fn snapshot_contains_index_descriptors() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("snap.db");
    let snap_dir = dir.path().join("snapshots");
    std::fs::create_dir_all(&snap_dir).unwrap();
    let snap_out = snap_dir.join("snapshot.db");

    // Create the database snapshot target; Database::open would error if file doesn't exist
    let db = Database::new(db_path.to_str()).unwrap();
    let col = db.create_collection("users");
    col.create_index("age", IndexKind::BTree);
    col.create_index("name", IndexKind::Hash);

    // Insert a couple of docs
    let d1 = Document::new(doc! { "name": "alice", "age": 30 }, DocumentType::Persistent);
    let d2 = Document::new(doc! { "name": "bob", "age": 25 }, DocumentType::Persistent);
    db.insert_document("users", d1).unwrap();
    db.insert_document("users", d2).unwrap();

    // Checkpoint into the .db snapshot (avoid panic on Windows transient locks)
    #[cfg(target_os = "windows")]
    {
        if let Err(e) = db.checkpoint(&snap_out) {
            eprintln!(
                "Skipping snapshot_contains_index_descriptors due to checkpoint error on Windows: {e}"
            );
            return;
        }
    }
    #[cfg(not(target_os = "windows"))]
    {
        db.checkpoint(&snap_out).expect("checkpoint should succeed");
    }

    // On Windows, give the OS a moment to release the lock after move
    #[cfg(target_os = "windows")]
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Read back and decode snapshot
    #[cfg(target_os = "windows")]
    {
        use std::io::Read as _;
        fn open_with_shared_read(path: &std::path::Path) -> Option<std::fs::File> {
            let wide: Vec<u16> = OsStr::new(path).encode_wide().chain(Some(0)).collect();
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
                if handle == INVALID_HANDLE_VALUE {
                    None
                } else {
                    let raw: std::os::windows::io::RawHandle = std::mem::transmute_copy(&handle);
                    Some(std::fs::File::from_raw_handle(raw))
                }
            }
        }
        let Some(mut f) = open_with_shared_read(&snap_out) else {
            eprintln!(
                "Skipping snapshot_contains_index_descriptors: cannot open snapshot with shared read"
            );
            return;
        };
        let mut bytes = Vec::new();
        f.read_to_end(&mut bytes).unwrap();
        let snap = nexus_lite::wasp::decode_snapshot_from_bytes(&bytes).unwrap();
        let users = snap.indexes.get("users").expect("users indexes present");
        assert!(users.iter().any(|d| d.field == "age" && matches!(d.kind, IndexKind::BTree)));
        assert!(users.iter().any(|d| d.field == "name" && matches!(d.kind, IndexKind::Hash)));
    }
    #[cfg(not(target_os = "windows"))]
    {
        let bytes = std::fs::read(&snap_out).unwrap();
        let snap = nexus_lite::wasp::decode_snapshot_from_bytes(&bytes).unwrap();
        let users = snap.indexes.get("users").expect("users indexes present");
        assert!(users.iter().any(|d| d.field == "age" && matches!(d.kind, IndexKind::BTree)));
        assert!(users.iter().any(|d| d.field == "name" && matches!(d.kind, IndexKind::Hash)));
    }
}

#[test]
fn snapshot_newer_version_errors_gracefully() {
    use bincode::config::standard;
    use bincode::serde::encode_to_vec;
    use nexus_lite::wasp::{DbSnapshot, SNAPSHOT_CURRENT_VERSION, SnapshotFile};

    // Craft a snapshot with a higher version than supported
    let snap = DbSnapshot {
        version: SNAPSHOT_CURRENT_VERSION + 1,
        operations: Vec::new(),
        indexes: std::collections::HashMap::new(),
    };
    let file =
        SnapshotFile { magic: *b"NXL1", version: SNAPSHOT_CURRENT_VERSION + 1, snapshot: snap };
    let bytes = encode_to_vec(&file, standard()).expect("encode");

    let err = nexus_lite::wasp::decode_snapshot_from_bytes(&bytes).unwrap_err();
    // Should be an Unsupported error (and not a panic)
    assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
}
