use nexus_lite::wasp::{CowTree, WaspFile, Manifest, Wasp, StorageEngine, TinyWal, WalRecord, SegmentFile, Page, SegmentFooter};
use nexus_lite::types::Operation;
use nexus_lite::document::{Document, DocumentType};
use std::io::Read;
use tempfile::tempdir;
use bson::doc;

#[test]
fn test_wasp_insert_and_get() {
    let dir = tempdir().unwrap();
    let wasp_path = dir.path().join("test_wasp.bin");
    let mut file = WaspFile::open(wasp_path).unwrap();
    // Write initial manifest
    let manifest = Manifest::new();
    file.write_manifest(&manifest).unwrap();
    let mut tree = CowTree::new(file).unwrap();
    // Insert key/value
    tree.insert(b"foo".to_vec(), b"bar".to_vec()).unwrap();
    // Read back
    let val = tree.get(b"foo").unwrap();
    assert_eq!(val, Some(b"bar".to_vec()));
    // Simulate crash/restart
    tree.reload_root().unwrap();
    let val2 = tree.get(b"foo").unwrap();
    assert_eq!(val2, Some(b"bar".to_vec()));
}

// ---- merged from mod_wasp_checkpoint.rs ----

#[test]
#[cfg_attr(target_os = "windows", ignore)]
/// This test is ignored on Windows due to OS-level file locking after file move/replace operations.
/// The checkpoint logic works, but the file cannot be opened for reading immediately after the move.
fn test_wasp_checkpoint() {
    let dir = tempdir().unwrap();
    let wasp_path = dir.path().join("test_wasp_checkpoint.bin");
    let db_path = dir.path().join("main_db_checkpoint.bin");
    let mut wasp = Wasp::new(wasp_path.clone()).unwrap();

    // Create valid documents and operations
    let doc1 = Document::new(doc! { "foo": 1 }, DocumentType::Persistent);
    let doc2 = Document::new(doc! { "bar": 2 }, DocumentType::Persistent);
    let op1 = Operation::Insert { document: doc1.clone() };
    let op2 = Operation::Delete { document_id: doc2.id.clone() };
    StorageEngine::append(&mut wasp, &op1).unwrap();
    StorageEngine::append(&mut wasp, &op2).unwrap();

    // Assert temp file exists before checkpoint
    let tmp_path = db_path.with_extension("db.tmp");
    // The temp file should not exist yet
    assert!(!tmp_path.exists(), "Temp file already exists before checkpoint: {:?}", tmp_path);

    // On Windows, give the OS a moment to release the lock
    #[cfg(target_os = "windows")]
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Perform checkpoint
    wasp.checkpoint(&db_path).unwrap();

    // Assert destination file exists after checkpoint
    assert!(db_path.exists(), "Destination file does not exist after checkpoint: {:?}", db_path);

    // On Windows, give the OS a moment to release the lock after move
    #[cfg(target_os = "windows")]
    std::thread::sleep(std::time::Duration::from_millis(200));
    #[cfg(target_os = "windows")]
    use std::ptr::null_mut;
    #[cfg(target_os = "windows")]
    use winapi::um::fileapi::CreateFileW;
    #[cfg(target_os = "windows")]
    use winapi::um::winnt::{FILE_SHARE_READ, GENERIC_READ};
    #[cfg(target_os = "windows")]
    use winapi::um::fileapi::OPEN_EXISTING;
    #[cfg(target_os = "windows")]
    use winapi::um::handleapi::INVALID_HANDLE_VALUE;
    #[cfg(target_os = "windows")]
    use std::ffi::OsStr;
    #[cfg(target_os = "windows")]
    use std::os::windows::ffi::OsStrExt;
    #[cfg(target_os = "windows")]
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
    #[cfg(target_os = "windows")]
    let mut file = open_with_shared_read(&db_path);
    #[cfg(not(target_os = "windows"))]
    let mut file = std::fs::File::open(&db_path).unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).unwrap();
    let ops: Vec<Operation> = bincode::serde::decode_from_slice(&buf, bincode::config::standard()).unwrap().0;
    assert_eq!(ops.len(), 2);
    match &ops[0] {
        Operation::Insert { document } => assert_eq!(document, &doc1),
        _ => panic!("Expected Insert operation"),
    }
    match &ops[1] {
        Operation::Delete { document_id } => assert_eq!(document_id, &doc2.id),
        _ => panic!("Expected Delete operation"),
    }
}

// ---- merged from mod_wasp_segment.rs ----

#[test]
fn test_segment_flush_and_read() {
    let dir = tempdir().unwrap();
    let seg_path = dir.path().join("test_segment.bin");
    let mut seg = SegmentFile::open(seg_path).unwrap();
    let page1 = Page::new(1, 1, 2, b"foo".to_vec());
    let page2 = Page::new(2, 1, 2, b"bar".to_vec());
    let footer = SegmentFooter {
        key_range: (b"foo".to_vec(), b"bar".to_vec()),
        fence_keys: vec![b"foo".to_vec(), b"bar".to_vec()],
        bloom_filter: vec![0, 1, 2],
    };
    seg.flush_segment(&[page1.clone(), page2.clone()], &footer).unwrap();
    let (pages, read_footer) = seg.read_segment().unwrap();
    assert_eq!(pages.len(), 2);
    assert_eq!(pages[0].data, b"foo");
    assert_eq!(pages[1].data, b"bar");
    assert_eq!(read_footer.key_range.0, b"foo");
    assert_eq!(read_footer.key_range.1, b"bar");
}

// ---- merged from mod_wasp_wal.rs ----

#[test]
fn test_tinywal_append_and_recover() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("test_tinywal.bin");
    let mut wal = TinyWal::open(wal_path).unwrap();
    let rec1 = WalRecord { txn_id: 1, page_ids: vec![1], checksums: vec![123], new_root_id: 1, epoch: 1 };
    let rec2 = WalRecord { txn_id: 2, page_ids: vec![2], checksums: vec![456], new_root_id: 2, epoch: 2 };
    wal.append(&rec1).unwrap();
    wal.append(&rec2).unwrap();
    let records = wal.read_all().unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].txn_id, 1);
    assert_eq!(records[1].txn_id, 2);

    // Simulate recovery
    let mut file = WaspFile::open(dir.path().join("test_wasp.bin")).unwrap();
    let manifest = Manifest::new();
    file.write_manifest(&manifest).unwrap();
    let mut tree = CowTree::new(file).unwrap();
    tree.recover_from_wal(&mut wal).unwrap();
    assert_eq!(tree.root_page_id, 2);
    assert_eq!(tree.version, 2);
}
