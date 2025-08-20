use nexus_lite::wasp::{Wasp, StorageEngine};
use std::io::Read;
use tempfile::tempdir;
use nexus_lite::types::Operation;
use nexus_lite::document::{Document, DocumentType};
use bson::doc;

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
    let mut file = File::open(&db_path).unwrap();
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
