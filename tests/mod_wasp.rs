use nexus_lite::wasp::{CowTree, WaspFile, Manifest};
use tempfile::tempdir;

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
