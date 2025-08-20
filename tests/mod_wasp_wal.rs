use nexus_lite::wasp::{CowTree, WaspFile, Manifest, TinyWal, WalRecord};
use tempfile::tempdir;

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
