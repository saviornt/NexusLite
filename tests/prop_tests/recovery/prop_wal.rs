use proptest::prelude::*;
use tempfile::tempdir;
use std::io::{Seek, SeekFrom, Write};

proptest! {
    #![proptest_config(proptest::test_runner::Config {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions"))),
        cases: 32,
        .. proptest::test_runner::Config::default()
    })]
    #[test]
    fn prop_wal_random_fragments_dont_panic(
        // Generate a sequence of valid or random fragments to append to WAL
        txn_ids in proptest::collection::vec(0u64..1000, 0..10),
        junk_blocks in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..64), 0..5),
        tails in proptest::option::of(proptest::collection::vec(any::<u8>(), 0..8))
    ) {
        use nexuslite::wasp::{TinyWal, WalRecord};
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("prop_fragments.wal");
        let mut wal = TinyWal::open(wal_path.clone()).unwrap();
        // Append some valid records
        for id in txn_ids {
            let rec = WalRecord { txn_id: id, page_ids: vec![id], checksums: vec![id as u32], new_root_id: id, epoch: id };
            wal.append(&rec).unwrap();
        }
        // Append random junk blocks with or without fake 8-byte lengths
        {
            let mut f = std::fs::OpenOptions::new().read(true).write(true).open(&wal_path).unwrap();
            f.seek(SeekFrom::End(0)).unwrap();
            for block in junk_blocks {
                if block.len() >= 8 {
                    // Pretend it's a length header sometimes
                    let use_len = block[0] % 2 == 0;
                    if use_len {
                        // choose a length that may or may not fit remaining block
                        let val = u64::from_le_bytes([
                            block[0], block[1], block[2], block[3], block[4], block[5], block[6], block[7]
                        ]);
                        f.write_all(&val.to_le_bytes()).unwrap();
                        f.write_all(&block[8..]).unwrap();
                    } else {
                        f.write_all(&block).unwrap();
                    }
                } else {
                    f.write_all(&block).unwrap();
                }
            }
            if let Some(t) = tails { f.write_all(&t).unwrap(); }
            f.flush().unwrap();
        }
        // read_all should never panic; it either returns decoded records or an error on a malformed chunk
        let _ = wal.read_all();
    }
}

// Log assertions: verify that writing to targets creates log files with content
use std::sync::{Mutex, OnceLock};
static LOGGER_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[test]
fn dev6_thread_local_logging_is_captured() {
    let _guard = LOGGER_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
    let _sink = nexuslite::utils::devlog::enable_thread_sink();
    nexuslite::dev6!("hello app");
    nexuslite::dev6!("audit entry");
    nexuslite::dev6!("metrics entry");
    let msgs = nexuslite::utils::devlog::drain();
    assert!(msgs.iter().any(|m| m.contains("hello app")));
    assert!(msgs.iter().any(|m| m.contains("audit entry")));
    assert!(msgs.iter().any(|m| m.contains("metrics entry")));
}
