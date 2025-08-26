use std::path::PathBuf;

#[test]
fn verify_manifests_on_empty_file_returns_report() {
    let dir = tempfile::tempdir().unwrap();
    let path: PathBuf = dir.path().join("empty.wasp");
    // create empty file
    std::fs::write(&path, &[]).unwrap();
    let rep = nexuslite::recovery::recover::verify_manifests(&path).unwrap();
    // On an empty file, both_valid is expected to be false
    assert!(!rep.both_valid);
}

#[test]
fn repair_manifests_on_empty_file_errors() {
    let dir = tempfile::tempdir().unwrap();
    let path: PathBuf = dir.path().join("empty2.wasp");
    std::fs::write(&path, &[]).unwrap();
    let err = nexuslite::recovery::recover::repair_manifests(&path).unwrap_err();
    assert!(
        err.kind() == std::io::ErrorKind::InvalidData
            || err.kind() == std::io::ErrorKind::UnexpectedEof
    );
}

#[test]
fn validate_resilience_runs() {
    let dir = tempfile::tempdir().unwrap();
    let path: PathBuf = dir.path().join("fuzz.wasp");
    // initialize file with WASP_PAGE_SIZE * 2 zeros to allow slot writes
    let size = (nexuslite::wasp::WASP_PAGE_SIZE as u64) * 2;
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f =
            std::fs::OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
        f.seek(SeekFrom::Start(size.saturating_sub(1))).unwrap();
        f.write_all(&[0u8]).unwrap();
    }
    let _ = nexuslite::recovery::recover::validate_resilience(&path).unwrap();
}
