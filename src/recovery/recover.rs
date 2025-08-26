//! Recovery manager façade: high-level helpers for verification and repair flows

use std::fs::OpenOptions;
use std::io::{self, Seek, SeekFrom};
use std::path::Path;

/// Verify both manifest slots and report integrity.
/// # Errors
/// Returns an error if the file cannot be opened.
pub fn verify_manifests(path: &Path) -> io::Result<super::wasp::ConsistencyReport> {
    let mut f = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
    // Seek to start to be explicit
    let _ = f.seek(SeekFrom::Start(0));
    Ok(super::wasp::ConsistencyChecker::new().check_detailed(&mut f))
}

/// Attempt recovery by copying the newest valid manifest slot to the other slot.
/// # Errors
/// Returns an error if the file cannot be opened or I/O fails during recovery.
pub fn repair_manifests(path: &Path) -> io::Result<super::wasp::ConsistencyReport> {
    let mut f = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
    super::wasp::recover_manifests(&mut f)
}

/// Validate resilience by fuzzing potential corruption and ensuring the secondary slot remains valid.
/// # Errors
/// Returns an error if the file cannot be opened.
pub fn validate_resilience(path: &Path) -> io::Result<bool> {
    let mut f = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
    Ok(super::wasp::fuzz_test_corruption(&mut f))
}
