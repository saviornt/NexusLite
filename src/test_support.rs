#![cfg(test)]

// Tiny test-only helpers for temp paths
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

/// Create a unique temp file path with the given stem and extension in the OS temp dir.
pub fn temp_path(stem: &str, ext: &str) -> PathBuf {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    let mut p = std::env::temp_dir();
    p.push(format!("{}_{}.{}", stem, now, ext));
    p
}

/// Convenience for a WASP file path in temp.
pub fn temp_wasp(stem: &str) -> PathBuf {
    temp_path(stem, "wasp")
}

/// Create a unique, empty temporary directory under the OS temp dir.
/// If the directory exists, it is removed first.
pub fn temp_dir(stem: &str) -> PathBuf {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    let mut p = std::env::temp_dir();
    p.push(format!("{}_{}", stem, now));
    if p.exists() {
        let _ = fs::remove_dir_all(&p);
    }
    fs::create_dir_all(&p).expect("create temp_dir failed");
    p
}

/// Convenience alias for DB folder creation.
pub fn temp_db_dir(stem: &str) -> PathBuf {
    temp_dir(stem)
}

/// Join a file path inside a directory (does not create the file).
pub fn temp_file_in(dir: &Path, name: &str) -> PathBuf {
    dir.join(name)
}
