use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

/// Normalize a database path: ensure .db extension and make absolute.
#[must_use]
pub fn normalize_db_path(name_or_path: Option<&str>) -> PathBuf {
    let raw = match name_or_path { Some(s) if !s.trim().is_empty() => PathBuf::from(s), _ => PathBuf::from("nexuslite") };
    let pb = if raw.extension().is_none() { let mut p = raw; p.set_extension("db"); p } else { raw };
    if pb.is_absolute() { pb } else { std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")).join(pb) }
}

/// Create a file with restrictive permissions where supported.
///
/// On Unix, this maps to 0o600. On Windows, the default inherits ACLs; we just avoid world-writable flags.
///
/// # Errors
/// Returns an error if the file cannot be created/opened.
pub fn create_secure(path: &Path) -> io::Result<File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        OpenOptions::new().write(true).create(true).truncate(false).mode(0o600).open(path)
    }
    #[cfg(not(unix))]
    {
        OpenOptions::new().write(true).create(true).truncate(false).open(path)
    }
}

/// Open a file for read/write without truncation.
///
/// # Errors
/// Returns an error if the file cannot be opened.
pub fn open_rw_no_trunc(path: &Path) -> io::Result<File> {
    OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)
}
