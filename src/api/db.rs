//! Database-level APIs: open/close/create/rename and collection listing.

use crate::errors::DbError;
use std::path::Path;

pub fn db_open<P: AsRef<Path>>(db_path: P) -> Result<crate::Database, DbError> {
    // Detect PBE-encrypted .db/.wasp and require credentials via env (non-interactive)
    let pb = db_path.as_ref().to_path_buf();
    let wasp = pb.with_extension("wasp");
    let pbe_db = crate::crypto::pbe_is_encrypted(&pb);
    let pbe_wasp = wasp.exists() && crate::crypto::pbe_is_encrypted(&wasp);
    if pbe_db || pbe_wasp {
        let username = std::env::var("NEXUSLITE_USERNAME").unwrap_or_default();
        let password = std::env::var("NEXUSLITE_PASSWORD").unwrap_or_default();
        if username.is_empty() || password.is_empty() {
            return Err(DbError::Io(
                "PBE-encrypted DB: set NEXUSLITE_USERNAME and NEXUSLITE_PASSWORD".into(),
            ));
        }
        super::crypto::decrypt_db_with_password(pb.as_path(), &username, &password)?;
    }
    let pstr = pb.to_string_lossy().to_string();
    crate::Database::open(&pstr).map_err(|e| DbError::Io(e.to_string()))
}

pub fn db_new(db_path: Option<&str>) -> Result<crate::Database, DbError> {
    crate::Database::new(db_path).map_err(|e| DbError::Io(e.to_string()))
}

pub fn db_close(db_path: Option<&str>) -> Result<(), DbError> {
    crate::Database::close(db_path)
}

pub fn db_create_collection(db: &crate::Database, name: &str) {
    let _ = db.create_collection(name);
}

pub fn db_list_collections(db: &crate::Database) -> Vec<String> {
    db.list_collection_names()
}

pub fn db_delete_collection(db: &crate::Database, name: &str) -> bool {
    db.delete_collection(name)
}

pub fn db_rename_collection(db: &crate::Database, old: &str, new: &str) -> Result<(), DbError> {
    db.rename_collection(old, new)
}
