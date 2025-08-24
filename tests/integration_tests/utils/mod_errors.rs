use nexus_lite::errors::DbError;
use std::io;

#[test]
fn test_io_error_display() {
	let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
	let db_err = DbError::Io(io_err.to_string());
	assert_eq!(format!("{db_err}"), "I/O error: file not found");
}

#[test]
fn test_no_such_collection_error_display() {
	let db_err = DbError::NoSuchCollection("my_collection".to_string());
	assert_eq!(format!("{db_err}"), "Collection not found: my_collection");
}

#[test]
fn test_collection_already_exists_error_display() {
	let db_err = DbError::CollectionAlreadyExists("existing_collection".to_string());
	assert_eq!(format!("{db_err}"), "Collection already exists: existing_collection");
}

#[test]
fn test_no_such_document_error_display() {
	let db_err = DbError::NoSuchDocument("doc_id_123".to_string());
	assert_eq!(format!("{db_err}"), "Document not found: doc_id_123");
}

#[test]
fn test_invalid_document_id_error_display() {
	let db_err = DbError::InvalidDocumentId("invalid_id".to_string());
	assert_eq!(format!("{db_err}"), "Invalid document ID: invalid_id");
}

#[test]
fn test_wal_error_display() {
	// Legacy variant still present for backward compatibility
	let db_err = DbError::WalError("WAL operation failed".to_string());
	assert_eq!(format!("{db_err}"), "WAL error: WAL operation failed");
	let db_err = DbError::CacheError("cache operation failed".to_string());
	assert!(matches!(db_err, DbError::CacheError(_)));
}

#[test]
fn test_cache_error_display() {
	let db_err = DbError::CacheError("Cache full".to_string());
	assert_eq!(format!("{db_err}"), "Cache error: Cache full");
}

#[test]
fn test_encryption_error_display() {
	let db_err = DbError::EncryptionError("Encryption failed".to_string());
	assert_eq!(format!("{db_err}"), "Encryption error: Encryption failed");
}

#[test]
fn test_decryption_error_display() {
	let db_err = DbError::DecryptionError("Decryption failed".to_string());
	assert_eq!(format!("{db_err}"), "Decryption error: Decryption failed");
}

#[test]
fn test_signature_verification_error_display() {
	let db_err = DbError::SignatureVerificationError("Signature mismatch".to_string());
	assert_eq!(format!("{db_err}"), "Signature verification error: Signature mismatch");
}
