use crate::errors::DbError;
use crate::types::{CollectionName, DocumentId};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::time::SystemTime;

/// WAL operation kinds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OpKind {
    Upsert,
    Delete,
    CreateCol,
}

/// One WAL record, bincode-encoded.
/// `value_json` is raw JSON bytes (serde_json), to remain format-agnostic in the future.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecord {
    pub op: OpKind,
    pub collection: Option<CollectionName>,
    pub id: Option<DocumentId>,
    pub value_json: Option<Vec<u8>>,
    pub expires_at: Option<SystemTime>,
    pub ts: SystemTime,
}

pub fn write_record<W: Write>(writer: &mut W, rec: &WalRecord) -> Result<(), DbError> {
    let bytes = bincode::serialize(rec)?;
    let len = bytes.len() as u32;
    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(&bytes)?;
    Ok(())
}

pub fn read_record<R: Read>(reader: &mut R) -> Result<Option<WalRecord>, DbError> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(DbError::Io(e)),
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    let rec: WalRecord = bincode::deserialize(&buf)?;
    Ok(Some(rec))
}
