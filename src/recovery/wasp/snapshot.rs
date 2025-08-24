use std::io;

use bincode::config::standard;
use bincode::serde::{decode_from_slice, encode_to_vec};
use serde::{Deserialize, Serialize};

use crate::index::IndexDescriptor;
use crate::types::Operation;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbSnapshot {
    pub version: u32,
    pub operations: Vec<Operation>,
    pub indexes: std::collections::HashMap<String, Vec<IndexDescriptor>>,
}

// Snapshot file wrapper with magic + version for forward/backward compatibility
pub const SNAPSHOT_MAGIC: [u8; 4] = *b"NXL1";
pub const SNAPSHOT_CURRENT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotFile { pub magic: [u8; 4], pub version: u32, pub snapshot: DbSnapshot }

impl SnapshotFile { #[must_use] pub const fn new(snapshot: DbSnapshot) -> Self { Self { magic: SNAPSHOT_MAGIC, version: SNAPSHOT_CURRENT_VERSION, snapshot } } }

/// Encode a snapshot file with magic+version header.
pub fn encode_snapshot_file(snap: &DbSnapshot) -> io::Result<Vec<u8>> {
    let file = SnapshotFile::new(snap.clone());
    encode_to_vec(&file, standard()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Decode a snapshot from bytes, supporting both wrapped and legacy formats.
pub fn decode_snapshot_from_bytes(bytes: &[u8]) -> io::Result<DbSnapshot> {
    if bytes.len() < 4 || bytes[0..4] != SNAPSHOT_MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "missing or invalid snapshot magic"));
    }
    let (file, _) = decode_from_slice::<SnapshotFile, _>(bytes, standard())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if file.version > SNAPSHOT_CURRENT_VERSION {
        return Err(io::Error::new(io::ErrorKind::Unsupported, "snapshot format version is newer than this build supports"));
    }
    Ok(file.snapshot)
}
