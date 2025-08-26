use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use bincode::config::standard;
use bincode::serde::{decode_from_slice, encode_to_vec};

use crate::index::IndexDescriptor;

use super::snapshot::{DbSnapshot, SNAPSHOT_CURRENT_VERSION, encode_snapshot_file};
use super::types::{IndexDelta, WaspFrame};
use crate::types::Operation;

/// Pluggable storage interface for write-append logs used by the engine.
/// Implementations must be Send + Sync to be shared across threads.
#[allow(clippy::missing_errors_doc)]
pub trait StorageEngine: Send + Sync {
    fn append(&mut self, operation: &crate::types::Operation) -> io::Result<()>;
    fn read_all(
        &self,
    ) -> io::Result<Vec<Result<crate::types::Operation, bincode::error::DecodeError>>>;
    fn checkpoint_with_meta(
        &mut self,
        _db_path: &std::path::Path,
        _indexes: std::collections::HashMap<String, Vec<IndexDescriptor>>,
    ) -> io::Result<()> {
        Ok(())
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
    fn append_index_delta(&mut self, _delta: IndexDelta) -> io::Result<()> {
        Ok(())
    }
    fn read_index_deltas(&self) -> io::Result<Vec<IndexDelta>> {
        Ok(vec![])
    }
}

/// WASP: a buffered, hybrid crash-consistent storage engine.
/// Provides an append/read API similar to WAL but uses a single segment file with buffered writes.
pub struct Wasp {
    file: File,
}

impl Wasp {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).read(true).open(path)?;
        Ok(Self { file })
    }

    /// Legacy checkpoint: persist all operations into the main DB file as `Vec<Operation>`.
    /// This preserves older test expectations that decode the DB file directly as a list of operations.
    #[allow(clippy::missing_errors_doc)]
    pub fn checkpoint(&mut self, db_path: &std::path::Path) -> io::Result<()> {
        // Gather operations from the WASP file
        let ops_res = <Self as StorageEngine>::read_all(self)?;
        let ops: Vec<Operation> = ops_res.into_iter().filter_map(Result::ok).collect();
        let encoded = encode_to_vec(&ops, standard()).map_err(io::Error::other)?;
        #[cfg(target_os = "windows")]
        {
            let mut db_file =
                OpenOptions::new().create(true).write(true).truncate(true).open(db_path)?;
            db_file.write_all(&encoded)?;
            db_file.sync_data()?;
        }
        #[cfg(not(target_os = "windows"))]
        {
            let tmp_path = db_path.with_extension("db.tmp");
            let mut db_file =
                OpenOptions::new().create(true).write(true).truncate(true).open(&tmp_path)?;
            db_file.write_all(&encoded)?;
            db_file.sync_data()?;
            drop(db_file);
            if db_path.exists() {
                let _ = std::fs::remove_file(db_path);
            }
            std::fs::rename(&tmp_path, db_path)?;
        }
        Ok(())
    }

    /// Checkpoint with index metadata: write a `DbSnapshot` containing (optional) operations and index descriptors.
    #[allow(clippy::missing_errors_doc)]
    pub fn checkpoint_with_meta(
        &mut self,
        db_path: &std::path::Path,
        indexes: std::collections::HashMap<String, Vec<IndexDescriptor>>,
    ) -> io::Result<()> {
        let snapshot =
            DbSnapshot { version: SNAPSHOT_CURRENT_VERSION, operations: Vec::new(), indexes };
        let encoded = encode_snapshot_file(&snapshot)?;
        #[cfg(target_os = "windows")]
        {
            let mut db_file =
                OpenOptions::new().create(true).write(true).truncate(true).open(db_path)?;
            db_file.write_all(&encoded)?;
            db_file.sync_data()?;
        }
        #[cfg(not(target_os = "windows"))]
        {
            let tmp_path = db_path.with_extension("db.tmp");
            let mut db_file =
                OpenOptions::new().create(true).write(true).truncate(true).open(&tmp_path)?;
            db_file.write_all(&encoded)?;
            db_file.sync_data()?;
            drop(db_file);
            if db_path.exists() {
                let _ = std::fs::remove_file(db_path);
            }
            std::fs::rename(&tmp_path, db_path)?;
        }
        #[cfg(not(target_os = "windows"))]
        {
            self.file.set_len(0)?;
            self.file.sync_data()?;
        }
        Ok(())
    }
}

impl StorageEngine for Wasp {
    #[allow(clippy::missing_errors_doc)]
    fn append(&mut self, operation: &crate::types::Operation) -> io::Result<()> {
        let frame = WaspFrame::Op(operation.clone());
        let encoded = encode_to_vec(&frame, standard()).map_err(io::Error::other)?;
        self.file
            .write_all(&(crate::utils::num::usize_to_u64(encoded.len())).to_be_bytes())?;
        self.file.write_all(&encoded)?;
        self.file.flush()
    }

    #[allow(clippy::missing_errors_doc)]
    fn read_all(
        &self,
    ) -> io::Result<Vec<Result<crate::types::Operation, bincode::error::DecodeError>>> {
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(0))?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let mut operations = Vec::new();
        let mut offset = 0usize;
        while offset + 8 <= buffer.len() {
            let len_bytes = &buffer[offset..offset + 8];
            let len = match <&[u8; 8]>::try_from(len_bytes) {
                Ok(arr) => match crate::utils::num::u64_to_usize(u64::from_be_bytes(*arr)) {
                    Some(v) => v,
                    None => break,
                },
                Err(_) => break,
            };
            offset += 8;
            if offset.checked_add(len).is_none_or(|end| end > buffer.len()) {
                break;
            }
            let encoded_op = &buffer[offset..offset + len];
            let frame = decode_from_slice::<super::types::WaspFrame, _>(encoded_op, standard());
            match frame {
                Ok((super::types::WaspFrame::Op(op), _)) => operations.push(Ok(op)),
                Ok((_other, _)) => {}
                Err(e) => operations.push(Err(e)),
            }
            offset += len;
        }
        Ok(operations)
    }

    #[allow(clippy::missing_errors_doc)]
    fn checkpoint_with_meta(
        &mut self,
        db_path: &std::path::Path,
        indexes: std::collections::HashMap<String, Vec<IndexDescriptor>>,
    ) -> io::Result<()> {
        self.checkpoint_with_meta(db_path, indexes)
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    #[allow(clippy::missing_errors_doc)]
    fn append_index_delta(&mut self, delta: IndexDelta) -> io::Result<()> {
        let frame = WaspFrame::Idx(delta);
        let encoded = encode_to_vec(&frame, standard()).map_err(io::Error::other)?;
        self.file
            .write_all(&(crate::utils::num::usize_to_u64(encoded.len())).to_be_bytes())?;
        self.file.write_all(&encoded)?;
        self.file.flush()
    }

    #[allow(clippy::missing_errors_doc)]
    fn read_index_deltas(&self) -> io::Result<Vec<IndexDelta>> {
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(0))?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let mut out = Vec::new();
        let mut offset = 0usize;
        while offset + 8 <= buffer.len() {
            let len_bytes = &buffer[offset..offset + 8];
            let len = match <&[u8; 8]>::try_from(len_bytes) {
                Ok(arr) => match crate::utils::num::u64_to_usize(u64::from_be_bytes(*arr)) {
                    Some(v) => v,
                    None => break,
                },
                Err(_) => break,
            };
            offset += 8;
            if offset.checked_add(len).is_none_or(|end| end > buffer.len()) {
                break;
            }
            let encoded = &buffer[offset..offset + len];
            if let Ok((WaspFrame::Idx(d), _)) =
                decode_from_slice::<WaspFrame, _>(encoded, standard())
            {
                out.push(d);
            }
            offset += len;
        }
        Ok(out)
    }
}
