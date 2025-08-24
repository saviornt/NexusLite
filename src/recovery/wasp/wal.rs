use std::fs::OpenOptions;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use bincode::config::standard;
use bincode::serde::{decode_from_slice, encode_to_vec};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecord {
    pub txn_id: u64,
    pub page_ids: Vec<u64>,
    pub checksums: Vec<u32>,
    pub new_root_id: u64,
    pub epoch: u64,
}

pub struct TinyWal { pub file: File }

impl TinyWal {
    pub fn sync(&mut self) -> io::Result<()> { self.file.sync_data() }
    pub fn open(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        Ok(Self { file })
    }
    pub fn append(&mut self, record: &WalRecord) -> io::Result<()> {
        let data = encode_to_vec(record, standard()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let len = data.len() as u64;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&data)?;
        self.file.sync_data()?;
        Ok(())
    }
    pub fn read_all(&mut self) -> io::Result<Vec<WalRecord>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut records = Vec::new();
        let mut buf = Vec::new();
        self.file.read_to_end(&mut buf)?;
        let mut offset = 0;
        while offset + 8 <= buf.len() {
            let len = match <&[u8; 8]>::try_from(&buf[offset..offset + 8]) { Ok(arr) => u64::from_le_bytes(*arr) as usize, Err(_) => break };
            offset += 8;
            if offset + len > buf.len() { break; }
            let (rec, _) = decode_from_slice::<WalRecord, _>(&buf[offset..offset + len], standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            records.push(rec);
            offset += len;
        }
        Ok(records)
    }
}
