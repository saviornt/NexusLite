use nexus_lite::types::Operation;
use bincode::config::standard;
use bincode::serde::{decode_from_slice, encode_to_vec};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::PathBuf;

pub struct Wal {
    file: File,
}

impl Wal {
    /// # Errors
    /// Returns an error if the WAL file cannot be opened.
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).read(true).open(path)?;
        Ok(Self { file })
    }

    /// # Errors
    /// Returns an error if the operation cannot be written to the WAL.
    pub fn append(&mut self, operation: &Operation) -> io::Result<()> {
        let encoded = encode_to_vec(operation, standard()).map_err(io::Error::other)?;
        self.file.write_all(&(encoded.len() as u64).to_be_bytes())?;
        self.file.write_all(&encoded)?;
        self.file.flush()
    }

    /// # Errors
    /// Returns an error if the WAL cannot be read.
    pub fn read_all(&self) -> io::Result<Vec<Result<Operation, bincode::error::DecodeError>>> {
        let mut file = self.file.try_clone()?;
        file.seek(io::SeekFrom::Start(0))?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let mut operations = Vec::new();
        let mut offset = 0;

        while offset + 8 <= buffer.len() {
            let len_bytes = &buffer[offset..offset + 8];
            let len = match <&[u8; 8]>::try_from(len_bytes) {
                Ok(arr) => u64::from_be_bytes(*arr),
                Err(_) => break,
            };
            offset += 8;
            if offset + usize::try_from(len).unwrap_or(usize::MAX) > buffer.len() {
                break;
            }
            let encoded_op = &buffer[offset..offset + usize::try_from(len).unwrap_or(0)];
            let operation = decode_from_slice::<nexus_lite::types::Operation, _>(encoded_op, standard());
            operations.push(operation.map(|(op, _)| op));
            offset += usize::try_from(len).unwrap_or(0);
        }

        Ok(operations)
    }
}
