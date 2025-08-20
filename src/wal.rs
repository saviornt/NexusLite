use crate::types::Operation;
use bincode::config::standard;
use bincode::serde::{encode_to_vec, decode_from_slice};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write, Seek};
use std::path::PathBuf;

pub struct Wal {
    file: File,
}

impl Wal {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        Ok(Wal { file })
    }

    pub fn append(&mut self, operation: &Operation) -> io::Result<()> {
        let encoded = encode_to_vec(operation, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.file.write_all(&(encoded.len() as u64).to_be_bytes())?;
        self.file.write_all(&encoded)?;
        self.file.flush()
    }

    pub fn read_all(&self) -> io::Result<Vec<Result<Operation, bincode::error::DecodeError>>> {
        let mut file = self.file.try_clone()?;
        file.seek(io::SeekFrom::Start(0))?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let mut operations = Vec::new();
        let mut offset = 0;

        while offset < buffer.len() {
            let len_bytes = &buffer[offset..offset + 8];
            let len = u64::from_be_bytes(len_bytes.try_into().unwrap());
            offset += 8;

            let encoded_op = &buffer[offset..offset + len as usize];
            let operation = decode_from_slice::<crate::types::Operation, _>(encoded_op, standard());
            operations.push(operation.map(|(op, _)| op));
            offset += len as usize;
        }

        Ok(operations)
    }
}