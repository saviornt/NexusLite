use std::fs::OpenOptions;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use bincode::config::standard;
use bincode::serde::{decode_from_slice, encode_to_vec};
use serde::{Deserialize, Serialize};

use super::page::Page;
use super::types::BloomFilter;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentFooter {
    pub key_range: (Vec<u8>, Vec<u8>),
    pub fence_keys: Vec<Vec<u8>>,
    pub bloom_filter: Vec<u8>, // serialized BloomFilter
}

impl SegmentFooter {
    #[must_use]
    pub fn new(keys: &[Vec<u8>], key_range: (Vec<u8>, Vec<u8>), fence_keys: Vec<Vec<u8>>) -> Self {
        let mut bloom = BloomFilter::new(1024, 3);
        for k in keys { bloom.insert(k); }
        let bloom_bytes = encode_to_vec(&bloom, standard()).unwrap_or_else(|_| Vec::new());
        Self { key_range, fence_keys, bloom_filter: bloom_bytes }
    }

    #[must_use]
    pub fn might_contain(&self, key: &[u8]) -> bool {
        match decode_from_slice::<BloomFilter, _>(&self.bloom_filter, standard()) {
            Ok((bloom, _)) => bloom.contains(key),
            Err(_) => false,
        }
    }
}

pub struct SegmentFile { pub file: File }

impl SegmentFile {
    pub fn open(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        Ok(Self { file })
    }

    pub fn flush_segment(&mut self, pages: &[Page], footer: &SegmentFooter) -> io::Result<()> {
        for page in pages {
            let page_bytes = encode_to_vec(page, standard()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            self.file.write_all(&page_bytes)?;
        }
        let footer_bytes = encode_to_vec(footer, standard()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.file.write_all(&footer_bytes)?;
        self.file.sync_data()?;
        Ok(())
    }

    pub fn read_segment(&mut self) -> io::Result<(Vec<Page>, SegmentFooter)> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut buf = Vec::new();
        self.file.read_to_end(&mut buf)?;
        let mut offset = 0;
        let mut pages = Vec::new();
        while offset + 8 < buf.len() {
            let try_page = decode_from_slice::<Page, _>(&buf[offset..], standard());
            if let Ok((page, used)) = try_page { pages.push(page); offset += used; } else { break; }
        }
        let (footer, _) = decode_from_slice::<SegmentFooter, _>(&buf[offset..], standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok((pages, footer))
    }
}
