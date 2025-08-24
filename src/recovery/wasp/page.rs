use bincode::config::standard;
use bincode::serde::encode_to_vec;
use crc32fast::Hasher as Crc32Hasher;
use serde::{Deserialize, Serialize};

// Page/Segment Sizes
pub const WASP_PAGE_SIZE: usize = 16 * 1024; // 16 KB
pub const WASP_SEGMENT_SIZE: usize = 128 * 1024 * 1024; // 128 MB

// Page header (64 bytes)
#[repr(C)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PageHeader {
    pub page_id: u64,
    pub version: u64,
    pub page_type: u8, // 0 = unused, 1 = manifest, 2 = btree, 3 = segment, etc.
    #[serde(with = "serde_bytes")]
    pub reserved: [u8; 39],
    pub data_len: u32, // length of data
    pub crc32: u32,    // checksum of header+data
}

impl PageHeader {
    #[must_use]
    pub const fn new(page_id: u64, version: u64, page_type: u8, data_len: u32) -> Self {
        Self { page_id, version, page_type, reserved: [0; 39], data_len, crc32: 0 }
    }
}

// Page format: [header|data|crc32]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Page {
    pub header: PageHeader,
    pub data: Vec<u8>,
}

impl Page {
    #[must_use]
    pub fn new(page_id: u64, version: u64, page_type: u8, data: Vec<u8>) -> Self {
        let mut header = PageHeader::new(page_id, version, page_type, data.len() as u32);
        let mut hasher = Crc32Hasher::new();
        if let Ok(hdr_bytes) = encode_to_vec(header, standard()) {
            hasher.update(&hdr_bytes);
        }
        hasher.update(&data);
        header.crc32 = hasher.finalize();
        Self { header, data }
    }

    #[must_use]
    pub fn verify_crc(&self) -> bool {
        let mut header = self.header;
        let crc_orig = header.crc32;
        header.crc32 = 0;
        let mut hasher = Crc32Hasher::new();
        if let Ok(hdr_bytes) = encode_to_vec(header, standard()) {
            hasher.update(&hdr_bytes);
        }
        hasher.update(&self.data);
        hasher.finalize() == crc_orig
    }
}
