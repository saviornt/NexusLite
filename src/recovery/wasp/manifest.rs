use std::fs::OpenOptions;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};

use bincode::config::standard;
use bincode::serde::{decode_from_slice, encode_to_vec};
use serde::{Deserialize, Serialize};

use super::page::{Page, WASP_PAGE_SIZE};

// Manifest structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u64,
    pub root_page_id: u64,
    pub active_segments: Vec<String>, // segment file names
    pub wal_metadata: Option<String>,
    // CoW B-tree allocator state
    #[serde(default)]
    pub next_page_id: u64,
    #[serde(default)]
    pub free_pages: Vec<u64>,
}

impl Manifest {
    #[must_use]
    pub const fn new() -> Self {
        Self { version: 1, root_page_id: 0, active_segments: Vec::new(), wal_metadata: None, next_page_id: 1, free_pages: Vec::new() }
    }
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> { encode_to_vec(self, standard()).unwrap_or_default() }
    /// # Errors
    /// Returns an error if deserialization fails.
    pub fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        decode_from_slice::<Self, _>(bytes, standard()).map(|(m, _)| m).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

impl Default for Manifest { fn default() -> Self { Self::new() } }

// WASP file: double-buffered manifest at start, then pages/segments
pub struct WaspFile {
    pub file: File,
    pub manifest_offsets: [u64; 2], // double-buffered
    pub manifest_version: u64,
}

impl WaspFile {
    /// # Errors
    /// Returns an error if the file cannot be opened or initialized.
    pub fn open(path: std::path::PathBuf) -> io::Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&path)?;
        let manifest_offsets = [0, WASP_PAGE_SIZE as u64];
        let meta = file.metadata()?;
        let min_size = 2 * WASP_PAGE_SIZE as u64;
        if meta.len() < min_size {
            // Write an initial manifest page to both slots
            let manifest = Manifest::new();
            let data = manifest.to_bytes();
            let page = Page::new(0, manifest.version, 1, data);
            let mut page_bytes = encode_to_vec(&page, standard()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            if page_bytes.len() < WASP_PAGE_SIZE { page_bytes.resize(WASP_PAGE_SIZE, 0); }
            file.seek(SeekFrom::Start(0))?; file.write_all(&page_bytes)?;
            file.seek(SeekFrom::Start(WASP_PAGE_SIZE as u64))?; file.write_all(&page_bytes)?;
            file.sync_data()?;
        }
        Ok(Self { file, manifest_offsets, manifest_version: 0 })
    }

    /// Write manifest to the next buffer slot (flip)
    pub fn write_manifest(&mut self, manifest: &Manifest) -> io::Result<()> {
        let next_slot = (self.manifest_version as usize + 1) % 2;
        let offset = self.manifest_offsets[next_slot];
        let data = manifest.to_bytes();
        let page = Page::new(0, manifest.version, 1, data);
        let page_bytes = encode_to_vec(&page, standard()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&page_bytes)?;
        self.file.sync_data()?;
        self.manifest_version = manifest.version;
        Ok(())
    }

    /// Read the latest valid manifest (check both slots)
    pub fn read_manifest(&mut self) -> io::Result<Manifest> {
        let mut best: Option<(u64, Manifest)> = None;
        for &offset in &self.manifest_offsets {
            self.file.seek(SeekFrom::Start(offset))?;
            let mut buf = vec![0u8; WASP_PAGE_SIZE];
            self.file.read_exact(&mut buf)?;
            let (page, _): (Page, _) = decode_from_slice::<Page, _>(&buf, standard()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            if page.header.page_type == 1 && page.verify_crc() && let Ok(manifest) = Manifest::from_bytes(&page.data) && best.as_ref().is_none_or(|(v, _)| manifest.version > *v) {
                best = Some((manifest.version, manifest));
            }
        }
        match best { Some((_, m)) => Ok(m), None => Err(io::Error::new(io::ErrorKind::NotFound, "No valid manifest found")) }
    }
}
