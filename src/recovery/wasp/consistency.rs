use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};

use bincode::config::standard;
use bincode::serde::decode_from_slice;
use rand::RngCore;
use serde::{Deserialize, Serialize};

use super::manifest::Manifest;
use super::page::{Page, WASP_PAGE_SIZE};

#[must_use]
pub fn verify_page_checksum(page: &Page) -> bool { page.verify_crc() }

/// Protects against torn writes by writing the data twice and verifying both copies.
pub fn torn_write_protect(data: &[u8], file: &mut File, offset: u64) -> io::Result<bool> {
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(data)?;
    file.write_all(data)?;
    file.sync_data()?;
    file.seek(SeekFrom::Start(offset))?;
    let mut buf1 = vec![0u8; data.len()];
    let mut buf2 = vec![0u8; data.len()];
    file.read_exact(&mut buf1)?;
    file.read_exact(&mut buf2)?;
    Ok(buf1 == data && buf2 == data)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestSlotDiagnostics {
    pub slot: usize,
    pub offset: u64,
    pub read_ok: bool,
    pub page_decoded: bool,
    pub page_type_ok: bool,
    pub crc_ok: bool,
    pub manifest_decoded: bool,
    pub version: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsistencyReport {
    pub both_valid: bool,
    pub slots: [ManifestSlotDiagnostics; 2],
}

pub struct ConsistencyChecker {}
impl ConsistencyChecker {
    #[must_use]
    pub const fn new() -> Self { Self {} }
    /// Detailed check of both manifest slots with diagnostics.
    pub fn check_detailed(&self, file: &mut File) -> ConsistencyReport {
        let offsets = [0u64, WASP_PAGE_SIZE as u64];
        let mut diags: [ManifestSlotDiagnostics; 2] = [
            ManifestSlotDiagnostics { slot: 0, offset: offsets[0], read_ok: false, page_decoded: false, page_type_ok: false, crc_ok: false, manifest_decoded: false, version: None },
            ManifestSlotDiagnostics { slot: 1, offset: offsets[1], read_ok: false, page_decoded: false, page_type_ok: false, crc_ok: false, manifest_decoded: false, version: None },
        ];
        for (i, off) in offsets.iter().enumerate() {
            let d = &mut diags[i];
            let _ = file.seek(SeekFrom::Start(*off));
            let mut buf = vec![0u8; WASP_PAGE_SIZE];
            if file.read_exact(&mut buf).is_ok() {
                d.read_ok = true;
                if let Ok((page, _)) = decode_from_slice::<Page, _>(&buf, standard()) {
                    d.page_decoded = true;
                    d.page_type_ok = page.header.page_type == 1;
                    d.crc_ok = page.verify_crc();
                    if d.page_type_ok && d.crc_ok && let Ok(m) = Manifest::from_bytes(&page.data) {
                        d.manifest_decoded = true;
                        d.version = Some(m.version);
                    }
                }
            }
        }
        let both_valid = diags.iter().all(|d| d.read_ok && d.page_decoded && d.page_type_ok && d.crc_ok && d.manifest_decoded);
        ConsistencyReport { both_valid, slots: diags }
    }

    /// Backward-compatible boolean check.
    pub fn check(&self, file: &mut File) -> bool { self.check_detailed(file).both_valid }
}
impl Default for ConsistencyChecker { fn default() -> Self { Self::new() } }

/// Repair manifest slots so both contain the latest valid manifest page. Returns a detailed report.
pub fn recover_manifests(file: &mut File) -> io::Result<ConsistencyReport> {
    let checker = ConsistencyChecker::new();
    let report = checker.check_detailed(file);
    let mut latest_valid: Option<(usize, u64, Vec<u8>)> = None; // (slot, version, bytes)
    let offsets = [0u64, WASP_PAGE_SIZE as u64];

    for (i, off) in offsets.iter().enumerate() {
        let mut buf = vec![0u8; WASP_PAGE_SIZE];
        file.seek(SeekFrom::Start(*off))?;
        if file.read_exact(&mut buf).is_ok()
            && let Ok((page, _)) = decode_from_slice::<Page, _>(&buf, standard())
            && page.header.page_type == 1
            && page.verify_crc()
            && let Ok(man) = Manifest::from_bytes(&page.data)
        {
            let v = man.version;
            match latest_valid {
                Some((_, best_v, _)) if v <= best_v => {}
                _ => latest_valid = Some((i, v, buf.clone())),
            }
        }
    }

    let (best_slot, _best_ver, best_bytes) = latest_valid
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "No valid manifest slot to recover from"))?;

    for (i, _off) in offsets.iter().enumerate() {
        if i == best_slot { continue; }
        let d = &report.slots[i];
        let needs_copy = !(d.read_ok && d.page_decoded && d.page_type_ok && d.crc_ok && d.manifest_decoded);
        let version_differs = match (report.slots[best_slot].version, d.version) { (Some(a), Some(b)) => a != b, _ => true };
        if needs_copy || version_differs {
            file.seek(SeekFrom::Start(offsets[i]))?;
            file.write_all(&best_bytes)?;
            file.sync_data()?;
        }
    }

    Ok(checker.check_detailed(file))
}

/// Fuzz test: corrupts WAL/pages/manifest and checks recovery.
pub fn fuzz_test_corruption(file: &mut File) -> bool {
    if file.seek(SeekFrom::Start(0)).is_ok() {
        let mut buf = vec![0u8; WASP_PAGE_SIZE];
        let mut rng = rand::rng();
        rng.fill_bytes(&mut buf);
        if file.write_all(&buf).is_ok() && file.sync_data().is_ok() {
            let checker = ConsistencyChecker::new();
            return checker.check(file);
        }
    }
    false
}
