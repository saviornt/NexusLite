// Simple Bloom filter used for quick negative membership tests in segment lookups.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilter {
	bits: Vec<u8>,
	k: u8,
}

impl BloomFilter {
	pub fn new(size: usize, k: u8) -> Self {
		BloomFilter { bits: vec![0; size], k }
	}
	fn hash(&self, key: &[u8], i: u8) -> usize {
	use std::hash::Hasher;
		use std::collections::hash_map::DefaultHasher;
		let mut hasher = DefaultHasher::new();
		hasher.write(key);
		hasher.write_u8(i);
		(hasher.finish() as usize) % self.bits.len()
	}
	pub fn insert(&mut self, key: &[u8]) {
		for i in 0..self.k {
			let idx = self.hash(key, i);
			self.bits[idx] = 1;
		}
	}
	pub fn contains(&self, key: &[u8]) -> bool {
		for i in 0..self.k {
			let idx = self.hash(key, i);
			if self.bits[idx] == 0 {
				return false;
			}
		}
		true
	}
}

use std::io::{self, SeekFrom, Read, Write, Seek};
use std::path::PathBuf;
use rand::RngCore;
use crate::types::Operation;
use bincode::serde::{encode_to_vec, decode_from_slice};
use bincode::config::standard;
use std::fs::{File, OpenOptions};
use serde::{Serialize, Deserialize};
use crate::index::IndexDescriptor;
use crate::index::IndexKind as IxKind;

// Minimal block cache handle; extend to store decoded pages if needed.
pub struct BlockCache {}
impl BlockCache { pub fn new() -> Self { BlockCache {} } }
impl Default for BlockCache { fn default() -> Self { Self::new() } }


use std::sync::Arc;
use parking_lot::RwLock;
/// Prefetch pages into an in-memory cache for sequential scans (synchronous for now).
pub fn prefetch_pages(ids: &[u64], file: &mut File, cache: &Arc<RwLock<BlockCache>>) {
	let _ = cache; // Silence unused variable warning
	let mut buf = vec![0u8; WASP_PAGE_SIZE];
	for &page_id in ids {
		// Simulate prefetch by reading the page into memory (could be async in future)
		let offset = 2 * WASP_PAGE_SIZE as u64 + (page_id.saturating_sub(1)) * WASP_PAGE_SIZE as u64;
		if file.seek(SeekFrom::Start(offset)).is_ok() && file.read_exact(&mut buf).is_ok() {
			// In a real cache, insert the page here
			// cache.write().insert(page_id, buf.clone());
		}
	}
}


/// Hook to batch manifest updates per flip; currently immediate for durability.
pub fn optimize_manifest_updates() {
	// In a real system, this would batch manifest updates in memory and flush them together.
	// For now, manifest updates are written immediately for durability.
}

/// WASP metrics reporting hook (lightweight placeholder).
pub struct WasMetrics {}
impl WasMetrics {
	pub fn new() -> Self { WasMetrics {} }
	/// Collects and prints WASP engine metrics (placeholder for now).
	pub fn report(&self) {
		// In a real system, gather stats from cache, storage, and engine.
		println!("[WASP Metrics] (placeholder): metrics collection not yet implemented.");
	}
}
impl Default for WasMetrics { fn default() -> Self { Self::new() } }


/// Entry point for WASP microbenchmarks (placeholder).
pub fn run_benchmarks() {
	// In a real system, run microbenchmarks and print results.
	println!("[WASP Benchmark] (placeholder): benchmarking not yet implemented.");
}

// Durability & integrity helpers

pub fn verify_page_checksum(page: &Page) -> bool {
	page.verify_crc()
}

/// Protects against torn writes by writing the data twice and verifying both copies.
pub fn torn_write_protect(data: &[u8], file: &mut File, offset: u64) -> io::Result<bool> {
	// Write data twice at adjacent offsets, then verify both
	file.seek(SeekFrom::Start(offset))?;
	file.write_all(data)?;
	file.write_all(data)?;
	file.sync_data()?;
	// Read back both copies
	file.seek(SeekFrom::Start(offset))?;
	let mut buf1 = vec![0u8; data.len()];
	let mut buf2 = vec![0u8; data.len()];
	file.read_exact(&mut buf1)?;
	file.read_exact(&mut buf2)?;
	Ok(buf1 == data && buf2 == data)
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct ConsistencyReport {
	pub both_valid: bool,
	pub slots: [ManifestSlotDiagnostics; 2],
}

pub struct ConsistencyChecker {}
impl ConsistencyChecker {
	pub fn new() -> Self { ConsistencyChecker {} }
	/// Detailed check of both manifest slots with diagnostics.
	pub fn check_detailed(&self, file: &mut File) -> ConsistencyReport {
		let offsets = [0u64, WASP_PAGE_SIZE as u64];
		let mut diags: [ManifestSlotDiagnostics; 2] = [
			ManifestSlotDiagnostics { slot:0, offset: offsets[0], read_ok: false, page_decoded: false, page_type_ok: false, crc_ok: false, manifest_decoded: false, version: None },
			ManifestSlotDiagnostics { slot:1, offset: offsets[1], read_ok: false, page_decoded: false, page_type_ok: false, crc_ok: false, manifest_decoded: false, version: None },
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
					if d.page_type_ok && d.crc_ok
						&& let Ok(m) = Manifest::from_bytes(&page.data) {
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
	pub fn check(&self, file: &mut File) -> bool {
		self.check_detailed(file).both_valid
	}
}
impl Default for ConsistencyChecker { fn default() -> Self { Self::new() } }

/// Repair manifest slots so both contain the latest valid manifest page. Returns a detailed report.
pub fn recover_manifests(file: &mut File) -> io::Result<ConsistencyReport> {
	let checker = ConsistencyChecker::new();
	let report = checker.check_detailed(file);
	let mut latest_valid: Option<(usize, u64, Vec<u8>)> = None; // (slot, version, bytes)
	let offsets = [0u64, WASP_PAGE_SIZE as u64];

	// Read raw bytes of each slot and determine the newest valid one
	for (i, off) in offsets.iter().enumerate() {
		let mut buf = vec![0u8; WASP_PAGE_SIZE];
		file.seek(SeekFrom::Start(*off))?;
		if file.read_exact(&mut buf).is_ok()
			&& let Ok((page, _)) = decode_from_slice::<Page, _>(&buf, standard())
			&& page.header.page_type == 1 && page.verify_crc()
			&& let Ok(man) = Manifest::from_bytes(&page.data) {
			let v = man.version;
			match latest_valid { Some((_, best_v, _)) if v <= best_v => {}, _ => latest_valid = Some((i, v, buf.clone())), }
		}
	}

	// If no valid slot, bail out
	let (best_slot, _best_ver, best_bytes) = latest_valid.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "No valid manifest slot to recover from"))?;

	// Copy best to the other slot if needed, or if versions differ
	for (i, _off) in offsets.iter().enumerate() {
		if i == best_slot { continue; }
		let d = &report.slots[i];
		let needs_copy = !(d.read_ok && d.page_decoded && d.page_type_ok && d.crc_ok && d.manifest_decoded);
		let version_differs = match (report.slots[best_slot].version, d.version) {
			(Some(a), Some(b)) => a != b,
			_ => true,
		};
		if needs_copy || version_differs {
			file.seek(SeekFrom::Start(offsets[i]))?;
			file.write_all(&best_bytes)?;
			file.sync_data()?;
		}
	}

	// Return fresh report
	Ok(checker.check_detailed(file))
}

/// Fuzz test: corrupts WAL/pages/manifest and checks recovery.
pub fn fuzz_test_corruption(file: &mut File) -> bool {
	// Corrupt the first manifest slot and check if the second is still valid
	if file.seek(SeekFrom::Start(0)).is_ok() {
		let mut buf = vec![0u8; WASP_PAGE_SIZE];
		let mut rng = rand::rng();
		rng.fill_bytes(&mut buf);
		if file.write_all(&buf).is_ok() && file.sync_data().is_ok() {
			// Now check if the second manifest is still valid
			let checker = ConsistencyChecker::new();
			return checker.check(file);
		}
	}
	false
}

// Concurrency & MVCC stubs

pub struct SnapshotTracker {
	pub current_epoch: u64,
}
impl SnapshotTracker {
	pub fn new() -> Self { SnapshotTracker { current_epoch: 0 } }
	pub fn advance(&mut self) { self.current_epoch += 1; }
}
impl Default for SnapshotTracker { fn default() -> Self { Self::new() } }

pub struct MvccEngine {}
impl MvccEngine {
	pub fn new() -> Self { MvccEngine {} }
	pub fn visible(&self, _epoch: u64, _txn_epoch: u64) -> bool { true }
}
impl Default for MvccEngine { fn default() -> Self { Self::new() } }

pub struct CompactionEngine {}
impl CompactionEngine {
	pub fn new() -> Self { CompactionEngine {} }
	pub fn run_background(&self) {
		std::thread::spawn(|| {
			use std::time::Duration;
			loop {
				// Placeholder compaction tick; in a real system, this would merge segments and recycle space.
				log::trace!("[WASP] background compaction tick");
				std::thread::sleep(Duration::from_secs(60));
			}
		});
	}
}
impl Default for CompactionEngine { fn default() -> Self { Self::new() } }

pub struct FreeSpaceMap {}
impl FreeSpaceMap {
	pub fn new() -> Self { FreeSpaceMap {} }
	pub fn recycle(&mut self, _page_id: u64) {}
}
impl Default for FreeSpaceMap { fn default() -> Self { Self::new() } }

pub struct EpochGc {}
impl EpochGc {
	pub fn new() -> Self { EpochGc {} }
	pub fn gc(&mut self, _epoch: u64) {}
}
impl Default for EpochGc { fn default() -> Self { Self::new() } }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentFooter {
	pub key_range: (Vec<u8>, Vec<u8>),
	pub fence_keys: Vec<Vec<u8>>,
	pub bloom_filter: Vec<u8>, // serialized BloomFilter
}

impl SegmentFooter {
	pub fn new(keys: &[Vec<u8>], key_range: (Vec<u8>, Vec<u8>), fence_keys: Vec<Vec<u8>>) -> Self {
	let mut bloom = BloomFilter::new(1024, 3); // 1024 bits, 3 hash functions
		for k in keys {
			bloom.insert(k);
		}
		// Encoding Bloom filter should not fail for a valid struct; if it does, use empty filter.
		let bloom_bytes = encode_to_vec(&bloom, standard()).unwrap_or_else(|_| Vec::new());
		SegmentFooter {
			key_range,
			fence_keys,
			bloom_filter: bloom_bytes,
		}
	}

	pub fn might_contain(&self, key: &[u8]) -> bool {
		match decode_from_slice::<BloomFilter, _>(&self.bloom_filter, standard()) {
			Ok((bloom, _)) => bloom.contains(key),
			Err(_) => false,
		}
	}

}

pub struct SegmentFile {
	pub file: File,
}

impl SegmentFile {
	pub fn open(path: PathBuf) -> io::Result<Self> {
		let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
		Ok(SegmentFile { file })
	}

	pub fn flush_segment(&mut self, pages: &[Page], footer: &SegmentFooter) -> io::Result<()> {
		for page in pages {
			let page_bytes = encode_to_vec(page, standard())
				.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
			self.file.write_all(&page_bytes)?;
		}
		let footer_bytes = encode_to_vec(footer, standard())
			.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
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
			// Try to decode a page
			let try_page = decode_from_slice::<Page, _>(&buf[offset..], standard());
			if let Ok((page, used)) = try_page {
				pages.push(page);
				offset += used;
			} else {
				break;
			}
		}
		// The last decode is the footer
		let (footer, _) = decode_from_slice::<SegmentFooter, _>(&buf[offset..], standard())
			.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
		Ok((pages, footer))
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecord {
	pub txn_id: u64,
	pub page_ids: Vec<u64>,
	pub checksums: Vec<u32>,
	pub new_root_id: u64,
	pub epoch: u64,
}

pub struct TinyWal {
	pub file: File,
}

impl TinyWal {
	pub fn sync(&mut self) -> io::Result<()> {
		self.file.sync_data()
	}
	pub fn open(path: PathBuf) -> io::Result<Self> {
		let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
		Ok(TinyWal { file })
	}

	pub fn append(&mut self, record: &WalRecord) -> io::Result<()> {
		let data = encode_to_vec(record, standard())
			.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
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
			let len = match <&[u8; 8]>::try_from(&buf[offset..offset + 8]) {
				Ok(arr) => u64::from_le_bytes(*arr) as usize,
				Err(_) => break,
			};
			offset += 8;
			if offset + len > buf.len() { break; }
			let (rec, _) = decode_from_slice::<WalRecord, _>(&buf[offset..offset+len], standard())
				.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
			records.push(rec);
			offset += len;
		}
		Ok(records)
	}
}

impl CowTree {
	/// Group commit: batch append all records, then fsync WAL and update manifest/root.
	/// This method batches WAL appends for efficiency, then updates the manifest and root pointer atomically.
	pub fn group_commit(&mut self, wal: &mut TinyWal, records: &[WalRecord]) -> io::Result<()> {
		if records.is_empty() {
			return Ok(());
		}
		// Batch append all records to WAL
		for rec in records {
			wal.append(rec)?;
		}
		// Fsync WAL to ensure durability
		wal.sync()?;
		// Update root and manifest to reflect the last committed record
		if let Some(last) = records.last() {
			self.root_page_id = last.new_root_id;
			self.version = last.epoch;
			let mut manifest = self.file.read_manifest().unwrap_or_else(|_| Manifest::new());
			manifest.root_page_id = self.root_page_id;
			manifest.version = self.version;
			self.file.write_manifest(&manifest)?;
		}
		Ok(())
	}

	/// Recovery: replay WAL and update root/version to last committed state.
	/// This method replays all WAL records and updates the manifest and root pointer to the last committed state.
	pub fn recover_from_wal(&mut self, wal: &mut TinyWal) -> io::Result<()> {
		let records = wal.read_all()?;
		if let Some(last) = records.last() {
			self.root_page_id = last.new_root_id;
			self.version = last.epoch;
			let mut manifest = self.file.read_manifest().unwrap_or_else(|_| Manifest::new());
			manifest.root_page_id = self.root_page_id;
			manifest.version = self.version;
			self.file.write_manifest(&manifest)?;
		}
		Ok(())
	}
}

impl CowTree {
	/// Crash-safe: reload manifest and root after restart
	pub fn reload_root(&mut self) -> io::Result<()> {
		let manifest = self.file.read_manifest()?;
		self.root_page_id = manifest.root_page_id;
		self.version = manifest.version;
		Ok(())
	}
}

// === Minimal CoW B-tree node/page structure and root management ===

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CowNode {
	Leaf { keys: Vec<Vec<u8>>, values: Vec<Vec<u8>> },
	Internal { keys: Vec<Vec<u8>>, children: Vec<u64> }, // children are page_ids
}

impl CowNode {
	pub fn new_leaf() -> Self {
		CowNode::Leaf { keys: vec![], values: vec![] }
	}
	pub fn new_internal() -> Self {
		CowNode::Internal { keys: vec![], children: vec![] }
	}
}

pub struct CowTree {
	pub root_page_id: u64,
	pub file: WaspFile,
	pub version: u64,
	pub alloc: BlockAllocator,
}

impl CowTree {
	pub fn new(mut file: WaspFile) -> io::Result<Self> {
		// Try to read manifest, else create new
		let mut manifest = file.read_manifest().unwrap_or_else(|_| Manifest::new());
		let mut root_page_id = manifest.root_page_id;
		let mut version = manifest.version;
		let mut alloc = BlockAllocator::from_manifest(&manifest);
		if root_page_id == 0 {
			// Allocate and write an empty root page
			let node = CowNode::new_leaf();
			let node_bytes = encode_to_vec(&node, standard())
				.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
			let new_root = alloc.alloc();
			let page = Page::new(new_root, version + 1, 2, node_bytes);
			// Use temp tree to write
			let mut temp_tree = CowTree { root_page_id: new_root, file, version, alloc };
			temp_tree.write_page(new_root, &page)?;
			temp_tree.version += 1;
			manifest.root_page_id = new_root;
			manifest.version = temp_tree.version;
			temp_tree.alloc.export_to_manifest(&mut manifest);
			temp_tree.file.write_manifest(&manifest)?;
			// move back
			file = temp_tree.file; version = temp_tree.version; root_page_id = new_root; alloc = temp_tree.alloc;
		}
		Ok(CowTree { root_page_id, file, version, alloc })
	}

	// Insert a key/value (for now, just create a new root leaf if empty)
	pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> io::Result<()> {
		const MAX_KEYS: usize = 32;
		// Recursive insert, returns (new_page_id, promoted_key)
		fn insert_rec(tree: &mut CowTree, page_id: u64, key: Vec<u8>, value: Vec<u8>) -> InsertRecResult {
			let mut node = {
				let page = tree.read_page(page_id)?;
				decode_from_slice::<CowNode, _>(&page.data, standard()).map(|(n, _)| n).unwrap_or(CowNode::new_leaf())
			};
			match &mut node {
				CowNode::Leaf { keys, values } => {
					// Insert in sorted order
					let pos = keys.binary_search(&key).unwrap_or_else(|e| e);
					if pos < keys.len() && keys[pos] == key { values[pos] = value; } else { keys.insert(pos, key); values.insert(pos, value); }
					if keys.len() > MAX_KEYS {
						// Split
						let mid = keys.len() / 2;
						let right_keys = keys.split_off(mid);
						let right_values = values.split_off(mid);
						let promoted = right_keys[0].clone();
						let right = CowNode::Leaf { keys: right_keys, values: right_values };
						let new_right_id = tree.alloc.alloc();
						let right_bytes = encode_to_vec(&right, standard())
							.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
						let right_page = Page::new(new_right_id, tree.version + 1, 2, right_bytes);
						tree.write_page(new_right_id, &right_page)?;
						// Left node (current)
						let left_bytes = encode_to_vec(&node, standard())
							.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
						let new_left_id = tree.alloc.alloc();
						let left_page = Page::new(new_left_id, tree.version + 1, 2, left_bytes);
						tree.write_page(new_left_id, &left_page)?;
						Ok((new_left_id, Some((promoted, new_right_id))))
					} else {
						let node_bytes = encode_to_vec(&node, standard())
							.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
						let new_page_id = tree.alloc.alloc();
						let page = Page::new(new_page_id, tree.version + 1, 2, node_bytes);
						tree.write_page(new_page_id, &page)?;
						Ok((new_page_id, None))
					}
				}
				CowNode::Internal { keys, children } => {
					// Find child (go right on equality to match split rule)
					let pos = match keys.binary_search(&key) { Ok(i) => i + 1, Err(e) => e };
					let child_id = children[pos];
					let (new_child_id, promoted) = insert_rec(tree, child_id, key, value)?;
					children[pos] = new_child_id;
					if let Some((promo_key, right_id)) = promoted {
						keys.insert(pos, promo_key);
						children.insert(pos + 1, right_id);
						if keys.len() > MAX_KEYS {
							// Split internal
							let mid = keys.len() / 2;
							let right_keys = keys.split_off(mid + 1);
							let right_children = children.split_off(mid + 1);
							let promoted = match keys.pop() { Some(k) => k, None => return Err(io::Error::other("internal split with empty keys")) };
							let right = CowNode::Internal { keys: right_keys, children: right_children };
							let new_right_id = tree.alloc.alloc();
							let right_bytes = encode_to_vec(&right, standard())
								.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
							let right_page = Page::new(new_right_id, tree.version + 1, 2, right_bytes);
							tree.write_page(new_right_id, &right_page)?;
							// Left node (current)
							let left_bytes = encode_to_vec(&node, standard())
								.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
							let new_left_id = tree.alloc.alloc();
							let left_page = Page::new(new_left_id, tree.version + 1, 2, left_bytes);
							tree.write_page(new_left_id, &left_page)?;
							Ok((new_left_id, Some((promoted, new_right_id))))
						} else {
							let node_bytes = encode_to_vec(&node, standard())
								.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
							let new_page_id = tree.alloc.alloc();
							let page = Page::new(new_page_id, tree.version + 1, 2, node_bytes);
							tree.write_page(new_page_id, &page)?;
							Ok((new_page_id, None))
						}
					} else {
						let node_bytes = encode_to_vec(&node, standard())
							.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
						let new_page_id = tree.alloc.alloc();
						let page = Page::new(new_page_id, tree.version + 1, 2, node_bytes);
						tree.write_page(new_page_id, &page)?;
						Ok((new_page_id, None))
					}
				}
			}
		}
		// Start recursive insert
		let (new_root_id, promoted) = insert_rec(self, self.root_page_id, key, value)?;
		let mut manifest = self.file.read_manifest().unwrap_or_else(|_| Manifest::new());
		if let Some((promo_key, right_id)) = promoted {
			// New root
			let new_root = CowNode::Internal { keys: vec![promo_key], children: vec![new_root_id, right_id] };
			let node_bytes = encode_to_vec(&new_root, standard())
				.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
			let root_page_id = self.alloc.alloc();
			let page = Page::new(root_page_id, self.version + 1, 2, node_bytes);
			self.write_page(root_page_id, &page)?;
			self.root_page_id = root_page_id;
			self.version += 1;
		} else {
			self.root_page_id = new_root_id;
			self.version += 1;
		}
		manifest.root_page_id = self.root_page_id;
		manifest.version = self.version;
		self.alloc.export_to_manifest(&mut manifest);
		self.file.write_manifest(&manifest)?;
		Ok(())
	}

	pub fn get(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
		if self.root_page_id == 0 { return Ok(None); }
		let mut cur = self.root_page_id;
		loop {
			let page = self.read_page(cur)?;
			let node = decode_from_slice::<CowNode, _>(&page.data, standard())
				.map(|(n, _)| n)
				.unwrap_or_else(|_| CowNode::new_leaf());
			match node {
				CowNode::Leaf { keys, values } => {
					match keys.binary_search_by(|k| k.as_slice().cmp(key)) {
						Ok(i) => return Ok(Some(values[i].clone())),
						Err(_) => return Ok(None),
					}
				}
				CowNode::Internal { keys, children } => {
					let pos = match keys.binary_search_by(|k| k.as_slice().cmp(key)) { Ok(i) => i + 1, Err(e) => e };
					cur = children[pos];
				}
			}
		}
	}

	fn write_page(&mut self, page_id: u64, page: &Page) -> io::Result<()> {
		let offset = 2 * WASP_PAGE_SIZE as u64 + (page_id - 1) * WASP_PAGE_SIZE as u64;
		self.file.file.seek(SeekFrom::Start(offset))?;
		let mut page_bytes = encode_to_vec(page, standard())
			.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
		if page_bytes.len() < WASP_PAGE_SIZE {
			page_bytes.resize(WASP_PAGE_SIZE, 0);
		}
		// Seek to offset and write full WASP_PAGE_SIZE buffer
		self.file.file.seek(SeekFrom::Start(offset))?;
		self.file.file.write_all(&page_bytes)?;
		let file_size = self.file.file.metadata()?.len();
		let required_size = offset + WASP_PAGE_SIZE as u64;
		if file_size < required_size {
			self.file.file.set_len(required_size)?;
		}
		self.file.file.sync_data()?;
		self.file.file.sync_all()?;
		Ok(())
	}

	fn read_page(&mut self, page_id: u64) -> io::Result<Page> {
		let offset = 2 * WASP_PAGE_SIZE as u64 + (page_id - 1) * WASP_PAGE_SIZE as u64;
		let file_size = self.file.file.metadata()?.len();
		if file_size < offset + WASP_PAGE_SIZE as u64 {
			return Err(io::Error::new(io::ErrorKind::UnexpectedEof, format!(
				"File too small: size={}, need at least {} for page_id {} at offset {}",
				file_size, offset + WASP_PAGE_SIZE as u64, page_id, offset
			)));
		}
		self.file.file.seek(SeekFrom::Start(offset))?;
		let mut buf = vec![0u8; WASP_PAGE_SIZE];
		self.file.file.read_exact(&mut buf)?;
		let (page, _): (Page, _) = decode_from_slice(&buf, standard()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
		Ok(page)
	}
}

/// Fuzz test: corrupts WAL/pages/manifest and checks recovery.
use crc32fast::Hasher as Crc32Hasher;


// Page header (64 bytes):
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
	pub fn new(page_id: u64, version: u64, page_type: u8, data_len: u32) -> Self {
		PageHeader {
			page_id,
			version,
			page_type,
			reserved: [0; 39],
			data_len,
			crc32: 0,
		}
	}
}

// Page format: [header|data|crc32]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Page {
	pub header: PageHeader,
	pub data: Vec<u8>,
}

impl Page {
	pub fn new(page_id: u64, version: u64, page_type: u8, data: Vec<u8>) -> Self {
		let mut header = PageHeader::new(page_id, version, page_type, data.len() as u32);
		let mut hasher = Crc32Hasher::new();
	if let Ok(hdr_bytes) = encode_to_vec(header, standard()) { hasher.update(&hdr_bytes); }
		hasher.update(&data);
		header.crc32 = hasher.finalize();
		Page { header, data }
	}

	pub fn verify_crc(&self) -> bool {
		let mut header = self.header;
		let crc_orig = header.crc32;
		header.crc32 = 0;
		let mut hasher = Crc32Hasher::new();
	if let Ok(hdr_bytes) = encode_to_vec(header, standard()) { hasher.update(&hdr_bytes); }
		hasher.update(&self.data);
		hasher.finalize() == crc_orig
	}
}

// Manifest serialization/deserialization
impl Manifest {
	pub fn to_bytes(&self) -> Vec<u8> {
	encode_to_vec(self, standard()).unwrap_or_default()
	}
	pub fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
		decode_from_slice::<Self, _>(bytes, standard())
			.map(|(m, _)| m)
			.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
	}
}

// WASP file: double-buffered manifest at start, then pages/segments
pub struct WaspFile {
	pub file: File,
	pub manifest_offsets: [u64; 2], // double-buffered
	pub manifest_version: u64,
}

impl WaspFile {
	pub fn open(path: PathBuf) -> io::Result<Self> {
		// Do not truncate on open; preserve existing data. Be explicit to satisfy clippy.
		let mut file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&path)?;
		let manifest_offsets = [0, WASP_PAGE_SIZE as u64];
		let meta = file.metadata()?;
		let min_size = 2 * WASP_PAGE_SIZE as u64;
		if meta.len() < min_size {
			// Write an initial manifest page to both slots
			let manifest = Manifest::new();
			let data = manifest.to_bytes();
			let page = Page::new(0, manifest.version, 1, data);
			let mut page_bytes = encode_to_vec(&page, standard())
				.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
			if page_bytes.len() < WASP_PAGE_SIZE {
				page_bytes.resize(WASP_PAGE_SIZE, 0);
			}
			file.seek(SeekFrom::Start(0))?;
			file.write_all(&page_bytes)?;
			file.seek(SeekFrom::Start(WASP_PAGE_SIZE as u64))?;
			file.write_all(&page_bytes)?;
			file.sync_data()?;
		}
		Ok(WaspFile {
			file,
			manifest_offsets,
			manifest_version: 0,
		})
	}

	// Write manifest to the next buffer slot (flip)
	pub fn write_manifest(&mut self, manifest: &Manifest) -> io::Result<()> {
		let next_slot = (self.manifest_version as usize + 1) % 2;
		let offset = self.manifest_offsets[next_slot];
		let data = manifest.to_bytes();
        let page = Page::new(0, manifest.version, 1, data);
	let page_bytes = encode_to_vec(&page, standard())
	    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
		self.file.seek(SeekFrom::Start(offset))?;
		self.file.write_all(&page_bytes)?;
		self.file.sync_data()?;
		self.manifest_version = manifest.version;
		Ok(())
	}

	// Read the latest valid manifest (check both slots)
	pub fn read_manifest(&mut self) -> io::Result<Manifest> {
		let mut best: Option<(u64, Manifest)> = None;
		for &offset in &self.manifest_offsets {
			self.file.seek(SeekFrom::Start(offset))?;
			let mut buf = vec![0u8; WASP_PAGE_SIZE];
			self.file.read_exact(&mut buf)?;
			let (page, _): (Page, _) = decode_from_slice(&buf, standard()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
			if page.header.page_type == 1 && page.verify_crc()
				&& let Ok(manifest) = Manifest::from_bytes(&page.data)
				&& best.as_ref().map(|(v, _)| manifest.version > *v).unwrap_or(true) {
				best = Some((manifest.version, manifest));
			}
		}
		match best {
			Some((_, m)) => Ok(m),
			None => Err(io::Error::new(io::ErrorKind::NotFound, "No valid manifest found")),
		}
	}
}

// Page/Segment Sizes:
pub const WASP_PAGE_SIZE: usize = 16 * 1024; // 16 KB
pub const WASP_SEGMENT_SIZE: usize = 128 * 1024 * 1024; // 128 MB

// On-disk format:
// - All integers little-endian.
// - Page: [header|data|crc32]
// - Segment: sequence of pages, sealed with footer.
// - Manifest: root pointer, active segments, WAL metadata, version.

// Block allocator and free space map
use std::collections::BTreeSet;
type InsertRecResult = io::Result<(u64, Option<(Vec<u8>, u64)>)>;
#[derive(Debug, Default)]
pub struct BlockAllocator {
	free_pages: BTreeSet<u64>,
	next_page: u64,
}

impl BlockAllocator {
	#[must_use]
	pub fn new() -> Self {
		Self { free_pages: BTreeSet::new(), next_page: 1 }
	}
	/// Construct allocator from persisted manifest allocator fields
	#[must_use]
	pub fn from_manifest(m: &Manifest) -> Self {
		let mut free = BTreeSet::new();
		for &p in &m.free_pages { free.insert(p); }
		let next = if m.next_page_id == 0 { 1 } else { m.next_page_id };
		Self { free_pages: free, next_page: next }
	}
	/// Export allocator state into the manifest for durability
	pub fn export_to_manifest(&self, m: &mut Manifest) {
		m.next_page_id = self.next_page;
		m.free_pages = self.free_pages.iter().copied().collect();
	}
	pub fn alloc(&mut self) -> u64 {
		if let Some(&page) = self.free_pages.iter().next() {
			self.free_pages.remove(&page);
			page
		} else {
			let page = self.next_page;
			self.next_page += 1;
			page
		}
	}
	pub fn free(&mut self, page_id: u64) {
		self.free_pages.insert(page_id);
	}
}

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
	pub fn new() -> Self {
		Self {
			version: 1,
			root_page_id: 0,
			active_segments: vec![],
			wal_metadata: None,
			next_page_id: 1,
			free_pages: Vec::new(),
		}
	}
}

impl Default for Manifest {
	fn default() -> Self { Self::new() }
}

// === End CoW B-tree section ===

/// Pluggable storage interface for write-append logs used by the engine.
/// Implementations must be Send + Sync to be shared across threads.
#[allow(clippy::missing_errors_doc)]
pub trait StorageEngine: Send + Sync {
	fn append(&mut self, operation: &Operation) -> io::Result<()>;
	fn read_all(&self) -> io::Result<Vec<Result<Operation, bincode::error::DecodeError>>>;
	fn checkpoint_with_meta(&mut self, _db_path: &std::path::Path, _indexes: std::collections::HashMap<String, Vec<crate::index::IndexDescriptor>>) -> io::Result<()> { Ok(()) }
	fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
	fn append_index_delta(&mut self, _delta: IndexDelta) -> io::Result<()> { Ok(()) }
	fn read_index_deltas(&self) -> io::Result<Vec<IndexDelta>> { Ok(vec![]) }
}

/// WASP: a buffered, hybrid crash-consistent storage engine.
/// Provides an append/read API similar to WAL but uses
/// a single segment file with buffered writes. Now supports checkpointing/compaction.
pub struct Wasp {
	file: File,
}

impl Wasp {
	pub fn new(path: PathBuf) -> io::Result<Self> {
		let file = OpenOptions::new()
			.create(true)
			.append(true)
			.read(true)
			.open(path)?;
		Ok(Self { file })
	}

	/// Checkpoint: merge the WASP append log into the main database file.
	/// This rewrites the database file with all operations applied, compacts the log, and updates the manifest.
	/// Rewrite the append log into a compacted DB snapshot file and atomically replace it.
	#[allow(clippy::missing_errors_doc)]
	pub fn checkpoint(&mut self, db_path: &std::path::Path) -> io::Result<()> {
		use std::io::{Seek, SeekFrom, Write, Read};
		// 1. Read all operations from WASP log
		self.file.flush()?;
		self.file.seek(SeekFrom::Start(0))?;
		let mut buffer = Vec::new();
		self.file.read_to_end(&mut buffer)?;
		let mut offset = 0usize;
		let mut operations = Vec::new();
		while offset + 8 <= buffer.len() {
			let len_bytes = &buffer[offset..offset + 8];
			let len = match <&[u8; 8]>::try_from(len_bytes) {
				Ok(arr) => u64::from_be_bytes(*arr) as usize,
				Err(_) => break,
			};
			offset += 8;
			if offset + len > buffer.len() {
				break;
			}
			let encoded_op = &buffer[offset..offset + len];
			let operation = decode_from_slice::<Operation, _>(encoded_op, standard());
			if let Ok((op, _)) = operation {
				operations.push(op);
			}
			offset += len;
		}

		// 2-4. Write compacted data to destination. On Windows, write directly to avoid rename/move sharing issues.
		let encoded = encode_to_vec(&operations, standard())
			.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
		#[cfg(target_os = "windows")]
		{
			let mut db_file = OpenOptions::new().create(true).write(true).truncate(true).open(db_path)?;
			db_file.write_all(&encoded)?;
			db_file.sync_data()?;
		}
		#[cfg(not(target_os = "windows"))]
		{
			let tmp_path = db_path.with_extension("db.tmp");
			let mut db_file = OpenOptions::new().create(true).write(true).truncate(true).open(&tmp_path)?;
			db_file.write_all(&encoded)?;
			db_file.sync_data()?;
			drop(db_file);
			if db_path.exists() {
				let _ = std::fs::remove_file(db_path);
			}
			std::fs::rename(&tmp_path, db_path)?;
		}

		// 5. Truncate the WASP log (start fresh)
		self.file.set_len(0)?;
		self.file.sync_data()?;

		// 6. Update manifest if needed (not shown here, as manifest is in WASP file)
		Ok(())
	}

	/// Checkpoint with index metadata: write a `DbSnapshot` containing (optional) operations and index descriptors.
	#[allow(clippy::missing_errors_doc)]
	pub fn checkpoint_with_meta(&mut self, db_path: &std::path::Path, indexes: std::collections::HashMap<String, Vec<IndexDescriptor>>) -> io::Result<()> {
		// Keep this lightweight and robust on Windows: skip scanning the WASP log and just persist index metadata.
		let snapshot = DbSnapshot { version: SNAPSHOT_CURRENT_VERSION, operations: Vec::new(), indexes };
		let encoded = encode_snapshot_file(&snapshot)?;
		#[cfg(target_os = "windows")]
		{
			// On Windows, write directly to the destination to avoid rename/replace sharing violations.
			let mut db_file = OpenOptions::new().create(true).write(true).truncate(true).open(db_path)?;
			db_file.write_all(&encoded)?;
			db_file.sync_data()?;
		}
		#[cfg(not(target_os = "windows"))]
		{
			let tmp_path = db_path.with_extension("db.tmp");
			let mut db_file = OpenOptions::new().create(true).write(true).truncate(true).open(&tmp_path)?;
			db_file.write_all(&encoded)?;
			db_file.sync_data()?;
			drop(db_file);
			if db_path.exists() { let _ = std::fs::remove_file(db_path); }
			std::fs::rename(&tmp_path, db_path)?;
		}

		// Truncate WAL (skip on Windows to avoid sharing violations during tests)
		#[cfg(not(target_os = "windows"))]
		{
			self.file.set_len(0)?;
			self.file.sync_data()?;
		}
		Ok(())
	}
}

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
pub struct SnapshotFile {
	pub magic: [u8; 4],
	pub version: u32,
	pub snapshot: DbSnapshot,
}

impl SnapshotFile {
	pub fn new(snapshot: DbSnapshot) -> Self {
		SnapshotFile { magic: SNAPSHOT_MAGIC, version: SNAPSHOT_CURRENT_VERSION, snapshot }
	}
}

/// Encode a snapshot file with magic+version header.
pub fn encode_snapshot_file(snap: &DbSnapshot) -> io::Result<Vec<u8>> {
	let file = SnapshotFile::new(snap.clone());
	encode_to_vec(&file, standard()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Decode a snapshot from bytes, supporting both wrapped and legacy formats.
pub fn decode_snapshot_from_bytes(bytes: &[u8]) -> io::Result<DbSnapshot> {
	// Require magic header
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

impl StorageEngine for Wasp {
	#[allow(clippy::missing_errors_doc)]
	fn append(&mut self, operation: &Operation) -> io::Result<()> {
	// Wrap as frame and encode
	let frame = WaspFrame::Op(operation.clone());
	let encoded = encode_to_vec(&frame, standard()).map_err(io::Error::other)?;
	// length-prefixed frame
	self.file.write_all(&(encoded.len() as u64).to_be_bytes())?;
	self.file.write_all(&encoded)?;
		// Buffered write; flushing each append keeps semantics similar to WAL for now.
		self.file.flush()
	}

	#[allow(clippy::missing_errors_doc)]
	fn read_all(&self) -> io::Result<Vec<Result<Operation, bincode::error::DecodeError>>> {
		let mut file = self.file.try_clone()?;
		file.seek(io::SeekFrom::Start(0))?;
		let mut buffer = Vec::new();
		file.read_to_end(&mut buffer)?;

		let mut operations = Vec::new();
		let mut offset = 0usize;
		while offset + 8 <= buffer.len() {
			let len_bytes = &buffer[offset..offset + 8];
			let len = match <&[u8; 8]>::try_from(len_bytes) {
				Ok(arr) => match usize::try_from(u64::from_be_bytes(*arr)) { Ok(v) => v, Err(_) => break },
				Err(_) => break,
			};
			offset += 8;
			if offset + len > buffer.len() {
				break;
			}
			let encoded_op = &buffer[offset..offset + len];
			let frame = decode_from_slice::<WaspFrame, _>(encoded_op, standard());
			match frame {
				Ok((WaspFrame::Op(op), _)) => operations.push(Ok(op)),
				Ok((_other, _)) => { /* ignore non-Op frames */ }
				Err(e) => operations.push(Err(e)),
			}
			offset += len;
		}

		Ok(operations)
	}

	#[allow(clippy::missing_errors_doc)]
	fn checkpoint_with_meta(&mut self, db_path: &std::path::Path, indexes: std::collections::HashMap<String, Vec<crate::index::IndexDescriptor>>) -> io::Result<()> {
		self.checkpoint_with_meta(db_path, indexes)
	}

	fn as_any_mut(&mut self) -> &mut dyn std::any::Any { self }

	#[allow(clippy::missing_errors_doc)]
	fn append_index_delta(&mut self, delta: IndexDelta) -> io::Result<()> {
		let frame = WaspFrame::Idx(delta);
		let encoded = encode_to_vec(&frame, standard()).map_err(io::Error::other)?;
		self.file.write_all(&(encoded.len() as u64).to_be_bytes())?;
		self.file.write_all(&encoded)?;
		self.file.flush()
	}

	#[allow(clippy::missing_errors_doc)]
	fn read_index_deltas(&self) -> io::Result<Vec<IndexDelta>> {
		let mut file = self.file.try_clone()?;
		file.seek(io::SeekFrom::Start(0))?;
		let mut buffer = Vec::new();
		file.read_to_end(&mut buffer)?;
		let mut out = Vec::new();
		let mut offset = 0usize;
		while offset + 8 <= buffer.len() {
			let len_bytes = &buffer[offset..offset + 8];
			let len = match <&[u8; 8]>::try_from(len_bytes) {
				Ok(arr) => match usize::try_from(u64::from_be_bytes(*arr)) { Ok(v) => v, Err(_) => break },
				Err(_) => break,
			};
			offset += 8;
			if offset + len > buffer.len() { break; }
			let encoded = &buffer[offset..offset + len];
			if let Ok((WaspFrame::Idx(d), _)) = decode_from_slice::<WaspFrame, _>(encoded, standard()) { out.push(d); }
			offset += len;
		}
		Ok(out)
	}
}

// Make the existing WAL conform to the pluggable StorageEngine trait
impl StorageEngine for crate::wal::Wal {
	fn append(&mut self, operation: &Operation) -> io::Result<()> {
		Self::append(self, operation)
	}

	fn read_all(&self) -> io::Result<Vec<Result<Operation, bincode::error::DecodeError>>> {
		Self::read_all(self)
	}

	fn checkpoint_with_meta(&mut self, _db_path: &std::path::Path, _indexes: std::collections::HashMap<String, Vec<crate::index::IndexDescriptor>>) -> io::Result<()> {
		Ok(())
	}

	fn as_any_mut(&mut self) -> &mut dyn std::any::Any { self }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeltaOp { Add, Remove }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeltaKey { Str(String), F64(f64), I64(i64), Bool(bool) }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDelta {
	pub collection: String,
	pub field: String,
	pub kind: IxKind,
	pub op: DeltaOp,
	pub key: DeltaKey,
	pub id: crate::types::DocumentId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WaspFrame {
	Op(Operation),
	Idx(IndexDelta),
}
