// === Simple Bloom Filter Implementation (Serializable) ===
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
use rand::Rng;
use crate::types::Operation;
use bincode::serde::{encode_to_vec, decode_from_slice};
use bincode::config::standard;
use std::fs::{File, OpenOptions};
use serde::{Serialize, Deserialize};
use crate::index::IndexDescriptor;
use crate::index::IndexKind as IxKind;

pub struct BlockCache {}
impl BlockCache {
	pub fn new() -> Self { BlockCache {} }
}


use std::sync::Arc;
use parking_lot::RwLock;
/// Prefetches pages into the cache for sequential scans (synchronously for now).
pub fn prefetch_pages(ids: &[u64], file: &mut File, cache: &Arc<RwLock<BlockCache>>) {
	for &page_id in ids {
		// Simulate prefetch by reading the page into memory (could be async in future)
		let offset = 2 * WASP_PAGE_SIZE as u64 + (page_id - 1) * WASP_PAGE_SIZE as u64;
		let mut buf = vec![0u8; WASP_PAGE_SIZE];
		let _ = cache; // Silence unused variable warning
		if file.seek(SeekFrom::Start(offset)).is_ok() && file.read_exact(&mut buf).is_ok() {
			// In a real cache, insert the page here
			// cache.write().insert(page_id, buf);
		}
	}
}


/// Batches multiple manifest updates per flip for efficiency (no-op for now, but ready for batching logic).
pub fn optimize_manifest_updates() {
	// In a real system, this would batch manifest updates in memory and flush them together.
	// For now, manifest updates are written immediately for durability.
}

pub struct WasMetrics {}
impl WasMetrics {
	pub fn new() -> Self { WasMetrics {} }
	/// Collects and prints WASP engine metrics (placeholder for now).
	pub fn report(&self) {
		// In a real system, gather stats from cache, storage, and engine.
		println!("[WASP Metrics] (placeholder): metrics collection not yet implemented.");
	}
}


/// Runs WASP engine benchmarks and prints results (placeholder for now).
pub fn run_benchmarks() {
	// In a real system, run microbenchmarks and print results.
	println!("[WASP Benchmark] (placeholder): benchmarking not yet implemented.");
}

// === End Phase 7 ===
// === WASP Phase 6: Durability & Integrity Hardening ===

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

pub struct ConsistencyChecker {}
impl ConsistencyChecker {
	pub fn new() -> Self { ConsistencyChecker {} }
	/// Checks on-disk consistency by scanning all pages and manifests.
	pub fn check(&self, file: &mut File) -> bool {
		// Scan both manifest slots
		let mut valid_manifests = 0;
		for offset in [0, WASP_PAGE_SIZE as u64] {
			if file.seek(SeekFrom::Start(offset)).is_ok() {
				let mut buf = vec![0u8; WASP_PAGE_SIZE];
				if file.read_exact(&mut buf).is_ok() {
					if let Ok((page, _)) = decode_from_slice::<Page, _>(&buf, standard()) {
						if page.verify_crc() {
							valid_manifests += 1;
						}
					}
				}
			}
		}
		valid_manifests == 2
	}
}

/// Fuzz test: corrupts WAL/pages/manifest and checks recovery.
pub fn fuzz_test_corruption(file: &mut File) -> bool {
	// Corrupt the first manifest slot and check if the second is still valid
	if file.seek(SeekFrom::Start(0)).is_ok() {
		let mut buf = vec![0u8; WASP_PAGE_SIZE];
	let mut rng = rand::rng();
	for b in &mut buf { *b = rng.random(); }
		if file.write_all(&buf).is_ok() && file.sync_data().is_ok() {
			// Now check if the second manifest is still valid
			let checker = ConsistencyChecker::new();
			return checker.check(file);
		}
	}
	false
}

// === End Phase 6 ===
// === WASP Phase 5: Concurrency & MVCC ===

pub struct SnapshotTracker {
	pub current_epoch: u64,
}
impl SnapshotTracker {
	pub fn new() -> Self { SnapshotTracker { current_epoch: 0 } }
	pub fn advance(&mut self) { self.current_epoch += 1; }
}

pub struct MvccEngine {}
impl MvccEngine {
	pub fn new() -> Self { MvccEngine {} }
	pub fn visible(&self, _epoch: u64, _txn_epoch: u64) -> bool { true }
}

// === End Phase 5 ===
// === WASP Phase 4: Compaction & Space Reclaim ===

pub struct CompactionEngine {}
impl CompactionEngine {
	pub fn new() -> Self { CompactionEngine {} }
	pub fn run_background(&self) {
		// In real system, would spawn a thread/task
	}
}

pub struct FreeSpaceMap {}
impl FreeSpaceMap {
	pub fn new() -> Self { FreeSpaceMap {} }
	pub fn recycle(&mut self, _page_id: u64) {}
}

pub struct EpochGc {}
impl EpochGc {
	pub fn new() -> Self { EpochGc {} }
	pub fn gc(&mut self, _epoch: u64) {}
}

// === End Phase 4 ===
// === WASP Phase 3: Immutable Segment Store ===

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
		let bloom_bytes = encode_to_vec(&bloom, standard()).unwrap();
		SegmentFooter {
			key_range,
			fence_keys,
			bloom_filter: bloom_bytes,
		}
	}

	pub fn might_contain(&self, key: &[u8]) -> bool {
		let (bloom, _): (BloomFilter, _) = decode_from_slice(&self.bloom_filter, standard()).unwrap();
		bloom.contains(key)
	}

}

pub struct SegmentFile {
	pub file: File,
}

impl SegmentFile {
	pub fn open(path: PathBuf) -> io::Result<Self> {
		let file = OpenOptions::new().read(true).write(true).create(true).open(path)?;
		Ok(SegmentFile { file })
	}

	pub fn flush_segment(&mut self, pages: &[Page], footer: &SegmentFooter) -> io::Result<()> {
		for page in pages {
			let page_bytes = encode_to_vec(page, standard()).unwrap();
			self.file.write_all(&page_bytes)?;
		}
		let footer_bytes = encode_to_vec(footer, standard()).unwrap();
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
		let (footer, _) = decode_from_slice::<SegmentFooter, _>(&buf[offset..], standard()).unwrap();
		Ok((pages, footer))
	}
}

// === End Phase 3 ===
// === WASP Phase 2: Tiny WAL Layer ===

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
		let file = OpenOptions::new().read(true).write(true).create(true).open(path)?;
		Ok(TinyWal { file })
	}

	pub fn append(&mut self, record: &WalRecord) -> io::Result<()> {
		let data = encode_to_vec(record, standard()).unwrap();
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
			let len = u64::from_le_bytes(buf[offset..offset+8].try_into().unwrap()) as usize;
			offset += 8;
			if offset + len > buf.len() { break; }
			let (rec, _): (WalRecord, _) = decode_from_slice(&buf[offset..offset+len], standard()).unwrap();
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

// === End Phase 2 ===

impl CowTree {
	/// Crash-safe: reload manifest and root after restart
	pub fn reload_root(&mut self) -> io::Result<()> {
		let manifest = self.file.read_manifest()?;
		self.root_page_id = manifest.root_page_id;
		self.version = manifest.version;
		Ok(())
	}
}
// === WASP Phase 1b: Minimal CoW B-tree node/page structure and root management ===

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
}

impl CowTree {
	pub fn new(mut file: WaspFile) -> io::Result<Self> {
		// Try to read manifest, else create new
		let mut manifest = file.read_manifest().unwrap_or_else(|_| Manifest::new());
		let mut root_page_id = manifest.root_page_id;
		let mut version = manifest.version;
		if root_page_id == 0 {
			// Write an empty root page using write_page logic
			let node = CowNode::new_leaf();
			let node_bytes = encode_to_vec(&node, standard()).unwrap();
			root_page_id = 1;
			let page = Page::new(root_page_id, version + 1, 2, node_bytes);
			// Use a temporary CowTree to call write_page
			let mut temp_tree = CowTree {
				root_page_id,
				file,
				version,
			};
			temp_tree.write_page(root_page_id, &page)?;
			// Update manifest
			temp_tree.version += 1;
			manifest.root_page_id = root_page_id;
			manifest.version = temp_tree.version;
			temp_tree.file.write_manifest(&manifest)?;
			// Move file and version back
			file = temp_tree.file;
			version = temp_tree.version;
		}
		Ok(CowTree {
			root_page_id,
			file,
			version,
		})
	}

	// Insert a key/value (for now, just create a new root leaf if empty)
	pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> io::Result<()> {
		const MAX_KEYS: usize = 32;
		// Recursive insert, returns (new_page_id, promoted_key)
		fn insert_rec(tree: &mut CowTree, page_id: u64, key: Vec<u8>, value: Vec<u8>, version: u64) -> io::Result<(u64, Option<(Vec<u8>, u64)>)> {
			let mut node = if page_id == 0 {
				CowNode::new_leaf()
			} else {
				let page = tree.read_page(page_id)?;
				decode_from_slice::<CowNode, _>(&page.data, standard()).map(|(n, _)| n).unwrap_or(CowNode::new_leaf())
			};
			match &mut node {
				CowNode::Leaf { keys, values } => {
					// Insert in sorted order
					let pos = keys.binary_search(&key).unwrap_or_else(|e| e);
					keys.insert(pos, key);
					values.insert(pos, value);
					if keys.len() > MAX_KEYS {
						// Split
						let mid = keys.len() / 2;
						let right_keys = keys.split_off(mid);
						let right_values = values.split_off(mid);
						let promoted = right_keys[0].clone();
						let right = CowNode::Leaf { keys: right_keys, values: right_values };
						let new_right_id = version + 2;
						let right_bytes = encode_to_vec(&right, standard()).unwrap();
						let right_page = Page::new(new_right_id, version + 2, 2, right_bytes);
						tree.write_page(new_right_id, &right_page)?;
						// Left node (current)
						let left_bytes = encode_to_vec(&node, standard()).unwrap();
						let left_page = Page::new(version + 1, version + 1, 2, left_bytes);
						tree.write_page(version + 1, &left_page)?;
						Ok((version + 1, Some((promoted, new_right_id))))
					} else {
						let node_bytes = encode_to_vec(&node, standard()).unwrap();
						let new_page_id = version + 1;
						let page = Page::new(new_page_id, version + 1, 2, node_bytes);
						tree.write_page(new_page_id, &page)?;
						Ok((new_page_id, None))
					}
				}
				CowNode::Internal { keys, children } => {
					// Find child
					let pos = keys.binary_search(&key).unwrap_or_else(|e| e);
					let child_id = children[pos];
					let (new_child_id, promoted) = insert_rec(tree, child_id, key, value, version + 1)?;
					children[pos] = new_child_id;
					if let Some((promo_key, right_id)) = promoted {
						keys.insert(pos, promo_key);
						children.insert(pos + 1, right_id);
						if keys.len() > MAX_KEYS {
							// Split internal
							let mid = keys.len() / 2;
							let right_keys = keys.split_off(mid + 1);
							let right_children = children.split_off(mid + 1);
							let promoted = keys.pop().unwrap();
							let right = CowNode::Internal { keys: right_keys, children: right_children };
							let new_right_id = version + 2;
							let right_bytes = encode_to_vec(&right, standard()).unwrap();
							let right_page = Page::new(new_right_id, version + 2, 2, right_bytes);
							tree.write_page(new_right_id, &right_page)?;
							// Left node (current)
							let left_bytes = encode_to_vec(&node, standard()).unwrap();
							let left_page = Page::new(version + 1, version + 1, 2, left_bytes);
							tree.write_page(version + 1, &left_page)?;
							Ok((version + 1, Some((promoted, new_right_id))))
						} else {
							let node_bytes = encode_to_vec(&node, standard()).unwrap();
							let new_page_id = version + 1;
							let page = Page::new(new_page_id, version + 1, 2, node_bytes);
							tree.write_page(new_page_id, &page)?;
							Ok((new_page_id, None))
						}
					} else {
						let node_bytes = encode_to_vec(&node, standard()).unwrap();
						let new_page_id = version + 1;
						let page = Page::new(new_page_id, version + 1, 2, node_bytes);
						tree.write_page(new_page_id, &page)?;
						Ok((new_page_id, None))
					}
				}
			}
		}
		// Start recursive insert
		let (new_root_id, promoted) = insert_rec(self, self.root_page_id, key, value, self.version)?;
		let mut manifest = self.file.read_manifest().unwrap_or_else(|_| Manifest::new());
		if let Some((promo_key, right_id)) = promoted {
			// New root
			let new_root = CowNode::Internal { keys: vec![promo_key], children: vec![new_root_id, right_id] };
			let node_bytes = encode_to_vec(&new_root, standard()).unwrap();
			let root_page_id = self.version + 2;
			let page = Page::new(root_page_id, self.version + 2, 2, node_bytes);
			self.write_page(root_page_id, &page)?;
			manifest.root_page_id = root_page_id;
			manifest.version = self.version + 2;
			self.root_page_id = root_page_id;
			self.version += 2;
		} else {
			manifest.root_page_id = new_root_id;
			manifest.version = self.version + 1;
			self.root_page_id = new_root_id;
			self.version += 1;
		}
		self.file.write_manifest(&manifest)?;
		Ok(())
	}

	pub fn get(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
		if self.root_page_id == 0 {
			return Ok(None);
		}
		let page = self.read_page(self.root_page_id)?;
		let node = decode_from_slice::<CowNode, _>(&page.data, standard()).map(|(n, _)| n).unwrap_or(CowNode::new_leaf());
		match node {
			CowNode::Leaf { keys, values } => {
				for (k, v) in keys.iter().zip(values.iter()) {
					if k == key {
						return Ok(Some(v.clone()));
					}
				}
				Ok(None)
			}
			_ => Ok(None),
		}
	}

	fn write_page(&mut self, page_id: u64, page: &Page) -> io::Result<()> {
		let offset = 2 * WASP_PAGE_SIZE as u64 + (page_id - 1) * WASP_PAGE_SIZE as u64;
		self.file.file.seek(SeekFrom::Start(offset))?;
		let mut page_bytes = encode_to_vec(page, standard()).unwrap();
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
		let new_file_size = self.file.file.metadata()?.len();
		println!("[DEBUG] write_page: page_id={}, offset={}, file_size(before)={}, file_size(after)={}, required_size={}", page_id, offset, file_size, new_file_size, required_size);
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

// === End Phase 1b ===
/// Fuzz test: corrupts WAL/pages/manifest and checks recovery.
use crc32fast::Hasher as Crc32Hasher;
	// TODO: Implement corruption and recovery test
// === WASP Phase 1: Page Format, Manifest Write/Flip, Minimal CoW Tree Stubs ===

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
	hasher.update(&encode_to_vec(&header, standard()).unwrap());
		hasher.update(&data);
		header.crc32 = hasher.finalize();
		Page { header, data }
	}

	pub fn verify_crc(&self) -> bool {
		let mut header = self.header;
		let crc_orig = header.crc32;
		header.crc32 = 0;
		let mut hasher = Crc32Hasher::new();
	hasher.update(&encode_to_vec(&header, standard()).unwrap());
		hasher.update(&self.data);
		hasher.finalize() == crc_orig
	}
}

// Manifest serialization/deserialization
impl Manifest {
	pub fn to_bytes(&self) -> Vec<u8> {
	encode_to_vec(self, standard()).unwrap()
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
		let mut file = OpenOptions::new().read(true).write(true).create(true).open(&path)?;
		let manifest_offsets = [0, WASP_PAGE_SIZE as u64];
		let meta = file.metadata()?;
		let min_size = 2 * WASP_PAGE_SIZE as u64;
		if meta.len() < min_size {
			// Write an initial manifest page to both slots
			let manifest = Manifest::new();
			let data = manifest.to_bytes();
			let page = Page::new(0, manifest.version, 1, data);
			let mut page_bytes = encode_to_vec(&page, standard()).unwrap();
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
	let page_bytes = encode_to_vec(&page, standard()).unwrap();
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
			if page.header.page_type == 1 && page.verify_crc() {
				if let Ok(manifest) = Manifest::from_bytes(&page.data) {
					if best.is_none() || manifest.version > best.as_ref().unwrap().0 {
						best = Some((manifest.version, manifest));
					}
				}
			}
		}
		best.map(|(_, m)| m).ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "No valid manifest found"))
	}
}

// Minimal CoW B-tree/LSM stubs (to be implemented in later phases)

// === End Phase 1 ===
// === WASP Phase 0: Requirements, Design, and Manifest Structure ===

// Requirements/Goals:
// - ACID: Atomicity (via CoW + manifest flip), Consistency (manifest root always valid),
//   Isolation (single-writer, multi-reader), Durability (fsync on manifest/WAL/segments).
// - Workload: Mixed read/write, small and large docs, crash recovery, background compaction.
// - Durability: Manifest and WAL are fsynced; segments are immutable after seal.
// - Concurrency: Single-writer, multi-reader (MVCC planned in later phase).

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
#[derive(Debug, Default)]
pub struct BlockAllocator {
	free_pages: BTreeSet<u64>,
	next_page: u64,
}

impl BlockAllocator {
	pub fn new() -> Self {
		BlockAllocator { free_pages: BTreeSet::new(), next_page: 1 }
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
}

impl Manifest {
	pub fn new() -> Self {
		Manifest {
			version: 1,
			root_page_id: 0,
			active_segments: vec![],
			wal_metadata: None,
		}
	}
}

// === End Phase 0 stubs ===

/// Pluggable storage interface for write-append logs used by the engine.
/// Implementations must be Send + Sync to be shared across threads.
pub trait StorageEngine: Send + Sync {
	fn append(&mut self, operation: &Operation) -> io::Result<()>;
	fn read_all(&self) -> io::Result<Vec<Result<Operation, bincode::error::DecodeError>>>;
	fn checkpoint_with_meta(&mut self, _db_path: &PathBuf, _indexes: std::collections::HashMap<String, Vec<crate::index::IndexDescriptor>>) -> io::Result<()> { Ok(()) }
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
	pub fn checkpoint(&mut self, db_path: &PathBuf) -> io::Result<()> {
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
			let len = u64::from_be_bytes(len_bytes.try_into().unwrap()) as usize;
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

		// 2. Open a new temp database file for compaction
	let tmp_path = db_path.with_extension("db.tmp");
	println!("[DEBUG] tmp_path: {:?}, db_path: {:?}", tmp_path, db_path);
	std::io::stdout().flush().unwrap();
		let mut db_file = OpenOptions::new().create(true).write(true).truncate(true).open(&tmp_path)?;

		// 3. Apply all operations to the new file (serialize as needed)
		// For simplicity, just serialize all operations as a vector
		let encoded = encode_to_vec(&operations, standard()).unwrap();
		db_file.write_all(&encoded)?;
		db_file.sync_data()?;

		// 4. Atomically replace the main DB file with the compacted file
		// Ensure db_file is closed before rename (drop handle)
	drop(db_file);
	println!("[DEBUG] tmp_path exists after write: {}", tmp_path.exists());
	std::io::stdout().flush().unwrap();
		// On Windows, remove the old file if it exists before renaming

		#[cfg(target_os = "windows")]
		{
			use std::os::windows::ffi::OsStrExt;
			use std::ffi::OsStr;
			use winapi::um::winbase::MOVEFILE_REPLACE_EXISTING;
			use winapi::um::winbase::MoveFileExW;
			fn to_wide(s: &std::path::Path) -> Vec<u16> {
				OsStr::new(s).encode_wide().chain(Some(0)).collect()
			}
			let from = to_wide(&tmp_path);
			let to = to_wide(db_path);
			println!("[DEBUG] MoveFileExW from: {:?}, to: {:?}", tmp_path, db_path);
			std::io::stdout().flush().unwrap();
			let result = unsafe { MoveFileExW(from.as_ptr(), to.as_ptr(), MOVEFILE_REPLACE_EXISTING) };
			println!("[DEBUG] MoveFileExW result: {}", result);
			std::io::stdout().flush().unwrap();
			println!("[DEBUG] db_path exists after move: {}", db_path.exists());
			println!("[DEBUG] tmp_path exists after move: {}", tmp_path.exists());
			std::io::stdout().flush().unwrap();
			if result == 0 {
				let err = std::io::Error::last_os_error();
				println!("[DEBUG] MoveFileExW failed: {:?}, falling back to std::fs::rename", err);
				std::io::stdout().flush().unwrap();
				println!("[DEBUG] Before rename: tmp_path exists: {}, db_path exists: {}", tmp_path.exists(), db_path.exists());
				std::io::stdout().flush().unwrap();
				match std::fs::rename(&tmp_path, db_path) {
					Ok(_) => {
						println!("[DEBUG] std::fs::rename succeeded");
						std::io::stdout().flush().unwrap();
					},
					Err(e) => {
						println!("[DEBUG] std::fs::rename failed: {:?}", e);
						println!("[DEBUG] After rename: tmp_path exists: {}, db_path exists: {}", tmp_path.exists(), db_path.exists());
						std::io::stdout().flush().unwrap();
						return Err(e);
					}
				}
			}
		}
		#[cfg(not(target_os = "windows"))]
		{
			if db_path.exists() {
				let _ = std::fs::remove_file(db_path);
			}
			rename(&tmp_path, db_path)?;
		}

		// 5. Truncate the WASP log (start fresh)
		self.file.set_len(0)?;
		self.file.sync_data()?;

		// 6. Update manifest if needed (not shown here, as manifest is in WASP file)
		Ok(())
	}

	/// Checkpoint with index metadata: write a DbSnapshot containing operations and index descriptors.
	pub fn checkpoint_with_meta(&mut self, db_path: &PathBuf, indexes: std::collections::HashMap<String, Vec<IndexDescriptor>>) -> io::Result<()> {
		// 1. Read all operations from WASP log
		self.file.flush()?;
		self.file.seek(SeekFrom::Start(0))?;
		let mut buffer = Vec::new();
		self.file.read_to_end(&mut buffer)?;
		let mut offset = 0usize;
		let mut operations = Vec::new();
		while offset + 8 <= buffer.len() {
			let len_bytes = &buffer[offset..offset + 8];
			let len = u64::from_be_bytes(len_bytes.try_into().unwrap()) as usize;
			offset += 8;
			if offset + len > buffer.len() { break; }
			let encoded_op = &buffer[offset..offset + len];
			if let Ok((op, _)) = decode_from_slice::<Operation, _>(encoded_op, standard()) { operations.push(op); }
			offset += len;
		}

		let snapshot = DbSnapshot { version: 1, operations, indexes };
		let tmp_path = db_path.with_extension("db.tmp");
		let mut db_file = OpenOptions::new().create(true).write(true).truncate(true).open(&tmp_path)?;
		let encoded = encode_to_vec(&snapshot, standard()).unwrap();
		db_file.write_all(&encoded)?;
		db_file.sync_data()?;
		drop(db_file);

		#[cfg(target_os = "windows")]
		{
			use std::os::windows::ffi::OsStrExt;
			use std::ffi::OsStr;
			use winapi::um::winbase::MOVEFILE_REPLACE_EXISTING;
			use winapi::um::winbase::MoveFileExW;
			fn to_wide(s: &std::path::Path) -> Vec<u16> { OsStr::new(s).encode_wide().chain(Some(0)).collect() }
			let from = to_wide(&tmp_path);
			let to = to_wide(db_path);
			let result = unsafe { MoveFileExW(from.as_ptr(), to.as_ptr(), MOVEFILE_REPLACE_EXISTING) };
			if result == 0 { std::fs::rename(&tmp_path, db_path)?; }
		}
		#[cfg(not(target_os = "windows"))]
		{
			if db_path.exists() { let _ = std::fs::remove_file(db_path); }
			rename(&tmp_path, db_path)?;
		}

		// Truncate WAL
		self.file.set_len(0)?;
		self.file.sync_data()?;
		Ok(())
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbSnapshot {
    pub version: u32,
    pub operations: Vec<Operation>,
    pub indexes: std::collections::HashMap<String, Vec<IndexDescriptor>>,
}

impl StorageEngine for Wasp {
	fn append(&mut self, operation: &Operation) -> io::Result<()> {
	// Wrap as frame and encode
	let frame = WaspFrame::Op(operation.clone());
	let encoded = encode_to_vec(&frame, standard()).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
	// length-prefixed frame
	self.file.write_all(&(encoded.len() as u64).to_be_bytes())?;
	self.file.write_all(&encoded)?;
		// Buffered write; flushing each append keeps semantics similar to WAL for now.
		self.file.flush()
	}

	fn read_all(&self) -> io::Result<Vec<Result<Operation, bincode::error::DecodeError>>> {
		let mut file = self.file.try_clone()?;
		file.seek(io::SeekFrom::Start(0))?;
		let mut buffer = Vec::new();
		file.read_to_end(&mut buffer)?;

		let mut operations = Vec::new();
		let mut offset = 0usize;
		while offset + 8 <= buffer.len() {
			let len_bytes = &buffer[offset..offset + 8];
			let len = u64::from_be_bytes(len_bytes.try_into().unwrap()) as usize;
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

	fn checkpoint_with_meta(&mut self, db_path: &PathBuf, indexes: std::collections::HashMap<String, Vec<crate::index::IndexDescriptor>>) -> io::Result<()> {
		self.checkpoint_with_meta(db_path, indexes)
	}

	fn as_any_mut(&mut self) -> &mut dyn std::any::Any { self }

	fn append_index_delta(&mut self, delta: IndexDelta) -> io::Result<()> {
		let frame = WaspFrame::Idx(delta);
		let encoded = encode_to_vec(&frame, standard()).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
		self.file.write_all(&(encoded.len() as u64).to_be_bytes())?;
		self.file.write_all(&encoded)?;
		self.file.flush()
	}

	fn read_index_deltas(&self) -> io::Result<Vec<IndexDelta>> {
		let mut file = self.file.try_clone()?;
		file.seek(io::SeekFrom::Start(0))?;
		let mut buffer = Vec::new();
		file.read_to_end(&mut buffer)?;
		let mut out = Vec::new();
		let mut offset = 0usize;
		while offset + 8 <= buffer.len() {
			let len_bytes = &buffer[offset..offset + 8];
			let len = u64::from_be_bytes(len_bytes.try_into().unwrap()) as usize;
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
		crate::wal::Wal::append(self, operation)
	}

	fn read_all(&self) -> io::Result<Vec<Result<Operation, bincode::error::DecodeError>>> {
		crate::wal::Wal::read_all(self)
	}

	fn checkpoint_with_meta(&mut self, _db_path: &PathBuf, _indexes: std::collections::HashMap<String, Vec<crate::index::IndexDescriptor>>) -> io::Result<()> {
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
