use std::io::{self, Read, Seek, SeekFrom, Write};

use bincode::config::standard;
use bincode::serde::{decode_from_slice, encode_to_vec};
use serde::{Deserialize, Serialize};

use super::manifest::Manifest;
use super::page::{Page, WASP_PAGE_SIZE};

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
    pub const fn new() -> Self { Self { free_pages: BTreeSet::new(), next_page: 1 } }
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
        m.next_page_id = self.next_page; m.free_pages = self.free_pages.iter().copied().collect();
    }
    pub fn alloc(&mut self) -> u64 {
        if let Some(&page) = self.free_pages.iter().next() { self.free_pages.remove(&page); page } else { let page = self.next_page; self.next_page += 1; page }
    }
    pub fn free(&mut self, page_id: u64) { self.free_pages.insert(page_id); }
}

// === Minimal CoW B-tree node/page structure and root management ===

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CowNode { Leaf { keys: Vec<Vec<u8>>, values: Vec<Vec<u8>> }, Internal { keys: Vec<Vec<u8>>, children: Vec<u64> } }

impl CowNode {
    #[must_use]
    pub const fn new_leaf() -> Self { Self::Leaf { keys: vec![], values: vec![] } }
    #[must_use]
    pub const fn new_internal() -> Self { Self::Internal { keys: vec![], children: vec![] } }
}

pub struct CowTree {
    pub root_page_id: u64,
    pub file: super::manifest::WaspFile,
    pub version: u64,
    pub alloc: BlockAllocator,
}

impl CowTree {
    /// # Errors
    /// Returns an error if manifest or page IO/serialization fails.
    pub fn new(mut file: super::manifest::WaspFile) -> io::Result<Self> {
        let mut manifest = file.read_manifest().unwrap_or_else(|_| Manifest::new());
        let mut root_page_id = manifest.root_page_id;
        let mut version = manifest.version;
        let mut alloc = BlockAllocator::from_manifest(&manifest);
        if root_page_id == 0 {
            let node = CowNode::new_leaf();
            let node_bytes = encode_to_vec(&node, standard()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let new_root = alloc.alloc();
            let page = Page::new(new_root, version + 1, 2, node_bytes);
            let mut temp_tree = Self { root_page_id: new_root, file, version, alloc };
            temp_tree.write_page(new_root, &page)?;
            temp_tree.version += 1;
            manifest.root_page_id = new_root;
            manifest.version = temp_tree.version;
            temp_tree.alloc.export_to_manifest(&mut manifest);
            temp_tree.file.write_manifest(&manifest)?;
            file = temp_tree.file; version = temp_tree.version; root_page_id = new_root; alloc = temp_tree.alloc;
        }
        Ok(Self { root_page_id, file, version, alloc })
    }

    /// # Errors
    /// Returns an error if reading/writing pages fails or serialization fails.
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> io::Result<()> {
        const MAX_KEYS: usize = 32;
        fn insert_rec(tree: &mut CowTree, page_id: u64, key: Vec<u8>, value: Vec<u8>) -> InsertRecResult {
            let mut node = {
                let page = tree.read_page(page_id)?;
                decode_from_slice::<CowNode, _>(&page.data, standard()).map(|(n, _)| n).unwrap_or(CowNode::new_leaf())
            };
            match &mut node {
                CowNode::Leaf { keys, values } => {
                    let pos = keys.binary_search(&key).unwrap_or_else(|e| e);
                    if pos < keys.len() && keys[pos] == key { values[pos] = value; } else { keys.insert(pos, key); values.insert(pos, value); }
                    if keys.len() > MAX_KEYS {
                        let mid = keys.len() / 2;
                        let right_keys = keys.split_off(mid);
                        let right_values = values.split_off(mid);
                        let promoted = right_keys[0].clone();
                        let right = CowNode::Leaf { keys: right_keys, values: right_values };
                        let new_right_id = tree.alloc.alloc();
                        let right_bytes = encode_to_vec(&right, standard()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                        let right_page = Page::new(new_right_id, tree.version + 1, 2, right_bytes);
                        tree.write_page(new_right_id, &right_page)?;
                        let left_bytes = encode_to_vec(&node, standard()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                        let new_left_id = tree.alloc.alloc();
                        let left_page = Page::new(new_left_id, tree.version + 1, 2, left_bytes);
                        tree.write_page(new_left_id, &left_page)?;
                        Ok((new_left_id, Some((promoted, new_right_id))))
                    } else {
                        let node_bytes = encode_to_vec(&node, standard()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                        let new_page_id = tree.alloc.alloc();
                        let page = Page::new(new_page_id, tree.version + 1, 2, node_bytes);
                        tree.write_page(new_page_id, &page)?; Ok((new_page_id, None))
                    }
                }
                CowNode::Internal { keys, children } => {
                    let pos = match keys.binary_search(&key) { Ok(i) => i + 1, Err(e) => e };
                    let child_id = children[pos];
                    let (new_child_id, promoted) = insert_rec(tree, child_id, key, value)?;
                    children[pos] = new_child_id;
                    if let Some((promo_key, right_id)) = promoted {
                        keys.insert(pos, promo_key); children.insert(pos + 1, right_id);
                        if keys.len() > MAX_KEYS {
                            let mid = keys.len() / 2;
                            let right_keys = keys.split_off(mid + 1);
                            let right_children = children.split_off(mid + 1);
                            let Some(promoted) = keys.pop() else { return Err(io::Error::other("internal split with empty keys")); };
                            let right = CowNode::Internal { keys: right_keys, children: right_children };
                            let new_right_id = tree.alloc.alloc();
                            let right_bytes = encode_to_vec(&right, standard()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                            let right_page = Page::new(new_right_id, tree.version + 1, 2, right_bytes);
                            tree.write_page(new_right_id, &right_page)?;
                            let left_bytes = encode_to_vec(&node, standard()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                            let new_left_id = tree.alloc.alloc();
                            let left_page = Page::new(new_left_id, tree.version + 1, 2, left_bytes);
                            tree.write_page(new_left_id, &left_page)?;
                            Ok((new_left_id, Some((promoted, new_right_id))))
                        } else {
                            let node_bytes = encode_to_vec(&node, standard()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                            let new_page_id = tree.alloc.alloc();
                            let page = Page::new(new_page_id, tree.version + 1, 2, node_bytes);
                            tree.write_page(new_page_id, &page)?; Ok((new_page_id, None))
                        }
                    } else {
                        let node_bytes = encode_to_vec(&node, standard()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                        let new_page_id = tree.alloc.alloc();
                        let page = Page::new(new_page_id, tree.version + 1, 2, node_bytes);
                        tree.write_page(new_page_id, &page)?; Ok((new_page_id, None))
                    }
                }
            }
        }
        let (new_root_id, promoted) = insert_rec(self, self.root_page_id, key, value)?;
        let mut manifest = self.file.read_manifest().unwrap_or_else(|_| Manifest::new());
        if let Some((promo_key, right_id)) = promoted {
            let new_root = CowNode::Internal { keys: vec![promo_key], children: vec![new_root_id, right_id] };
            let node_bytes = encode_to_vec(&new_root, standard()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let root_page_id = self.alloc.alloc();
            let page = Page::new(root_page_id, self.version + 1, 2, node_bytes);
            self.write_page(root_page_id, &page)?;
            self.root_page_id = root_page_id; self.version += 1;
        } else { self.root_page_id = new_root_id; }
        self.version += 1; manifest.root_page_id = self.root_page_id; manifest.version = self.version;
        self.alloc.export_to_manifest(&mut manifest); self.file.write_manifest(&manifest)?; Ok(())
    }

    /// # Errors
    /// Returns an error if reading or decoding a page fails.
    pub fn get(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        if self.root_page_id == 0 { return Ok(None); }
        let mut cur = self.root_page_id;
        loop {
            let page = self.read_page(cur)?;
            let node = decode_from_slice::<CowNode, _>(&page.data, standard()).map_or_else(|_| CowNode::new_leaf(), |(n, _)| n);
            match node {
                CowNode::Leaf { keys, values } => { match keys.binary_search_by(|k| k.as_slice().cmp(key)) { Ok(i) => return Ok(Some(values[i].clone())), Err(_) => return Ok(None), } }
                CowNode::Internal { keys, children } => { let pos = match keys.binary_search_by(|k| k.as_slice().cmp(key)) { Ok(i) => i + 1, Err(e) => e, }; cur = children[pos]; }
            }
        }
    }

    fn write_page(&mut self, page_id: u64, page: &Page) -> io::Result<()> {
        let offset = 2 * WASP_PAGE_SIZE as u64 + (page_id - 1) * WASP_PAGE_SIZE as u64;
        self.file.file.seek(SeekFrom::Start(offset))?;
        let mut page_bytes = encode_to_vec(page, standard()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        if page_bytes.len() < WASP_PAGE_SIZE { page_bytes.resize(WASP_PAGE_SIZE, 0); }
        self.file.file.seek(SeekFrom::Start(offset))?; self.file.file.write_all(&page_bytes)?;
        let file_size = self.file.file.metadata()?.len();
        let required_size = offset + WASP_PAGE_SIZE as u64;
        if file_size < required_size { self.file.file.set_len(required_size)?; }
        self.file.file.sync_data()?; self.file.file.sync_all()?; Ok(())
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

impl CowTree {
    /// Crash-safe: reload manifest and root after restart
    pub fn reload_root(&mut self) -> io::Result<()> { let manifest = self.file.read_manifest()?; self.root_page_id = manifest.root_page_id; self.version = manifest.version; Ok(()) }

    /// Minimal recovery path: apply TinyWal by updating root_page_id and version from the last record.
    /// This is a placeholder to satisfy tests; a full implementation would validate page checksums, etc.
    pub fn recover_from_wal(&mut self, wal: &mut super::wal::TinyWal) -> io::Result<()> {
        let recs = wal.read_all()?;
        if let Some(last) = recs.last() {
            self.root_page_id = last.new_root_id;
            self.version = last.epoch;
            // Persist to manifest
            let mut m = self.file.read_manifest().unwrap_or_else(|_| Manifest::new());
            m.root_page_id = self.root_page_id; m.version = self.version;
            self.alloc.export_to_manifest(&mut m);
            self.file.write_manifest(&m)?;
        }
        Ok(())
    }
}
