use serde::{Deserialize, Serialize};

use crate::index::IndexKind as IxKind;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilter {
    pub bits: Vec<u8>,
    pub k: u8,
}
impl BloomFilter {
    #[must_use]
    pub fn new(size: usize, k: u8) -> Self {
        Self { bits: vec![0; size], k }
    }
    fn hash(&self, key: &[u8], i: u8) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;
        let mut hasher = DefaultHasher::new();
        hasher.write(key);
        hasher.write_u8(i);
    let hash64 = hasher.finish();
    // Perform modulo in u64 domain then narrow; this avoids premature truncation on 32-bit.
    let len = self.bits.len() as u64;
    let idx64 = if len == 0 { 0 } else { hash64 % len };
    crate::utils::num::u64_to_usize(idx64).unwrap_or(0)
    }
    pub fn insert(&mut self, key: &[u8]) {
        for i in 0..self.k {
            let idx = self.hash(key, i);
            self.bits[idx] = 1;
        }
    }
    #[must_use]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeltaOp {
    Add,
    Remove,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeltaKey {
    Str(String),
    F64(f64),
    I64(i64),
    Bool(bool),
}

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
    Op(crate::types::Operation),
    Idx(IndexDelta),
}
