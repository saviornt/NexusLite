use crate::types::DocumentId;
use bson::{Bson, Document as BsonDocument};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexKind {
    Hash,
    BTree,
    // Vector, // TODO: enable when vector index is implemented
}

#[derive(Debug, Clone, Default)]
pub struct IndexStats {
    pub keys: usize,
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    pub build_time_ms: u128,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum IndexKeyKind {
    Str(String),
    F64(OrderedFloat<f64>),
    I64(i64),
    Bool(bool),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EqKey(IndexKeyKind);

impl Hash for EqKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match &self.0 {
            IndexKeyKind::Str(s) => {
                0u8.hash(state);
                s.hash(state);
            }
            IndexKeyKind::F64(f) => {
                1u8.hash(state);
                f.hash(state);
            }
            IndexKeyKind::I64(i) => {
                2u8.hash(state);
                i.hash(state);
            }
            IndexKeyKind::Bool(b) => {
                3u8.hash(state);
                b.hash(state);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct OrdKey(IndexKeyKind);

#[must_use]
pub fn key_from_bson(v: &Bson) -> Option<IndexKeyKind> {
    match v {
        Bson::String(s) => Some(IndexKeyKind::Str(s.clone())),
        Bson::Int32(i) => Some(IndexKeyKind::I64(i64::from(*i))),
        Bson::Int64(i) => Some(IndexKeyKind::I64(*i)),
        Bson::Double(f) => Some(IndexKeyKind::F64(OrderedFloat(*f))),
        Bson::Boolean(b) => Some(IndexKeyKind::Bool(*b)),
        _ => None,
    }
}

fn get_path<'a>(doc: &'a BsonDocument, path: &str) -> Option<&'a Bson> {
    let mut parts = path.split('.');
    let first = parts.next()?;
    let mut cur = doc.get(first)?;
    for p in parts {
        match cur {
            Bson::Document(d) => {
                cur = d.get(p)?;
            }
            _ => return None,
        }
    }
    Some(cur)
}

#[derive(Debug, Clone)]
pub struct HashIndex {
    pub field: String,
    pub map: HashMap<EqKey, HashSet<DocumentId>>,
    pub stats: IndexStats,
}

impl HashIndex {
    #[must_use]
    pub fn new(field: String) -> Self {
        Self { field, map: HashMap::new(), stats: IndexStats::default() }
    }
    pub fn insert(&mut self, doc: &BsonDocument, id: &DocumentId) {
        if let Some(v) = get_path(doc, &self.field)
            && let Some(k) = key_from_bson(v).map(EqKey)
        {
            let set = self.map.entry(k).or_default();
            if set.insert(id.clone()) {
                self.stats.entries += 1;
            }
            self.stats.keys = self.map.len();
        }
    }
    pub fn remove(&mut self, doc: &BsonDocument, id: &DocumentId) {
        if let Some(v) = get_path(doc, &self.field)
            && let Some(k) = key_from_bson(v).map(EqKey)
            && let Some(set) = self.map.get_mut(&k)
        {
            if set.remove(id) {
                self.stats.entries = self.stats.entries.saturating_sub(1);
            }
            if set.is_empty() {
                self.map.remove(&k);
            }
            self.stats.keys = self.map.len();
        }
    }
    pub fn lookup_eq(&mut self, v: &Bson) -> Option<Vec<DocumentId>> {
        if let Some(k) = key_from_bson(v).map(EqKey)
            && let Some(set) = self.map.get(&k)
        {
            self.stats.hits += 1;
            return Some(set.iter().cloned().collect());
        }
        self.stats.misses += 1;
        None
    }
}

#[derive(Debug, Clone)]
pub struct BTreeIndex {
    pub field: String,
    pub map: BTreeMap<OrdKey, BTreeSet<DocumentId>>,
    pub stats: IndexStats,
}

impl BTreeIndex {
    #[must_use]
    pub fn new(field: String) -> Self {
        Self { field, map: BTreeMap::new(), stats: IndexStats::default() }
    }
    pub fn insert(&mut self, doc: &BsonDocument, id: &DocumentId) {
        if let Some(v) = get_path(doc, &self.field)
            && let Some(k) = key_from_bson(v).map(OrdKey)
        {
            let set = self.map.entry(k).or_default();
            if set.insert(id.clone()) {
                self.stats.entries += 1;
            }
            self.stats.keys = self.map.len();
        }
    }
    pub fn remove(&mut self, doc: &BsonDocument, id: &DocumentId) {
        if let Some(v) = get_path(doc, &self.field)
            && let Some(k) = key_from_bson(v).map(OrdKey)
            && let Some(set) = self.map.get_mut(&k)
        {
            if set.remove(id) {
                self.stats.entries = self.stats.entries.saturating_sub(1);
            }
            if set.is_empty() {
                self.map.remove(&k);
            }
            self.stats.keys = self.map.len();
        }
    }
    pub fn lookup_range(
        &mut self,
        min: Option<&Bson>,
        max: Option<&Bson>,
        inclusive_min: bool,
        inclusive_max: bool,
    ) -> Option<Vec<DocumentId>> {
        // Build bounds
        let start = min.and_then(|b| key_from_bson(b).map(OrdKey));
        let end = max.and_then(|b| key_from_bson(b).map(OrdKey));
        let mut out: Vec<DocumentId> = Vec::new();
        let iter: Box<dyn Iterator<Item = (&OrdKey, &BTreeSet<DocumentId>)>> =
            match (start.as_ref(), end.as_ref()) {
                (Some(s), Some(e)) => {
                    if inclusive_min && inclusive_max {
                        Box::new(self.map.range(s.clone()..=e.clone()))
                    } else if inclusive_min {
                        Box::new(self.map.range(s.clone()..e.clone()))
                    } else if inclusive_max {
                        Box::new(self.map.range((
                            std::ops::Bound::Excluded(s.clone()),
                            std::ops::Bound::Included(e.clone()),
                        )))
                    } else {
                        Box::new(self.map.range((
                            std::ops::Bound::Excluded(s.clone()),
                            std::ops::Bound::Excluded(e.clone()),
                        )))
                    }
                }
                (Some(s), None) => {
                    if inclusive_min {
                        Box::new(self.map.range(s.clone()..))
                    } else {
                        Box::new(self.map.range((
                            std::ops::Bound::Excluded(s.clone()),
                            std::ops::Bound::Unbounded,
                        )))
                    }
                }
                (None, Some(e)) => {
                    if inclusive_max {
                        Box::new(self.map.range(..=e.clone()))
                    } else {
                        Box::new(self.map.range(..e.clone()))
                    }
                }
                (None, None) => Box::new(self.map.iter()),
            };
        for (_k, set) in iter {
            out.extend(set.iter().cloned());
        }
        if out.is_empty() {
            self.stats.misses += 1;
            None
        } else {
            self.stats.hits += 1;
            Some(out)
        }
    }
}

#[derive(Debug, Clone)]
pub enum IndexImpl {
    Hash(HashIndex),
    BTree(BTreeIndex),
    // Vector(VectorIndex), // TODO: placeholder for future ANN index
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDescriptor {
    pub field: String,
    pub kind: IndexKind,
}

pub const INDEX_METADATA_VERSION: u32 = 1;

#[derive(Debug, Default)]
pub struct IndexManager {
    pub indexes: HashMap<String, IndexImpl>, // key: field path
}

impl IndexManager {
    #[must_use]
    pub fn new() -> Self {
        Self { indexes: HashMap::new() }
    }
    pub fn create_index(&mut self, field: &str, kind: IndexKind) {
        let idx = match kind {
            IndexKind::Hash => IndexImpl::Hash(HashIndex::new(field.to_string())),
            IndexKind::BTree => IndexImpl::BTree(BTreeIndex::new(field.to_string())),
            // IndexKind::Vector => IndexImpl::Vector(VectorIndex::new(field.to_string())),
        };
        self.indexes.insert(field.to_string(), idx);
    }
    pub fn drop_index(&mut self, field: &str) {
        self.indexes.remove(field);
    }
    #[must_use]
    pub fn descriptors(&self) -> Vec<IndexDescriptor> {
        self.indexes
            .iter()
            .map(|(f, i)| IndexDescriptor {
                field: f.clone(),
                kind: match i {
                    IndexImpl::Hash(_) => IndexKind::Hash,
                    IndexImpl::BTree(_) => IndexKind::BTree,
                    // IndexImpl::Vector(_) => IndexKind::Vector,
                },
            })
            .collect()
    }
}

pub fn index_insert_all(mgr: &mut IndexManager, doc: &BsonDocument, id: &DocumentId) {
    for idx in mgr.indexes.values_mut() {
        match idx {
            IndexImpl::Hash(h) => h.insert(doc, id),
            IndexImpl::BTree(b) => b.insert(doc, id),
            // IndexImpl::Vector(v) => v.insert(doc, id),
        }
    }
}

pub fn index_remove_all(mgr: &mut IndexManager, doc: &BsonDocument, id: &DocumentId) {
    for idx in mgr.indexes.values_mut() {
        match idx {
            IndexImpl::Hash(h) => h.remove(doc, id),
            IndexImpl::BTree(b) => b.remove(doc, id),
            // IndexImpl::Vector(v) => v.remove(doc, id),
        }
    }
}

pub fn lookup_eq(mgr: &mut IndexManager, field: &str, v: &Bson) -> Option<Vec<DocumentId>> {
    match mgr.indexes.get_mut(field) {
        Some(IndexImpl::Hash(h)) => h.lookup_eq(v),
        Some(IndexImpl::BTree(b)) => {
            // Equality via BTree exact bound
            b.lookup_range(Some(v), Some(v), true, true)
        }
        // Some(IndexImpl::Vector(v)) => v.lookup_knn(v),
        _ => None,
    }
}

pub fn lookup_range(
    mgr: &mut IndexManager,
    field: &str,
    min: Option<&Bson>,
    max: Option<&Bson>,
    incl_min: bool,
    incl_max: bool,
) -> Option<Vec<DocumentId>> {
    match mgr.indexes.get_mut(field) {
        Some(IndexImpl::BTree(b)) => b.lookup_range(min, max, incl_min, incl_max),
        // Some(IndexImpl::Vector(v)) => v.lookup_range(min, max, incl_min, incl_max),
        _ => None,
    }
}

// Below is a placeholder for a future approximate nearest neighbor (ANN) vector index.
// It is intentionally commented out to avoid changing public API until ready.
//
// pub struct VectorIndex {
//     pub field: String,
//     // TODO: store vector embeddings and an ANN structure (e.g., HNSW)
// }
// impl VectorIndex {
//     pub fn new(field: String) -> Self { Self { field } }
//     pub fn insert(&mut self, _doc: &BsonDocument, _id: &DocumentId) { /* TODO */ }
//     pub fn remove(&mut self, _doc: &BsonDocument, _id: &DocumentId) { /* TODO */ }
//     pub fn lookup_knn(&mut self, _q: &Bson) -> Option<Vec<DocumentId>> { None }
// }
