use bson::Bson;
use serde::{Deserialize, Serialize};

// Safety limits to prevent resource abuse
pub(crate) const MAX_PATH_DEPTH: usize = 32;
pub(crate) const MAX_IN_SET: usize = 1000;
pub(crate) const MAX_SORT_FIELDS: usize = 8;
pub(crate) const MAX_PROJECTION_FIELDS: usize = 64;
pub(crate) const MAX_LIMIT: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Order {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortSpec {
    pub field: String,
    pub order: Order,
}

/// Options for `find_docs`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FindOptions {
    pub projection: Option<Vec<String>>,
    pub sort: Option<Vec<SortSpec>>,
    pub limit: Option<usize>,
    pub skip: Option<usize>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub enum CmpOp {
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone)]
pub enum Filter {
    True,
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
    Exists { path: String, exists: bool },
    In { path: String, values: Vec<Bson> },
    Nin { path: String, values: Vec<Bson> },
    Cmp { path: String, op: CmpOp, value: Bson },
    #[cfg(feature = "regex")]
    Regex { path: String, pattern: String, case_insensitive: bool },
}

#[derive(Debug, Default, Clone)]
pub struct UpdateDoc {
    pub set: Vec<(String, Bson)>,
    pub inc: Vec<(String, f64)>,
    pub unset: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct UpdateReport {
    pub matched: u64,
    pub modified: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DeleteReport {
    pub deleted: u64,
}
