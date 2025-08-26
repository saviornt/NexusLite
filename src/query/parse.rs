use crate::errors::DbError;
use bson::Bson;
use serde::{Deserialize, Serialize};

use super::types::{CmpOp, Filter, MAX_IN_SET, UpdateDoc};

// Serde-facing structures for safe JSON parsing of filters/updates
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FilterSerde {
    And {
        #[serde(rename = "$and")]
        and: Vec<FilterSerde>,
    },
    Or {
        #[serde(rename = "$or")]
        or: Vec<FilterSerde>,
    },
    Not {
        #[serde(rename = "$not")]
        not: Box<FilterSerde>,
    },
    Exists {
        field: String,
        #[serde(rename = "$exists")]
        exists: bool,
    },
    Cmp {
        field: String,
        #[serde(rename = "$eq")]
        eq: Box<Option<Bson>>,
        #[serde(rename = "$gt")]
        gt: Box<Option<Bson>>,
        #[serde(rename = "$gte")]
        gte: Box<Option<Bson>>,
        #[serde(rename = "$lt")]
        lt: Box<Option<Bson>>,
        #[serde(rename = "$lte")]
        lte: Box<Option<Bson>>,
    },
    In {
        field: String,
        #[serde(rename = "$in")]
        in_vals: Vec<Bson>,
    },
    Nin {
        field: String,
        #[serde(rename = "$nin")]
        nin_vals: Vec<Bson>,
    },
    #[cfg(feature = "regex")]
    Regex {
        field: String,
        #[serde(rename = "$regex")]
        pattern: String,
        #[serde(default)]
        case_insensitive: bool,
    },
    True(bool),
}

impl TryFrom<FilterSerde> for Filter {
    type Error = DbError;
    fn try_from(fs: FilterSerde) -> Result<Self, Self::Error> {
        use FilterSerde as FS;
        Ok(match fs {
            FS::And { and } => {
                Self::And(and.into_iter().map(Self::try_from).collect::<Result<_, _>>()?)
            }
            FS::Or { or } => {
                Self::Or(or.into_iter().map(Self::try_from).collect::<Result<_, _>>()?)
            }
            FS::Not { not } => Self::Not(Box::new(Self::try_from(*not)?)),
            FS::Exists { field, exists } => Self::Exists { path: field, exists },
            FS::Cmp { field, eq, gt, gte, lt, lte } => {
                if let Some(v) = *eq {
                    Self::Cmp { path: field, op: CmpOp::Eq, value: v }
                } else if let Some(v) = *gt {
                    Self::Cmp { path: field, op: CmpOp::Gt, value: v }
                } else if let Some(v) = *gte {
                    Self::Cmp { path: field, op: CmpOp::Gte, value: v }
                } else if let Some(v) = *lt {
                    Self::Cmp { path: field, op: CmpOp::Lt, value: v }
                } else if let Some(v) = *lte {
                    Self::Cmp { path: field, op: CmpOp::Lte, value: v }
                } else {
                    return Err(DbError::QueryError("No comparison operator provided".into()));
                }
            }
            FS::In { field, in_vals } => {
                Self::In { path: field, values: in_vals.into_iter().take(MAX_IN_SET).collect() }
            }
            FS::Nin { field, nin_vals } => {
                Self::Nin { path: field, values: nin_vals.into_iter().take(MAX_IN_SET).collect() }
            }
            #[cfg(feature = "regex")]
            FS::Regex { field, pattern, case_insensitive } => {
                Self::Regex { path: field, pattern, case_insensitive }
            }
            FS::True(b) => {
                if b {
                    Self::True
                } else {
                    Self::Not(Box::new(Self::True))
                }
            }
        })
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct UpdateDocSerde {
    #[serde(default, rename = "$set")]
    pub set: Option<bson::Document>,
    #[serde(default, rename = "$inc")]
    pub inc: Option<bson::Document>,
    #[serde(default, rename = "$unset")]
    pub unset: Option<Vec<String>>,
}

impl TryFrom<UpdateDocSerde> for UpdateDoc {
    type Error = DbError;
    fn try_from(us: UpdateDocSerde) -> Result<Self, Self::Error> {
        let mut out = Self::default();
        if let Some(setd) = us.set {
            for (k, v) in setd.into_iter().take(128) {
                out.set.push((k, v));
            }
        }
        if let Some(incd) = us.inc {
            for (k, v) in incd.into_iter().take(128) {
                #[allow(clippy::cast_precision_loss)]
                let f = match v {
                    Bson::Int32(i) => f64::from(i),
                    Bson::Int64(i) => i as f64,
                    Bson::Double(d) => d,
                    _ => {
                        return Err(DbError::QueryError("$inc requires numeric".into()));
                    }
                };
                out.inc.push((k, f));
            }
        }
        if let Some(unset) = us.unset {
            out.unset = unset.into_iter().take(128).collect();
        }
        Ok(out)
    }
}

/// # Errors
/// Returns an error if the JSON string cannot be parsed into a filter structure.
pub fn parse_filter_json(json: &str) -> Result<Filter, DbError> {
    let fs: FilterSerde = serde_json::from_str(json)?;
    Filter::try_from(fs)
}

/// # Errors
/// Returns an error if the JSON string cannot be parsed into an update structure.
pub fn parse_update_json(json: &str) -> Result<UpdateDoc, DbError> {
    let us: UpdateDocSerde = serde_json::from_str(json)?;
    UpdateDoc::try_from(us)
}
