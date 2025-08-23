use crate::collection::Collection;
use crate::index;
use crate::document::Document;
use crate::types::DocumentId;
use bson::{Bson, Document as BsonDocument};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::sync::Arc;
use crate::wasp::{DeltaKey, DeltaOp};
use crate::telemetry;
use crate::errors::DbError;

// Safety limits to prevent resource abuse
const MAX_PATH_DEPTH: usize = 32;
const MAX_IN_SET: usize = 1000;
const MAX_SORT_FIELDS: usize = 8;
const MAX_PROJECTION_FIELDS: usize = 64;
const MAX_LIMIT: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Order { Asc, Desc }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortSpec { pub field: String, pub order: Order }

/// Options for `find_docs`.
///
/// Semantics:
/// - When `projection` is `Some(fields)`, the returned documents contain only those fields.
/// - Sorting is applied before projection.
/// - Results are sliced by `skip`/`limit` with an internal maximum of `MAX_LIMIT`.
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
pub enum CmpOp { Eq, Gt, Gte, Lt, Lte }

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
pub struct UpdateReport { pub matched: u64, pub modified: u64 }

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DeleteReport { pub deleted: u64 }

/// A forward-only cursor over query results.
///
/// Note: When available, this cursor holds the materialized result documents (e.g., after
/// applying projection), avoiding re-fetching from the collection during iteration.
#[derive(Clone)]
pub struct Cursor {
    pub collection: Arc<Collection>,
    pub ids: Vec<DocumentId>,
    pub pos: usize,
    pub docs: Option<Vec<Document>>, // when present, iterate these (e.g., after projection)
}

impl Cursor {
    pub fn advance(&mut self) -> Option<Document> {
        if let Some(ref docs) = self.docs {
            if self.pos >= docs.len() { return None; }
            let d = docs[self.pos].clone();
            self.pos += 1;
            return Some(d);
        }
        if self.pos >= self.ids.len() { return None; }
        let id = self.ids[self.pos].clone();
        self.pos += 1;
        self.collection.find_document(&id)
    }
    #[must_use]
    pub fn to_vec(mut self) -> Vec<Document> {
        if let Some(docs) = self.docs.take() {
            return docs;
        }
        let mut out = Vec::with_capacity(self.ids.len());
        while let Some(d) = self.advance() { out.push(d); }
        out
    }
}

impl Iterator for Cursor {
    type Item = Document;
    fn next(&mut self) -> Option<Self::Item> { self.advance() }
}

// Serde-facing structures for safe JSON parsing of filters/updates
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FilterSerde {
    // Logical
    And { #[serde(rename = "$and")] and: Vec<FilterSerde> },
    Or { #[serde(rename = "$or")] or: Vec<FilterSerde> },
    Not { #[serde(rename = "$not")] not: Box<FilterSerde> },
    // Exists
    Exists { field: String, #[serde(rename = "$exists")] exists: bool },
    // Comparisons and membership
    Cmp {
        field: String,
        #[serde(rename = "$eq")] eq: Box<Option<Bson>>,
        #[serde(rename = "$gt")] gt: Box<Option<Bson>>,
        #[serde(rename = "$gte")] gte: Box<Option<Bson>>,
        #[serde(rename = "$lt")] lt: Box<Option<Bson>>,
        #[serde(rename = "$lte")] lte: Box<Option<Bson>>,
    },
    In { field: String, #[serde(rename = "$in")] in_vals: Vec<Bson> },
    Nin { field: String, #[serde(rename = "$nin")] nin_vals: Vec<Bson> },
    #[cfg(feature = "regex")]
    Regex { field: String, #[serde(rename = "$regex")] pattern: String, #[serde(default)] case_insensitive: bool },
    // Allow the literal true to map to Filter::True
    True(bool),
}

impl TryFrom<FilterSerde> for Filter {
    type Error = crate::errors::DbError;
    fn try_from(fs: FilterSerde) -> Result<Self, Self::Error> {
        use FilterSerde as FS;
        Ok(match fs {
            FS::And { and } => Self::And(and.into_iter().map(Self::try_from).collect::<Result<_, _>>()?),
            FS::Or { or } => Self::Or(or.into_iter().map(Self::try_from).collect::<Result<_, _>>()?),
            FS::Not { not } => Self::Not(Box::new(Self::try_from(*not)?)),
            FS::Exists { field, exists } => Self::Exists { path: field, exists },
            FS::Cmp { field, eq, gt, gte, lt, lte } => {
                if let Some(v) = *eq { Self::Cmp { path: field, op: CmpOp::Eq, value: v } }
                else if let Some(v) = *gt { Self::Cmp { path: field, op: CmpOp::Gt, value: v } }
                else if let Some(v) = *gte { Self::Cmp { path: field, op: CmpOp::Gte, value: v } }
                else if let Some(v) = *lt { Self::Cmp { path: field, op: CmpOp::Lt, value: v } }
                else if let Some(v) = *lte { Self::Cmp { path: field, op: CmpOp::Lte, value: v } }
                else { return Err(crate::errors::DbError::QueryError("No comparison operator provided".into())); }
            }
            FS::In { field, in_vals } => Self::In { path: field, values: in_vals.into_iter().take(MAX_IN_SET).collect() },
            FS::Nin { field, nin_vals } => Self::Nin { path: field, values: nin_vals.into_iter().take(MAX_IN_SET).collect() },
            #[cfg(feature = "regex")]
            FS::Regex { field, pattern, case_insensitive } => Self::Regex { path: field, pattern, case_insensitive },
            FS::True(b) => if b { Self::True } else { Self::Not(Box::new(Self::True)) },
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
    type Error = crate::errors::DbError;
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
                let f = match v { Bson::Int32(i) => f64::from(i), Bson::Int64(i) => i as f64, Bson::Double(d) => d, _ => return Err(crate::errors::DbError::QueryError("$inc requires numeric".into())) };
                out.inc.push((k, f));
            }
        }
    if let Some(unset) = us.unset { out.unset = unset.into_iter().take(128).collect(); }
        Ok(out)
    }
}

/// # Errors
/// Returns an error if the JSON string cannot be parsed into a filter structure.
pub fn parse_filter_json(json: &str) -> Result<Filter, crate::errors::DbError> {
    let fs: FilterSerde = serde_json::from_str(json)?;
    Filter::try_from(fs)
}

/// # Errors
/// Returns an error if the JSON string cannot be parsed into an update structure.
pub fn parse_update_json(json: &str) -> Result<UpdateDoc, crate::errors::DbError> {
    let us: UpdateDocSerde = serde_json::from_str(json)?;
    UpdateDoc::try_from(us)
}

pub fn update_many(col: &Arc<Collection>, filter: &Filter, update: &UpdateDoc) -> UpdateReport {
    let mut matched = 0u64;
    let mut modified = 0u64;
    // Snapshot candidate IDs to avoid cloning entire collection
    let ids: Vec<DocumentId> = col
        .list_ids()
        .into_iter()
        .filter(|id| col.find_document(id).is_some_and(|d| eval_filter(&d.data.0, filter)))
        .collect();
    for id in ids {
        if let Some(mut doc) = col.find_document(&id) {
            matched += 1;
            let changed = apply_update(&mut doc, update);
            if changed { modified += 1; }
            col.update_document(&id, doc);
        }
    }
    UpdateReport { matched, modified }
}

pub fn update_one(col: &Arc<Collection>, filter: &Filter, update: &UpdateDoc) -> UpdateReport {
    // Find first matching ID
    if let Some(id) = col
        .list_ids()
        .into_iter()
        .find(|id| col.find_document(id).is_some_and(|d| eval_filter(&d.data.0, filter)))
        && let Some(mut doc) = col.find_document(&id)
    {
        let changed = apply_update(&mut doc, update);
        col.update_document(&id, doc);
    return UpdateReport { matched: 1, modified: u64::from(changed) };
    }
    UpdateReport { matched: 0, modified: 0 }
}

pub fn delete_many(col: &Arc<Collection>, filter: &Filter) -> DeleteReport {
    let mut deleted = 0u64;
    let ids: Vec<DocumentId> = col
        .list_ids()
        .into_iter()
        .filter(|id| col.find_document(id).is_some_and(|d| eval_filter(&d.data.0, filter)))
        .collect();
    for id in ids {
        if col.delete_document(&id) { deleted += 1; }
    }
    DeleteReport { deleted }
}

pub fn delete_one(col: &Arc<Collection>, filter: &Filter) -> DeleteReport {
    if let Some(id) = col
        .list_ids()
        .into_iter()
        .find(|id| col.find_document(id).is_some_and(|d| eval_filter(&d.data.0, filter)))
    {
    let deleted = u64::from(col.delete_document(&id));
        return DeleteReport { deleted };
    }
    DeleteReport { deleted: 0 }
}

pub fn find_docs(col: &Arc<Collection>, filter: &Filter, opts: &FindOptions) -> Cursor {
    // Basic per-collection rate limit: consume a token; if absent path may be used by CLI/API prechecks.
    let _ = telemetry::try_consume_token(&col.name_str(), 1);
    let deadline = opts.timeout_ms.map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));
    let start_t = std::time::Instant::now();
    let needs_projection = opts.projection.is_some();

    // If no projection is needed, prefer a lazy path accumulating only IDs to avoid cloning many docs.
    if !needs_projection && opts.sort.is_none() {
        // Try to get candidate IDs from index, else all IDs
        let mut ids: Vec<crate::types::DocumentId> =
            plan_index_candidates(col, filter).map_or_else(|| col.list_ids(), |cands| cands);
        // Filter IDs by evaluating on fetched docs lazily
        ids.retain(|id| {
            if let Some(dl) = deadline && std::time::Instant::now() > dl { return false; }
            col.find_document(id).is_some_and(|d| eval_filter(&d.data.0, filter))
        });
    let skip = opts.skip.unwrap_or(0);
    // Enforce global max result size from telemetry (abuse resistance)
    let max_res = telemetry::max_result_limit_for(&col.name_str()).min(MAX_LIMIT);
    let limit = opts.limit.unwrap_or(usize::MAX).min(max_res);
        let end = skip.saturating_add(limit).min(ids.len());
        let sliced: Vec<_> = if skip >= ids.len() { Vec::new() } else { ids[skip..end].to_vec() };
    let dur = start_t.elapsed().as_millis();
    telemetry::log_query(&col.name_str(), filter_type_name(filter), dur, opts.limit, opts.skip, None);
        return Cursor { collection: col.clone(), ids: sliced, pos: 0, docs: None };
    }

    // Otherwise, materialize docs for sorting/projection as before
    let mut docs: Vec<Document> = plan_index_candidates(col, filter)
        .map_or_else(|| col.get_all_documents(), |cands| cands.into_iter().filter_map(|id| col.find_document(&id)).collect());
    docs.retain(|d| {
        if let Some(dl) = deadline && std::time::Instant::now() > dl { return false; }
        eval_filter(&d.data.0, filter)
    });
    if let Some(specs) = &opts.sort {
        let limited_specs: Vec<SortSpec> = specs.iter().take(MAX_SORT_FIELDS).cloned().collect();
        sort_docs(&mut docs, &limited_specs);
    }
    let skip = opts.skip.unwrap_or(0);
    let max_res = telemetry::max_result_limit_for(&col.name_str()).min(MAX_LIMIT);
    let limit = opts.limit.unwrap_or(usize::MAX).min(max_res);
    let end = skip.saturating_add(limit).min(docs.len());
    let slice = if skip >= docs.len() { &docs[0..0] } else { &docs[skip..end] };
    let mut projected: Vec<Document> = slice.to_vec();
    if let Some(fields) = &opts.projection {
        let limited_fields: Vec<String> = fields.iter().take(MAX_PROJECTION_FIELDS).cloned().collect();
        for d in &mut projected {
            d.data.0 = project(&d.data.0, &limited_fields);
        }
    }
    let ids = projected.iter().map(|d| d.id.clone()).collect();
    let dur = start_t.elapsed().as_millis();
    telemetry::log_query(&col.name_str(), filter_type_name(filter), dur, opts.limit, opts.skip, None);
    Cursor { collection: col.clone(), ids, pos: 0, docs: Some(projected) }
}

const fn filter_type_name(f: &Filter) -> &'static str {
    match f {
        Filter::True => "true",
        Filter::And(_) => "$and",
        Filter::Or(_) => "$or",
        Filter::Not(_) => "$not",
        Filter::Exists { .. } => "$exists",
        Filter::In { .. } => "$in",
        Filter::Nin { .. } => "$nin",
        Filter::Cmp { op, .. } => match op { CmpOp::Eq => "$eq", CmpOp::Gt => "$gt", CmpOp::Gte => "$gte", CmpOp::Lt => "$lt", CmpOp::Lte => "$lte" },
        #[cfg(feature = "regex")]
        Filter::Regex { .. } => "$regex",
    }
}

fn plan_index_candidates(col: &Arc<Collection>, filter: &Filter) -> Option<Vec<DocumentId>> {
    match filter {
        Filter::Cmp { path, op, value } => {
            let mut base = {
                let mut mgr = col.indexes.write();
                match op {
                    CmpOp::Eq => index::lookup_eq(&mut mgr, path, value),
                    CmpOp::Gt => index::lookup_range(&mut mgr, path, Some(value), None, false, false),
                    CmpOp::Gte => index::lookup_range(&mut mgr, path, Some(value), None, true, false),
                    CmpOp::Lt => index::lookup_range(&mut mgr, path, None, Some(value), false, false),
                    CmpOp::Lte => index::lookup_range(&mut mgr, path, None, Some(value), false, true),
                }
            };

            // Merge overlay deltas from WASP for this collection/field
            let deltas = col.index_deltas();
            if deltas.is_empty() { return base; }
            let mut set: std::collections::HashSet<DocumentId> = base.take().unwrap_or_default().into_iter().collect();
            let col_name = col.name_str();
            match op {
                CmpOp::Eq => {
                    let key_opt = delta_key_from_bson(value);
                    for d in &deltas {
                        if d.collection == col_name && d.field == *path
                            && let Some(k) = key_opt.as_ref()
                            && delta_key_eq(d.key.clone(), k)
                        {
                            match d.op { DeltaOp::Add => { set.insert(d.id.clone()); }, DeltaOp::Remove => { set.remove(&d.id); } }
                        }
                    }
                }
                CmpOp::Gt | CmpOp::Gte | CmpOp::Lt | CmpOp::Lte => {
                    let (min, max, incl_min, incl_max) = match op {
                        CmpOp::Gt => (Some(value), None, false, false),
                        CmpOp::Gte => (Some(value), None, true, false),
                        CmpOp::Lt => (None, Some(value), false, false),
                        CmpOp::Lte => (None, Some(value), false, true),
                        CmpOp::Eq => unreachable!(),
                    };
                    for d in &deltas {
                        if d.collection == col_name && d.field == *path
                            && delta_key_in_range(&d.key, min, max, incl_min, incl_max)
                        {
                            match d.op { DeltaOp::Add => { set.insert(d.id.clone()); }, DeltaOp::Remove => { set.remove(&d.id); } }
                        }
                    }
                }
            }
            Some(set.into_iter().collect())
        }
        _ => None,
    }
}

fn delta_key_from_bson(v: &Bson) -> Option<DeltaKey> {
    match v {
        Bson::String(s) => Some(DeltaKey::Str(s.clone())),
    Bson::Int32(i) => Some(DeltaKey::I64(i64::from(*i))),
        Bson::Int64(i) => Some(DeltaKey::I64(*i)),
        Bson::Double(f) => Some(DeltaKey::F64(*f)),
        Bson::Boolean(b) => Some(DeltaKey::Bool(*b)),
        _ => None,
    }
}

#[allow(clippy::float_cmp, clippy::cast_precision_loss)]
fn delta_key_eq(a: DeltaKey, b: &DeltaKey) -> bool {
    use DeltaKey as DK;
    match (a, b) {
        (DK::Str(x), DK::Str(y)) => x == *y,
        (DK::Bool(x), DK::Bool(y)) => x == *y,
        (DK::I64(x), DK::I64(y)) => x == *y,
        (DK::F64(x), DK::F64(y)) => x == *y,
        (DK::I64(x), DK::F64(y)) => (x as f64) == *y,
        (DK::F64(x), DK::I64(y)) => x == (*y as f64),
        _ => false,
    }
}

#[allow(clippy::cast_precision_loss)]
fn delta_key_cmp_val(a: &DeltaKey, v: &Bson) -> Option<Ordering> {
    use DeltaKey as DK;
    match (a, v) {
        (DK::Str(x), Bson::String(y)) => Some(x.cmp(y)),
        (DK::Bool(x), Bson::Boolean(y)) => Some(x.cmp(y)),
    (DK::I64(x), Bson::Int32(y)) => Some((*x).cmp(&i64::from(*y))),
        (DK::I64(x), Bson::Int64(y)) => Some(x.cmp(y)),
        (DK::I64(x), Bson::Double(y)) => (*x as f64).partial_cmp(y),
        (DK::F64(x), Bson::Double(y)) => x.partial_cmp(y),
    (DK::F64(x), Bson::Int32(y)) => x.partial_cmp(&f64::from(*y)),
    (DK::F64(x), Bson::Int64(y)) => x.partial_cmp(&(*y as f64)),
        _ => None,
    }
}

fn delta_key_in_range(key: &DeltaKey, min: Option<&Bson>, max: Option<&Bson>, incl_min: bool, incl_max: bool) -> bool {
    if let Some(minv) = min {
        if let Some(ord) = delta_key_cmp_val(key, minv) {
            if ord == Ordering::Less || (!incl_min && ord == Ordering::Equal) { return false; }
        } else { return false; }
    }
    if let Some(maxv) = max {
        if let Some(ord) = delta_key_cmp_val(key, maxv) {
            if ord == Ordering::Greater || (!incl_max && ord == Ordering::Equal) { return false; }
        } else { return false; }
    }
    true
}

pub fn count_docs(col: &Arc<Collection>, filter: &Filter) -> usize {
    let _ = telemetry::try_consume_token(&col.name_str(), 1);
    plan_index_candidates(col, filter).map_or_else(
        || col.list_ids().into_iter().filter_map(|id| col.find_document(&id)).filter(|d| eval_filter(&d.data.0, filter)).count(),
        |cands| cands.into_iter().filter_map(|id| col.find_document(&id)).filter(|d| eval_filter(&d.data.0, filter)).count(),
    )
}

/// Rate-limit aware variants returning explicit errors instead of partial results.
///
/// # Errors
/// Returns `DbError::RateLimitedWithRetry` when the per-collection token bucket would be exceeded.
pub fn find_docs_rate_limited(col: &Arc<Collection>, filter: &Filter, opts: &FindOptions) -> Result<Cursor, DbError> {
    if telemetry::would_limit(&col.name_str(), 1) {
        telemetry::log_rate_limited(&col.name_str(), "find");
    let ra = telemetry::retry_after_ms(&col.name_str(), 1);
    return Err(DbError::RateLimitedWithRetry { retry_after_ms: ra });
    }
    Ok(find_docs(col, filter, opts))
}

/// # Errors
/// Returns `DbError::RateLimitedWithRetry` when the per-collection token bucket would be exceeded.
pub fn count_docs_rate_limited(col: &Arc<Collection>, filter: &Filter) -> Result<usize, DbError> {
    if telemetry::would_limit(&col.name_str(), 1) {
        telemetry::log_rate_limited(&col.name_str(), "count");
    let ra = telemetry::retry_after_ms(&col.name_str(), 1);
    return Err(DbError::RateLimitedWithRetry { retry_after_ms: ra });
    }
    Ok(count_docs(col, filter))
}

#[must_use]
pub fn eval_filter(doc: &BsonDocument, f: &Filter) -> bool {
    match f {
        Filter::True => true,
        Filter::And(v) => v.iter().all(|x| eval_filter(doc, x)),
        Filter::Or(v) => v.iter().any(|x| eval_filter(doc, x)),
        Filter::Not(b) => !eval_filter(doc, b),
        Filter::Exists { path, exists } => get_path(doc, path).is_some() == *exists,
        Filter::In { path, values } => get_path(doc, path)
            .is_some_and(|v| values.iter().take(MAX_IN_SET).any(|x| bson_equal(v, x))),
        Filter::Nin { path, values } => get_path(doc, path)
            .is_none_or(|v| values.iter().take(MAX_IN_SET).all(|x| !bson_equal(v, x))),
        Filter::Cmp { path, op, value } => {
            match (get_path(doc, path), op) {
                (Some(v), CmpOp::Eq) => bson_equal(v, value),
                (Some(v), CmpOp::Gt) => bson_cmp(v, value).is_some_and(|o| o == std::cmp::Ordering::Greater),
                (Some(v), CmpOp::Gte) => bson_cmp(v, value).is_some_and(|o| o != std::cmp::Ordering::Less),
                (Some(v), CmpOp::Lt) => bson_cmp(v, value).is_some_and(|o| o == std::cmp::Ordering::Less),
                (Some(v), CmpOp::Lte) => bson_cmp(v, value).is_some_and(|o| o != std::cmp::Ordering::Greater),
                _ => false,
            }
        }
        #[cfg(feature = "regex")]
        Filter::Regex { path, pattern, case_insensitive } => {
            match get_path(doc, path) {
                Some(Bson::String(s)) => {
                    if pattern.len() > 512 { return false; }
                    let pat = if *case_insensitive { format!("(?i){}", pattern) } else { pattern.clone() };
                    let re = match regex::Regex::new(&pat) { Ok(r) => r, Err(_) => return false };
                    re.is_match(s)
                }
                _ => false,
            }
        }
    }
}

fn get_path<'a>(doc: &'a BsonDocument, path: &str) -> Option<&'a Bson> {
    let mut iter = path.split('.');
    let first = iter.next()?;
    // Enforce path depth limit
    let mut depth = 1usize;
    let mut cur: Option<&Bson> = doc.get(first);
    for part in iter {
        depth += 1;
        if depth > MAX_PATH_DEPTH { return None; }
        match cur {
            Some(Bson::Document(d)) => { cur = d.get(part); }
            _ => return None,
        }
    }
    cur
}

#[allow(clippy::cast_precision_loss)]
fn to_f64(b: &Bson) -> Option<f64> {
    match b {
    Bson::Int32(i) => Some(f64::from(*i)),
    Bson::Int64(i) => Some(*i as f64),
        Bson::Double(f) => Some(*f),
        _ => None,
    }
}

#[allow(clippy::float_cmp, clippy::cast_precision_loss)]
fn bson_equal(a: &Bson, b: &Bson) -> bool {
    match (a, b) {
        (Bson::Int32(x), Bson::Int64(y)) => i64::from(*x) == *y,
        (Bson::Int64(x), Bson::Int32(y)) => *x == i64::from(*y),
        (Bson::Int32(x), Bson::Double(y)) => f64::from(*x) == *y,
        (Bson::Double(x), Bson::Int32(y)) => *x == f64::from(*y),
        (Bson::Int64(x), Bson::Double(y)) => (*x as f64) == *y,
        (Bson::Double(x), Bson::Int64(y)) => *x == (*y as f64),
        _ => a == b,
    }
}

fn bson_cmp(a: &Bson, b: &Bson) -> Option<Ordering> {
    if let (Some(af), Some(bf)) = (to_f64(a), to_f64(b)) { return af.partial_cmp(&bf); }
    match (a, b) {
        (Bson::String(x), Bson::String(y)) => Some(x.cmp(y)),
        (Bson::Boolean(x), Bson::Boolean(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

#[must_use]
fn project(doc: &BsonDocument, fields: &[String]) -> BsonDocument {
    let mut out = BsonDocument::new();
    for f in fields {
        if let Some(v) = get_path(doc, f) { out.insert(f.clone(), v.clone()); }
    }
    out
}

fn sort_docs(docs: &mut [Document], specs: &[SortSpec]) {
    docs.sort_by(|a, b| compare_docs(&a.data.0, &b.data.0, specs));
}

fn compare_docs(a: &BsonDocument, b: &BsonDocument, specs: &[SortSpec]) -> Ordering {
    for s in specs {
        let av = get_path(a, &s.field);
        let bv = get_path(b, &s.field);
        let ord = match (av, bv) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(ax), Some(bx)) => bson_cmp(ax, bx).unwrap_or(Ordering::Equal),
        };
        if ord != Ordering::Equal { return if s.order == Order::Asc { ord } else { ord.reverse() }; }
    }
    Ordering::Equal
}

pub fn apply_update(doc: &mut Document, upd: &UpdateDoc) -> bool {
    let mut modified = false;
    // Enforce caps on number of fields updated per operation to bound work
    for (path, val) in upd.set.iter().take(128) { modified |= set_path(&mut doc.data.0, path, val.clone()); }
    for (path, inc) in upd.inc.iter().take(128) { modified |= inc_path(&mut doc.data.0, path, *inc); }
    for path in upd.unset.iter().take(128) { modified |= unset_path(&mut doc.data.0, path); }
    if modified { doc.metadata.updated_at = crate::types::SerializableDateTime(chrono::Utc::now()); }
    modified
}

fn set_path(doc: &mut BsonDocument, path: &str, val: Bson) -> bool {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.is_empty() { return false; }
    let mut cur = doc;
    for key in parts.iter().take(parts.len() - 1) {
        let key_str: &str = key;
        let need_new = !matches!(cur.get(key_str), Some(Bson::Document(_)));
        if need_new {
            cur.insert(key_str.to_string(), Bson::Document(BsonDocument::new()));
        }
        if let Some(Bson::Document(d)) = cur.get_mut(key_str) { cur = d; } else { return false; }
    }
    let Some(last) = parts.last() else { return false };
    let prev = cur.get(*last).cloned();
    let changed = prev.as_ref().is_none_or(|p| !bson_equal(p, &val));
    cur.insert((*last).to_string(), val);
    changed
}

#[allow(clippy::cast_precision_loss)]
fn inc_path(doc: &mut BsonDocument, path: &str, delta: f64) -> bool {
    let cur_val = get_path(doc, path).cloned();
    let new_val = match cur_val {
    Some(Bson::Int32(i)) => Bson::Double(f64::from(i) + delta),
        Some(Bson::Int64(i)) => Bson::Double(i as f64 + delta),
        Some(Bson::Double(f)) => Bson::Double(f + delta),
        None => Bson::Double(delta),
        _ => return false,
    };
    set_path(doc, path, new_val)
}

fn unset_path(doc: &mut BsonDocument, path: &str) -> bool {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.is_empty() { return false; }
    let mut cur = doc;
    for key in parts.iter().take(parts.len() - 1) {
        let key = *key;
        match cur.get_mut(key) { Some(Bson::Document(d)) => { cur = d; }, _ => return false }
    }
    let Some(last) = parts.last() else { return false };
    cur.remove(*last).is_some()
}
