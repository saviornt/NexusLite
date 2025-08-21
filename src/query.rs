use crate::collection::Collection;
use crate::index;
use crate::document::Document;
use crate::types::DocumentId;
use bson::{Bson, Document as BsonDocument};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::sync::Arc;
use crate::wasp::{DeltaKey, DeltaOp};

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

#[derive(Clone)]
pub struct Cursor {
    pub collection: Arc<Collection>,
    pub ids: Vec<DocumentId>,
    pub pos: usize,
}

impl Cursor {
    pub fn next(&mut self) -> Option<Document> {
        if self.pos >= self.ids.len() { return None; }
        let id = self.ids[self.pos].clone();
        self.pos += 1;
        self.collection.find_document(&id)
    }
    pub fn to_vec(mut self) -> Vec<Document> {
        let mut out = Vec::with_capacity(self.ids.len());
        while let Some(d) = self.next() { out.push(d); }
        out
    }
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
        #[serde(rename = "$eq")] eq: Option<Bson>,
        #[serde(rename = "$gt")] gt: Option<Bson>,
        #[serde(rename = "$gte")] gte: Option<Bson>,
        #[serde(rename = "$lt")] lt: Option<Bson>,
        #[serde(rename = "$lte")] lte: Option<Bson>,
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
            FS::And { and } => Filter::And(and.into_iter().map(Filter::try_from).collect::<Result<_, _>>()?),
            FS::Or { or } => Filter::Or(or.into_iter().map(Filter::try_from).collect::<Result<_, _>>()?),
            FS::Not { not } => Filter::Not(Box::new(Filter::try_from(*not)?)),
            FS::Exists { field, exists } => Filter::Exists { path: field, exists },
            FS::Cmp { field, eq, gt, gte, lt, lte } => {
                if let Some(v) = eq { Filter::Cmp { path: field, op: CmpOp::Eq, value: v } }
                else if let Some(v) = gt { Filter::Cmp { path: field, op: CmpOp::Gt, value: v } }
                else if let Some(v) = gte { Filter::Cmp { path: field, op: CmpOp::Gte, value: v } }
                else if let Some(v) = lt { Filter::Cmp { path: field, op: CmpOp::Lt, value: v } }
                else if let Some(v) = lte { Filter::Cmp { path: field, op: CmpOp::Lte, value: v } }
                else { return Err(crate::errors::DbError::QueryError("No comparison operator provided".into())); }
            }
            FS::In { field, in_vals } => Filter::In { path: field, values: in_vals.into_iter().take(MAX_IN_SET).collect() },
            FS::Nin { field, nin_vals } => Filter::Nin { path: field, values: nin_vals.into_iter().take(MAX_IN_SET).collect() },
            #[cfg(feature = "regex")]
            FS::Regex { field, pattern, case_insensitive } => Filter::Regex { path: field, pattern, case_insensitive },
            FS::True(b) => if b { Filter::True } else { Filter::Not(Box::new(Filter::True)) },
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
        let mut out = UpdateDoc::default();
    if let Some(setd) = us.set {
            for (k, v) in setd.into_iter().take(128) {
                out.set.push((k, v));
            }
        }
    if let Some(incd) = us.inc {
            for (k, v) in incd.into_iter().take(128) {
                let f = match v { Bson::Int32(i) => i as f64, Bson::Int64(i) => i as f64, Bson::Double(d) => d, _ => return Err(crate::errors::DbError::QueryError("$inc requires numeric".into())) };
                out.inc.push((k, f));
            }
        }
    if let Some(unset) = us.unset { out.unset = unset.into_iter().take(128).collect(); }
        Ok(out)
    }
}

pub fn parse_filter_json(json: &str) -> Result<Filter, crate::errors::DbError> {
    let fs: FilterSerde = serde_json::from_str(json)?;
    Filter::try_from(fs)
}

pub fn parse_update_json(json: &str) -> Result<UpdateDoc, crate::errors::DbError> {
    let us: UpdateDocSerde = serde_json::from_str(json)?;
    UpdateDoc::try_from(us)
}

pub fn update_many(col: &Arc<Collection>, filter: &Filter, update: &UpdateDoc) -> UpdateReport {
    let mut matched = 0u64;
    let mut modified = 0u64;
    // Snapshot IDs to avoid holding locks during updates
    let ids: Vec<DocumentId> = col
        .get_all_documents()
        .into_iter()
        .filter(|d| eval_filter(&d.data.0, filter))
        .map(|d| d.id)
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
        .get_all_documents()
        .into_iter()
        .find(|d| eval_filter(&d.data.0, filter))
        .map(|d| d.id)
    {
        if let Some(mut doc) = col.find_document(&id) {
            let changed = apply_update(&mut doc, update);
            col.update_document(&id, doc);
            return UpdateReport { matched: 1, modified: if changed { 1 } else { 0 } };
        }
    }
    UpdateReport { matched: 0, modified: 0 }
}

pub fn delete_many(col: &Arc<Collection>, filter: &Filter) -> DeleteReport {
    let mut deleted = 0u64;
    let ids: Vec<DocumentId> = col
        .get_all_documents()
        .into_iter()
        .filter(|d| eval_filter(&d.data.0, filter))
        .map(|d| d.id)
        .collect();
    for id in ids {
        if col.delete_document(&id) { deleted += 1; }
    }
    DeleteReport { deleted }
}

pub fn delete_one(col: &Arc<Collection>, filter: &Filter) -> DeleteReport {
    if let Some(id) = col
        .get_all_documents()
        .into_iter()
        .find(|d| eval_filter(&d.data.0, filter))
        .map(|d| d.id)
    {
        let deleted = if col.delete_document(&id) { 1 } else { 0 };
        return DeleteReport { deleted };
    }
    DeleteReport { deleted: 0 }
}

pub fn find_docs(col: &Arc<Collection>, filter: &Filter, opts: &FindOptions) -> Cursor {
    let deadline = opts.timeout_ms.map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));
    // Planner: try to use a single-field index for simple equality/range, else full scan
    let mut docs: Vec<Document> = if let Some(cands) = plan_index_candidates(col, filter) {
        // Materialize candidate docs
        cands.into_iter().filter_map(|id| col.find_document(&id)).collect()
    } else {
        col.get_all_documents()
    };
    docs.retain(|d| {
        if let Some(dl) = deadline { if std::time::Instant::now() > dl { return false; } }
        eval_filter(&d.data.0, filter)
    });
    if let Some(specs) = &opts.sort {
        // Enforce max sort fields
        let mut limited_specs: Vec<SortSpec> = specs.iter().cloned().take(MAX_SORT_FIELDS).collect();
        sort_docs(&mut docs, &mut limited_specs);
    }
    let skip = opts.skip.unwrap_or(0);
    let limit = opts.limit.unwrap_or(usize::MAX).min(MAX_LIMIT);
    let end = skip.saturating_add(limit).min(docs.len());
    let slice = if skip >= docs.len() { &docs[0..0] } else { &docs[skip..end] };
    let mut projected: Vec<Document> = slice.iter().cloned().collect();
    if let Some(fields) = &opts.projection {
        // Enforce max projection fields
        let limited_fields: Vec<String> = fields.iter().cloned().take(MAX_PROJECTION_FIELDS).collect();
        for d in &mut projected {
            d.data.0 = project(&d.data.0, &limited_fields).unwrap_or_else(|| d.data.0.clone());
        }
    }
    let ids = projected.iter().map(|d| d.id.clone()).collect();
    Cursor { collection: col.clone(), ids, pos: 0 }
}

fn plan_index_candidates(col: &Arc<Collection>, filter: &Filter) -> Option<Vec<DocumentId>> {
    match filter {
        Filter::Cmp { path, op, value } => {
            let mut mgr = col.indexes.write();
            let mut base = match op {
                CmpOp::Eq => index::lookup_eq(&mut mgr, path, value),
                CmpOp::Gt => index::lookup_range(&mut mgr, path, Some(value), None, false, false),
                CmpOp::Gte => index::lookup_range(&mut mgr, path, Some(value), None, true, false),
                CmpOp::Lt => index::lookup_range(&mut mgr, path, None, Some(value), false, false),
                CmpOp::Lte => index::lookup_range(&mut mgr, path, None, Some(value), false, true),
            };

            // Merge overlay deltas from WASP for this collection/field
            let deltas = col.index_deltas();
            if deltas.is_empty() { return base; }
            use std::collections::HashSet;
            let mut set: HashSet<DocumentId> = base.take().unwrap_or_default().into_iter().collect();
            let col_name = col.name_str();
            match op {
                CmpOp::Eq => {
                    let key_opt = delta_key_from_bson(value);
                    for d in deltas.iter() {
                        if d.collection == col_name && d.field == *path {
                            if let Some(k) = key_opt.as_ref() {
                                if delta_key_eq(d.key.clone(), k) {
                                    match d.op { DeltaOp::Add => { set.insert(d.id.clone()); }, DeltaOp::Remove => { set.remove(&d.id); } }
                                }
                            }
                        }
                    }
                }
                CmpOp::Gt | CmpOp::Gte | CmpOp::Lt | CmpOp::Lte => {
                    let (min, max, incl_min, incl_max) = match op {
                        CmpOp::Gt => (Some(value), None, false, false),
                        CmpOp::Gte => (Some(value), None, true, false),
                        CmpOp::Lt => (None, Some(value), false, false),
                        CmpOp::Lte => (None, Some(value), false, true),
                        _ => (None, None, false, false),
                    };
                    for d in deltas.iter() {
                        if d.collection == col_name && d.field == *path {
                            if delta_key_in_range(&d.key, min, max, incl_min, incl_max) {
                                match d.op { DeltaOp::Add => { set.insert(d.id.clone()); }, DeltaOp::Remove => { set.remove(&d.id); } }
                            }
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
        Bson::Int32(i) => Some(DeltaKey::I64(*i as i64)),
        Bson::Int64(i) => Some(DeltaKey::I64(*i)),
        Bson::Double(f) => Some(DeltaKey::F64(*f)),
        Bson::Boolean(b) => Some(DeltaKey::Bool(*b)),
        _ => None,
    }
}

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

fn delta_key_cmp_val(a: &DeltaKey, v: &Bson) -> Option<Ordering> {
    use DeltaKey as DK;
    match (a, v) {
        (DK::Str(x), Bson::String(y)) => Some(x.cmp(y)),
        (DK::Bool(x), Bson::Boolean(y)) => Some(x.cmp(y)),
    (DK::I64(x), Bson::Int32(y)) => Some((*x as i64).cmp(&(*y as i64))),
        (DK::I64(x), Bson::Int64(y)) => Some(x.cmp(y)),
        (DK::I64(x), Bson::Double(y)) => (*x as f64).partial_cmp(y),
        (DK::F64(x), Bson::Double(y)) => x.partial_cmp(y),
        (DK::F64(x), Bson::Int32(y)) => x.partial_cmp(&(*y as f64)),
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
    if let Some(cands) = plan_index_candidates(col, filter) {
        cands.into_iter().filter_map(|id| col.find_document(&id)).filter(|d| eval_filter(&d.data.0, filter)).count()
    } else {
        col.get_all_documents().into_iter().filter(|d| eval_filter(&d.data.0, filter)).count()
    }
}

pub fn eval_filter(doc: &BsonDocument, f: &Filter) -> bool {
    match f {
        Filter::True => true,
        Filter::And(v) => v.iter().all(|x| eval_filter(doc, x)),
        Filter::Or(v) => v.iter().any(|x| eval_filter(doc, x)),
        Filter::Not(b) => !eval_filter(doc, b),
        Filter::Exists { path, exists } => get_path(doc, path).is_some() == *exists,
        Filter::In { path, values } => {
            if let Some(v) = get_path(doc, path) {
                values.iter().take(MAX_IN_SET).any(|x| bson_equal(v, x))
            } else { false }
        }
        Filter::Nin { path, values } => {
            if let Some(v) = get_path(doc, path) {
                values.iter().take(MAX_IN_SET).all(|x| !bson_equal(v, x))
            } else { true }
        }
        Filter::Cmp { path, op, value } => {
            match (get_path(doc, path), op) {
                (Some(v), CmpOp::Eq) => bson_equal(v, value),
                (Some(v), CmpOp::Gt) => bson_cmp(v, value).map(|o| o == std::cmp::Ordering::Greater).unwrap_or(false),
                (Some(v), CmpOp::Gte) => bson_cmp(v, value).map(|o| o != std::cmp::Ordering::Less).unwrap_or(false),
                (Some(v), CmpOp::Lt) => bson_cmp(v, value).map(|o| o == std::cmp::Ordering::Less).unwrap_or(false),
                (Some(v), CmpOp::Lte) => bson_cmp(v, value).map(|o| o != std::cmp::Ordering::Greater).unwrap_or(false),
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

fn to_f64(b: &Bson) -> Option<f64> {
    match b {
        Bson::Int32(i) => Some(*i as f64),
        Bson::Int64(i) => Some(*i as f64),
        Bson::Double(f) => Some(*f),
        _ => None,
    }
}

fn bson_equal(a: &Bson, b: &Bson) -> bool { match (a, b) { (Bson::Int32(x), Bson::Int64(y)) => *x as i64 == *y, (Bson::Int64(x), Bson::Int32(y)) => *x == *y as i64, (Bson::Int32(x), Bson::Double(y)) => (*x as f64) == *y, (Bson::Double(x), Bson::Int32(y)) => *x == (*y as f64), (Bson::Int64(x), Bson::Double(y)) => (*x as f64) == *y, (Bson::Double(x), Bson::Int64(y)) => *x == (*y as f64), _ => a == b } }

fn bson_cmp(a: &Bson, b: &Bson) -> Option<Ordering> {
    if let (Some(af), Some(bf)) = (to_f64(a), to_f64(b)) { return af.partial_cmp(&bf); }
    match (a, b) {
        (Bson::String(x), Bson::String(y)) => Some(x.cmp(y)),
        (Bson::Boolean(x), Bson::Boolean(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

fn project(doc: &BsonDocument, fields: &Vec<String>) -> Option<BsonDocument> {
    let mut out = BsonDocument::new();
    for f in fields {
        if let Some(v) = get_path(doc, f) { out.insert(f.clone(), v.clone()); }
    }
    Some(out)
}

fn sort_docs(docs: &mut [Document], specs: &Vec<SortSpec>) {
    docs.sort_by(|a, b| compare_docs(&a.data.0, &b.data.0, specs));
}

fn compare_docs(a: &BsonDocument, b: &BsonDocument, specs: &Vec<SortSpec>) -> Ordering {
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
    for i in 0..parts.len() - 1 {
        let key = parts[i].to_string();
        let need_new = match cur.get_mut(&key) {
            Some(Bson::Document(_)) => false,
            Some(_) => true,
            None => true,
        };
        if need_new {
            cur.insert(key.clone(), Bson::Document(BsonDocument::new()));
        }
        let child = cur.get_mut(&key).expect("inserted or existed document");
        if let Bson::Document(d) = child { cur = d; } else { return false; }
    }
    let last = parts.last().unwrap();
    let prev = cur.get(*last).cloned();
    let changed = match prev.as_ref() { Some(p) => !bson_equal(p, &val), None => true };
    cur.insert((*last).to_string(), val);
    changed
}

fn inc_path(doc: &mut BsonDocument, path: &str, delta: f64) -> bool {
    let cur_val = get_path(doc, path).cloned();
    let new_val = match cur_val {
        Some(Bson::Int32(i)) => Bson::Double(i as f64 + delta),
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
    for i in 0..parts.len() - 1 {
        let key = parts[i];
        match cur.get_mut(key) { Some(Bson::Document(d)) => { cur = d; }, _ => return false }
    }
    let last = parts.last().unwrap();
    cur.remove(*last).is_some()
}
