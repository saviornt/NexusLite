use crate::collection::Collection;
use crate::document::Document;
use crate::errors::DbError;
use crate::types::DocumentId;
use std::sync::Arc;

use super::cursor::Cursor;
use super::eval::{compare_docs, eval_filter, project_fields};
use super::telemetry;
use super::types::{Filter, FindOptions, UpdateDoc, UpdateReport, DeleteReport, CmpOp, MAX_LIMIT, MAX_PROJECTION_FIELDS, MAX_SORT_FIELDS};

pub fn find_docs(col: &Arc<Collection>, filter: &Filter, opts: &FindOptions) -> Cursor {
    let _ = telemetry::try_consume_token(&col.name_str(), 1);
    let deadline = opts.timeout_ms.map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));
    let needs_projection = opts.projection.is_some();

    if !needs_projection && opts.sort.is_none() {
        let mut ids: Vec<DocumentId> = plan_index_candidates(col, filter).map_or_else(|| col.list_ids(), |c| c);
        ids.retain(|id| {
            if let Some(dl) = deadline && std::time::Instant::now() > dl { return false; }
            col.find_document(id).is_some_and(|d| eval_filter(&d.data.0, filter))
        });
        let skip = opts.skip.unwrap_or(0);
        let limit = opts.limit.unwrap_or(usize::MAX).min(MAX_LIMIT);
        let end = (skip + limit).min(ids.len());
        let ids = if skip >= ids.len() { Vec::new() } else { ids[skip..end].to_vec() };
        return Cursor { collection: col.clone(), ids, pos: 0, docs: None };
    }

    let mut docs: Vec<Document> = col
        .list_ids()
        .into_iter()
        .filter_map(|id| col.find_document(&id))
        .filter(|d| eval_filter(&d.data.0, filter))
        .collect();

    if let Some(sort) = &opts.sort { if sort.len() > MAX_SORT_FIELDS { log::warn!("sort spec too long: {}", sort.len()); }
        docs.sort_by(|a, b| compare_docs(&a.data.0, &b.data.0, sort)); }

    if let Some(fields) = &opts.projection { let fields: Vec<String> = fields.iter().take(MAX_PROJECTION_FIELDS).cloned().collect();
        for d in &mut docs { d.data.0 = project_fields(&d.data.0, &fields); } }

    let skip = opts.skip.unwrap_or(0);
    let limit = opts.limit.unwrap_or(usize::MAX).min(MAX_LIMIT);
    let end = (skip + limit).min(docs.len());
    let docs = if skip >= docs.len() { Vec::new() } else { docs[skip..end].to_vec() };
    Cursor { collection: col.clone(), ids: Vec::new(), pos: 0, docs: Some(docs) }
}

pub fn find_docs_rate_limited(
    col: &Arc<Collection>,
    filter: &Filter,
    opts: &FindOptions,
) -> Result<Cursor, DbError> {
    let deadline = opts.timeout_ms.map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));
    if let Some(dl) = deadline && std::time::Instant::now() > dl { return Err(DbError::QueryError("timeout".into())); }
    Ok(find_docs(col, filter, opts))
}

pub fn count_docs_rate_limited(col: &Arc<Collection>, filter: &Filter) -> Result<usize, DbError> {
    let start = std::time::Instant::now();
    let ids = col.list_ids();
    let mut n = 0usize;
    for id in ids {
        if let Some(d) = col.find_document(&id) && eval_filter(&d.data.0, filter) { n += 1; }
        if start.elapsed().as_millis() > 5000 { return Err(DbError::QueryError("timeout".into())); }
    }
    Ok(n)
}

#[must_use]
pub fn count_docs(col: &Arc<Collection>, filter: &Filter) -> usize {
    let ids = col.list_ids();
    let mut n = 0usize;
    for id in ids { if let Some(d) = col.find_document(&id) && eval_filter(&d.data.0, filter) { n += 1; } }
    n
}

pub fn update_many(col: &Arc<Collection>, filter: &Filter, update: &UpdateDoc) -> UpdateReport {
    let mut matched = 0u64; let mut modified = 0u64;
    let ids: Vec<DocumentId> = col
        .list_ids()
        .into_iter()
        .filter(|id| col.find_document(id).is_some_and(|d| eval_filter(&d.data.0, filter)))
        .collect();
    for id in ids { if let Some(mut doc) = col.find_document(&id) { matched += 1; let changed = apply_update(&mut doc, update); if changed { modified += 1; } col.update_document(&id, doc); } }
    UpdateReport { matched, modified }
}

pub fn update_one(col: &Arc<Collection>, filter: &Filter, update: &UpdateDoc) -> UpdateReport {
    if let Some(id) = col
        .list_ids()
        .into_iter()
        .find(|id| col.find_document(id).is_some_and(|d| eval_filter(&d.data.0, filter)))
        && let Some(mut doc) = col.find_document(&id)
    { let changed = apply_update(&mut doc, update); col.update_document(&id, doc); return UpdateReport { matched: 1, modified: u64::from(changed) }; }
    UpdateReport { matched: 0, modified: 0 }
}

pub fn delete_many(col: &Arc<Collection>, filter: &Filter) -> DeleteReport {
    let mut deleted = 0u64; let ids: Vec<DocumentId> = col
        .list_ids()
        .into_iter()
        .filter(|id| col.find_document(id).is_some_and(|d| eval_filter(&d.data.0, filter)))
        .collect();
    for id in ids { if col.delete_document(&id) { deleted += 1; } }
    DeleteReport { deleted }
}

pub fn delete_one(col: &Arc<Collection>, filter: &Filter) -> DeleteReport {
    if let Some(id) = col
        .list_ids()
        .into_iter()
        .find(|id| col.find_document(id).is_some_and(|d| eval_filter(&d.data.0, filter)))
    { let deleted = u64::from(col.delete_document(&id)); return DeleteReport { deleted }; }
    DeleteReport { deleted: 0 }
}

pub fn apply_update(doc: &mut Document, upd: &UpdateDoc) -> bool {
    fn ensure_subdoc<'a>(root: &'a mut bson::Document, key: &str) -> &'a mut bson::Document {
        let needs_new = match root.get_mut(key) { Some(bson::Bson::Document(_)) => false, Some(_) => true, None => true };
        if needs_new { root.insert(key.to_string(), bson::Bson::Document(bson::Document::new())); }
        match root.get_mut(key) { Some(bson::Bson::Document(d)) => d, _ => unreachable!(), }
    }
    fn traverse_to_parent<'a>(root: &'a mut bson::Document, path: &str) -> (&'a mut bson::Document, String) {
        let mut cur = root; let mut iter = path.split('.').peekable(); let mut last = String::new();
        while let Some(seg) = iter.next() { if iter.peek().is_none() { last = seg.to_string(); break; } cur = ensure_subdoc(cur, seg); }
        (cur, last)
    }
    fn set_path(root: &mut bson::Document, path: &str, value: bson::Bson) -> bool { let (parent, last) = traverse_to_parent(root, path); let old = parent.insert(last, value.clone()); old.as_ref() != Some(&value) }
    fn get_path(root: &bson::Document, path: &str) -> Option<bson::Bson> {
        let mut cur = root; let mut iter = path.split('.').peekable();
        while let Some(seg) = iter.next() { if iter.peek().is_none() { return cur.get(seg).cloned(); } match cur.get(seg) { Some(bson::Bson::Document(d)) => { cur = d; } _ => return None, } }
        None
    }
    fn unset_path(root: &mut bson::Document, path: &str) -> bool { let (parent, last) = traverse_to_parent(root, path); parent.remove(&last).is_some() }
    fn as_f64(v: &bson::Bson) -> f64 { match v { bson::Bson::Double(f) => *f, bson::Bson::Int32(i) => *i as f64, bson::Bson::Int64(i) => *i as f64, bson::Bson::Decimal128(d) => d.to_string().parse::<f64>().unwrap_or(0.0), _ => 0.0 } }
    fn inc_path(root: &mut bson::Document, path: &str, by: f64) -> bool { let cur = get_path(root, path).unwrap_or(bson::Bson::Double(0.0)); let newv = bson::Bson::Double(as_f64(&cur) + by); set_path(root, path, newv) }

    let mut changed = false;
    for (k, v) in &upd.set { if set_path(&mut doc.data.0, k, v.clone()) { changed = true; } }
    for (k, inc) in &upd.inc { if inc_path(&mut doc.data.0, k, *inc) { changed = true; } }
    for k in &upd.unset { if unset_path(&mut doc.data.0, k) { changed = true; } }
    changed
}

fn plan_index_candidates(col: &Arc<Collection>, filter: &Filter) -> Option<Vec<DocumentId>> {
    match filter {
        Filter::Cmp { path, op: CmpOp::Eq, value } => {
            let mut mgr = col.indexes.write();
            if mgr.indexes.contains_key(path) { return crate::index::lookup_eq(&mut mgr, path, value); }
            None
        }
        Filter::And(fs) => {
            for f in fs { if let Some(ids) = plan_index_candidates(col, f) { return Some(ids); } }
            None
        }
        _ => None,
    }
}

#[allow(dead_code)]
fn _apply_update_with_deltas(_doc: &mut Document, _upd: &UpdateDoc) -> Vec<(crate::wasp::DeltaKey, crate::wasp::DeltaOp)> { Vec::new() }

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn parse_simple_filter() {
        let j = r#"{"field":"x","$eq":1}"#;
        let f = crate::query::parse_filter_json(j).unwrap();
        assert!(matches!(f, Filter::Cmp { path, .. } if path == "x"));
    }

    #[test]
    fn update_doc_set_inc_unset() {
        let mut d = Document::new(doc! {"x": 1, "y": 2 }, crate::document::DocumentType::Persistent);
        let ud = UpdateDoc { set: vec![("y".into(), bson::Bson::Int32(5))], inc: vec![("x".into(), 2.0)], unset: vec!["z".into()] };
        let changed = super::apply_update(&mut d, &ud);
        assert!(changed);
        assert_eq!(d.data.0.get_i32("y").unwrap(), 5);
        assert_eq!(d.data.0.get_f64("x").unwrap(), 3.0);
    }
}
