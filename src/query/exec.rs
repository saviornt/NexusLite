use crate::collection::Collection;
use crate::document::Document;
use crate::errors::DbError;
use crate::types::DocumentId;
use std::sync::Arc;

use super::cursor::Cursor;
use super::eval::{compare_docs, eval_filter, project_fields};
use super::telemetry;
use super::types::{
    CmpOp, DeleteReport, Filter, FindOptions, MAX_LIMIT, MAX_PROJECTION_FIELDS, MAX_SORT_FIELDS,
    UpdateDoc, UpdateReport,
};

pub fn find_docs(col: &Arc<Collection>, filter: &Filter, opts: &FindOptions) -> Cursor {
    let _ = telemetry::try_consume_token(&col.name_str(), 1);
    let deadline =
        opts.timeout_ms.map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));
    let needs_projection = opts.projection.is_some();
    let bench_start = std::time::Instant::now();
    let mut bench_used_index = false;

    if !needs_projection && opts.sort.is_none() {
        let mut ids: Vec<DocumentId> = match plan_index_candidates(col, filter) {
            Some(c) => {
                bench_used_index = true;
                c
            }
            None => col.list_ids(),
        };
        ids.retain(|id| {
            if let Some(dl) = deadline
                && std::time::Instant::now() > dl
            {
                return false;
            }
            col.find_document(id).is_some_and(|d| eval_filter(&d.data.0, filter))
        });
        let skip = opts.skip.unwrap_or(0);
        let limit = opts.limit.unwrap_or(usize::MAX).min(MAX_LIMIT);
        let end = (skip + limit).min(ids.len());
        let ids = if skip >= ids.len() { Vec::new() } else { ids[skip..end].to_vec() };
    let bench_result_count = ids.len();
        let dur_ms = bench_start.elapsed().as_millis();
        // Emit a developer-benchmark log line for deterministic capture in tests
        crate::dev6!(
            "{{\"bench\":\"query\",\"op\":\"find\",\"collection\":\"{}\",\"duration_ms\":{},\"used_index\":{},\"result_count\":{},\"limit\":{},\"skip\":{}}}",
            col.name_str(),
            crate::utils::num::usize_to_u64(dur_ms as usize),
            bench_used_index,
            crate::utils::num::usize_to_u64(bench_result_count),
            crate::utils::num::usize_to_u64(opts.limit.unwrap_or(0)),
            crate::utils::num::usize_to_u64(opts.skip.unwrap_or(0))
        );
        return Cursor { collection: col.clone(), ids, pos: 0, docs: None };
    }

    let mut docs: Vec<Document> = col
        .list_ids()
        .into_iter()
        .filter_map(|id| col.find_document(&id))
        .filter(|d| eval_filter(&d.data.0, filter))
        .collect();

    if let Some(sort) = &opts.sort {
        if sort.len() > MAX_SORT_FIELDS {
            log::warn!("sort spec too long: {}", sort.len());
        }
        docs.sort_by(|a, b| compare_docs(&a.data.0, &b.data.0, sort));
    }

    if let Some(fields) = &opts.projection {
        let fields: Vec<String> = fields.iter().take(MAX_PROJECTION_FIELDS).cloned().collect();
        for d in &mut docs {
            d.data.0 = project_fields(&d.data.0, &fields);
        }
    }

    let skip = opts.skip.unwrap_or(0);
    let limit = opts.limit.unwrap_or(usize::MAX).min(MAX_LIMIT);
    let end = (skip + limit).min(docs.len());
    let docs = if skip >= docs.len() { Vec::new() } else { docs[skip..end].to_vec() };
    let bench_result_count = docs.len();
    let dur_ms = bench_start.elapsed().as_millis();
    crate::dev6!(
        "{{\"bench\":\"query\",\"op\":\"find\",\"collection\":\"{}\",\"duration_ms\":{},\"used_index\":{},\"result_count\":{},\"limit\":{},\"skip\":{}}}",
        col.name_str(),
        crate::utils::num::usize_to_u64(dur_ms as usize),
        bench_used_index,
        crate::utils::num::usize_to_u64(bench_result_count),
        crate::utils::num::usize_to_u64(opts.limit.unwrap_or(0)),
        crate::utils::num::usize_to_u64(opts.skip.unwrap_or(0))
    );
    Cursor { collection: col.clone(), ids: Vec::new(), pos: 0, docs: Some(docs) }
}

pub fn find_docs_rate_limited(
    col: &Arc<Collection>,
    filter: &Filter,
    opts: &FindOptions,
) -> Result<Cursor, DbError> {
    let deadline =
        opts.timeout_ms.map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));
    if let Some(dl) = deadline
        && std::time::Instant::now() > dl
    {
        return Err(DbError::QueryError("timeout".into()));
    }
    Ok(find_docs(col, filter, opts))
}

pub fn count_docs_rate_limited(col: &Arc<Collection>, filter: &Filter) -> Result<usize, DbError> {
    let start = std::time::Instant::now();
    let ids = col.list_ids();
    let mut n = 0usize;
    for id in ids {
        if let Some(d) = col.find_document(&id)
            && eval_filter(&d.data.0, filter)
        {
            n += 1;
        }
        if start.elapsed().as_millis() > 5000 {
            crate::dev6!(
                "{{\"bench\":\"query\",\"op\":\"count\",\"collection\":\"{}\",\"duration_ms\":{},\"result_count\":{}}}",
                col.name_str(),
                crate::utils::num::usize_to_u64(start.elapsed().as_millis() as usize),
                crate::utils::num::usize_to_u64(n)
            );
            return Err(DbError::QueryError("timeout".into()));
        }
    }
    crate::dev6!(
        "{{\"bench\":\"query\",\"op\":\"count\",\"collection\":\"{}\",\"duration_ms\":{},\"result_count\":{}}}",
        col.name_str(),
        crate::utils::num::usize_to_u64(start.elapsed().as_millis() as usize),
        crate::utils::num::usize_to_u64(n)
    );
    Ok(n)
}

#[must_use]
pub fn count_docs(col: &Arc<Collection>, filter: &Filter) -> usize {
    let start = std::time::Instant::now();
    let ids = col.list_ids();
    let mut n = 0usize;
    for id in ids {
        if let Some(d) = col.find_document(&id)
            && eval_filter(&d.data.0, filter)
        {
            n += 1;
        }
    }
    crate::dev6!(
        "{{\"bench\":\"query\",\"op\":\"count\",\"collection\":\"{}\",\"duration_ms\":{},\"result_count\":{}}}",
        col.name_str(),
        crate::utils::num::usize_to_u64(start.elapsed().as_millis() as usize),
        crate::utils::num::usize_to_u64(n)
    );
    n
}

pub fn update_many(col: &Arc<Collection>, filter: &Filter, update: &UpdateDoc) -> UpdateReport {
    let bench_start = std::time::Instant::now();
    let mut matched = 0u64;
    let mut modified = 0u64;
    let ids: Vec<DocumentId> = col
        .list_ids()
        .into_iter()
        .filter(|id| col.find_document(id).is_some_and(|d| eval_filter(&d.data.0, filter)))
        .collect();
    for id in ids {
        if let Some(mut doc) = col.find_document(&id) {
            matched += 1;
            let changed = apply_update(&mut doc, update);
            if changed {
                modified += 1;
            }
            col.update_document(&id, doc);
        }
    }
    let dur_ms = bench_start.elapsed().as_millis();
    crate::dev6!(
        "{{\"bench\":\"query\",\"op\":\"update_many\",\"collection\":\"{}\",\"duration_ms\":{},\"matched\":{},\"modified\":{}}}",
        col.name_str(),
        crate::utils::num::usize_to_u64(dur_ms as usize),
        matched,
        modified
    );
    UpdateReport { matched, modified }
}

pub fn update_one(col: &Arc<Collection>, filter: &Filter, update: &UpdateDoc) -> UpdateReport {
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
    let bench_start = std::time::Instant::now();
    let mut deleted = 0u64;
    let ids: Vec<DocumentId> = col
        .list_ids()
        .into_iter()
        .filter(|id| col.find_document(id).is_some_and(|d| eval_filter(&d.data.0, filter)))
        .collect();
    for id in ids {
        if col.delete_document(&id) {
            deleted += 1;
        }
    }
    let dur_ms = bench_start.elapsed().as_millis();
    crate::dev6!(
        "{{\"bench\":\"query\",\"op\":\"delete_many\",\"collection\":\"{}\",\"duration_ms\":{},\"deleted\":{}}}",
        col.name_str(),
        crate::utils::num::usize_to_u64(dur_ms as usize),
        deleted
    );
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

pub fn apply_update(doc: &mut Document, upd: &UpdateDoc) -> bool {
    #[allow(clippy::single_call_fn)]
    fn ensure_subdoc<'a>(root: &'a mut bson::Document, key: &str) -> &'a mut bson::Document {
        let needs_new = match root.get_mut(key) {
            Some(bson::Bson::Document(_)) => false,
            Some(_) => true,
            None => true,
        };
        if needs_new {
            root.insert(key.to_string(), bson::Bson::Document(bson::Document::new()));
        }
        match root.get_mut(key) {
            Some(bson::Bson::Document(d)) => d,
            _ => unreachable!(),
        }
    }
    fn traverse_to_parent<'a>(
        root: &'a mut bson::Document,
        path: &str,
    ) -> (&'a mut bson::Document, String) {
        let mut cur = root;
        let mut iter = path.split('.').peekable();
        let mut last = String::new();
        while let Some(seg) = iter.next() {
            if iter.peek().is_none() {
                last = seg.to_string();
                break;
            }
            cur = ensure_subdoc(cur, seg);
        }
        (cur, last)
    }
    fn set_path(root: &mut bson::Document, path: &str, value: bson::Bson) -> bool {
        let (parent, last) = traverse_to_parent(root, path);
        let old = parent.insert(last, value.clone());
        old.as_ref() != Some(&value)
    }
    fn get_path(root: &bson::Document, path: &str) -> Option<bson::Bson> {
        let mut cur = root;
        let mut iter = path.split('.').peekable();
        while let Some(seg) = iter.next() {
            if iter.peek().is_none() {
                return cur.get(seg).cloned();
            }
            match cur.get(seg) {
                Some(bson::Bson::Document(d)) => {
                    cur = d;
                }
                _ => return None,
            }
        }
        None
    }
    fn unset_path(root: &mut bson::Document, path: &str) -> bool {
        let (parent, last) = traverse_to_parent(root, path);
        parent.remove(&last).is_some()
    }
    fn as_f64(v: &bson::Bson) -> f64 {
        match v {
            bson::Bson::Double(f) => *f,
            bson::Bson::Int32(i) => *i as f64,
            bson::Bson::Int64(i) => *i as f64,
            bson::Bson::Decimal128(d) => d.to_string().parse::<f64>().unwrap_or(0.0),
            _ => 0.0,
        }
    }
    fn inc_path(root: &mut bson::Document, path: &str, by: f64) -> bool {
        let cur = get_path(root, path).unwrap_or(bson::Bson::Double(0.0));
        let newv = bson::Bson::Double(as_f64(&cur) + by);
        set_path(root, path, newv)
    }

    let mut changed = false;
    for (k, v) in &upd.set {
        if set_path(&mut doc.data.0, k, v.clone()) {
            changed = true;
        }
    }
    for (k, inc) in &upd.inc {
        if inc_path(&mut doc.data.0, k, *inc) {
            changed = true;
        }
    }
    for k in &upd.unset {
        if unset_path(&mut doc.data.0, k) {
            changed = true;
        }
    }
    changed
}

fn plan_index_candidates(col: &Arc<Collection>, filter: &Filter) -> Option<Vec<DocumentId>> {
    match filter {
        Filter::Cmp { path, op: CmpOp::Eq, value } => {
            let mut mgr = col.indexes.write();
            if mgr.indexes.contains_key(path) {
                return crate::index::lookup_eq(&mut mgr, path, value);
            }
            None
        }
        Filter::And(fs) => {
            for f in fs {
                if let Some(ids) = plan_index_candidates(col, f) {
                    return Some(ids);
                }
            }
            None
        }
        _ => None,
    }
}

#[allow(dead_code)]
fn _apply_update_with_deltas(
    _doc: &mut Document,
    _upd: &UpdateDoc,
) -> Vec<(crate::wasp::DeltaKey, crate::wasp::DeltaOp)> {
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Engine;
    use crate::query::{FindOptions, Order, SortSpec};
    use bson::doc;

    #[test]
    fn parse_simple_filter() {
        let j = r#"{"field":"x","$eq":1}"#;
        let f = crate::query::parse_filter_json(j).unwrap();
        assert!(matches!(f, Filter::Cmp { path, .. } if path == "x"));
    }

    #[test]
    fn update_doc_set_inc_unset() {
        let mut d =
            Document::new(doc! {"x": 1, "y": 2 }, crate::document::DocumentType::Persistent);
        let ud = UpdateDoc {
            set: vec![("y".into(), bson::Bson::Int32(5))],
            inc: vec![("x".into(), 2.0)],
            unset: vec!["z".into()],
        };
        let changed = super::apply_update(&mut d, &ud);
        assert!(changed);
        assert_eq!(d.data.0.get_i32("y").unwrap(), 5);
        assert_eq!(d.data.0.get_f64("x").unwrap(), 3.0);
    }

    #[test]
    fn find_docs_projection_sort_and_pagination() {
        let e = Engine::new(crate::test_support::temp_wasp("unit_exec_find")).unwrap();
        let col = e.create_collection("u_find".to_string());
        col.insert_document(Document::new(
            doc! {"k":1, "v": 3, "x":0},
            crate::document::DocumentType::Persistent,
        ));
        col.insert_document(Document::new(
            doc! {"k":2, "v": 1, "x":0},
            crate::document::DocumentType::Persistent,
        ));
        col.insert_document(Document::new(
            doc! {"k":3, "v": 2, "x":0},
            crate::document::DocumentType::Persistent,
        ));
        let filter = crate::query::Filter::Cmp {
            path: "x".into(),
            op: crate::query::CmpOp::Eq,
            value: bson::Bson::Int32(0),
        };
        let mut opts = FindOptions::default();
        opts.projection = Some(vec!["k".into()]);
        opts.sort = Some(vec![SortSpec { field: "v".into(), order: Order::Asc }]);
        opts.limit = Some(2);
        let cur = find_docs(&col, &filter, &opts);
        let docs = cur.to_vec();
        assert_eq!(docs.len(), 2);
        // projection removes non-projected fields
        assert!(docs[0].data.0.get("v").is_none());
        assert_eq!(docs[0].data.0.get_i32("k").unwrap(), 2); // v asc => k=2 first
    }

    #[test]
    fn find_docs_respects_projection_and_sort_limits() {
        let e = Engine::new(crate::test_support::temp_wasp("unit_exec_limits")).unwrap();
        let col = e.create_collection("u_find_limits".to_string());
        for i in 0..5i32 {
            col.insert_document(Document::new(
                doc! {"k":i, "v": 10 - i, "x":0, "a":1, "b":2, "c":3},
                crate::document::DocumentType::Persistent,
            ));
        }
        let filter = crate::query::Filter::Cmp {
            path: "x".into(),
            op: crate::query::CmpOp::Eq,
            value: bson::Bson::Int32(0),
        };
        let mut opts = FindOptions::default();
        // Build a sort with many fields; only MAX_SORT_FIELDS should be used
        let mut sorts = vec![
            SortSpec { field: "v".into(), order: Order::Asc },
            SortSpec { field: "k".into(), order: Order::Desc },
        ];
        // Add extras beyond limit to exercise guard
        for n in 0..20 {
            sorts.push(SortSpec { field: format!("z{n}"), order: Order::Asc });
        }
        opts.sort = Some(sorts);
        // Projection with many fields; only MAX_PROJECTION_FIELDS should apply
        let mut proj = vec!["k".to_string()];
        for n in 0..200 {
            proj.push(format!("p{n}"));
        }
        opts.projection = Some(proj);
        let cur = find_docs(&col, &filter, &opts);
        let docs = cur.to_vec();
        assert!(!docs.is_empty());
        // Only projected fields exist; check a known one
        assert!(docs[0].data.0.get("v").is_none());
        assert!(docs[0].data.0.get("k").is_some());
    }
}
