use bson::{Bson, Document as BsonDocument};
use std::cmp::Ordering;

use super::types::{CmpOp, Filter, MAX_IN_SET, MAX_PATH_DEPTH, MAX_SORT_FIELDS, SortSpec};

pub fn eval_filter(doc: &BsonDocument, filter: &Filter) -> bool {
    match filter {
        Filter::True => true,
        Filter::And(fs) => fs.iter().all(|f| eval_filter(doc, f)),
        Filter::Or(fs) => fs.iter().any(|f| eval_filter(doc, f)),
        Filter::Not(f) => !eval_filter(doc, f),
        Filter::Exists { path, exists } => get_path(doc, path).is_some() == *exists,
        Filter::In { path, values } => get_path(doc, path).is_some_and(|v| is_in_set(v, values)),
        Filter::Nin { path, values } => !get_path(doc, path).is_some_and(|v| is_in_set(v, values)),
        Filter::Cmp { path, op, value } => {
            if let Some(v) = get_path(doc, path) {
                match op {
                    CmpOp::Eq => v == value,
                    CmpOp::Gt => compare_bson(v, value) == Ordering::Greater,
                    CmpOp::Gte => {
                        let c = compare_bson(v, value);
                        c == Ordering::Greater || c == Ordering::Equal
                    }
                    CmpOp::Lt => compare_bson(v, value) == Ordering::Less,
                    CmpOp::Lte => {
                        let c = compare_bson(v, value);
                        c == Ordering::Less || c == Ordering::Equal
                    }
                }
            } else {
                false
            }
        }
        #[cfg(feature = "regex")]
        Filter::Regex { path, pattern, case_insensitive } => {
            if let Some(bson::Bson::String(s)) = get_path(doc, path) {
                let mut re = regex::RegexBuilder::new(pattern);
                re.case_insensitive(*case_insensitive);
                if let Ok(r) = re.build() { r.is_match(s) } else { false }
            } else {
                false
            }
        }
    }
}

pub fn compare_docs(a: &BsonDocument, b: &BsonDocument, sort: &[SortSpec]) -> Ordering {
    for s in sort.iter().take(MAX_SORT_FIELDS) {
        let va = a.get(&s.field);
        let vb = b.get(&s.field);
        let ord = match (va, vb) {
            (Some(x), Some(y)) => compare_bson(x, y),
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            (None, None) => Ordering::Equal,
        };
        if ord != Ordering::Equal {
            return if matches!(s.order, super::types::Order::Asc) { ord } else { ord.reverse() };
        }
    }
    Ordering::Equal
}

fn is_in_set(v: &Bson, set: &[Bson]) -> bool {
    set.iter().take(MAX_IN_SET).any(|x| x == v)
}

fn get_path<'a>(doc: &'a BsonDocument, path: &str) -> Option<&'a Bson> {
    if path.is_empty() || path.len() > 1024 {
        return None;
    }
    let mut cur = doc;
    let mut segs = 0usize;
    let parts = path.split('.');
    let last = parts.clone().next_back().unwrap_or("");
    for part in parts {
        segs += 1;
        if segs > MAX_PATH_DEPTH {
            return None;
        }
        match cur.get(part) {
            Some(Bson::Document(d)) => cur = d,
            Some(v) if part == last => return Some(v),
            _ => return None,
        }
    }
    None
}

pub fn compare_bson(a: &Bson, b: &Bson) -> Ordering {
    use bson::Bson as T;
    fn is_num(x: &T) -> bool {
        matches!(x, T::Int32(_) | T::Int64(_) | T::Double(_) | T::Decimal128(_))
    }
    fn as_f64_num(x: &T) -> f64 {
        match x {
            T::Int32(i) => *i as f64,
            T::Int64(i) => *i as f64,
            T::Double(f) => *f,
            T::Decimal128(d) => d.to_string().parse::<f64>().unwrap_or(f64::NAN),
            _ => f64::NAN,
        }
    }
    if is_num(a) && is_num(b) {
        return as_f64_num(a).total_cmp(&as_f64_num(b));
    }
    match (a, b) {
        (T::String(x), T::String(y)) => x.cmp(y),
        (T::Boolean(x), T::Boolean(y)) => x.cmp(y),
        _ => type_rank(a).cmp(&type_rank(b)),
    }
}

fn type_rank(v: &Bson) -> u8 {
    use bson::Bson as T;
    match v {
        T::Null => 0,
        T::Boolean(_) => 1,
        T::Int32(_) => 2,
        T::Int64(_) => 3,
        T::Double(_) => 4,
        T::String(_) => 5,
        T::Array(_) => 6,
        T::Document(_) => 7,
        T::Binary(_) => 8,
        T::ObjectId(_) => 9,
        T::DateTime(_) => 10,
        T::RegularExpression(_) => 11,
        T::Timestamp(_) => 12,
        T::Symbol(_) => 13,
        T::Decimal128(_) => 14,
        T::Undefined => 15,
        T::DbPointer(_) => 16,
        T::JavaScriptCode(_) => 17,
        T::JavaScriptCodeWithScope(_) => 18,
        T::MaxKey => 250,
        T::MinKey => 251,
    }
}

pub fn project_fields(doc: &BsonDocument, fields: &[String]) -> BsonDocument {
    let mut out = BsonDocument::new();
    for f in fields {
        if let Some(v) = doc.get(f) {
            out.insert(f.clone(), v.clone());
        }
    }
    out
}
