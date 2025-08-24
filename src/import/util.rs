use crate::document::{Document, DocumentType};
use bson::Document as BsonDocument;

pub fn apply_ttl(doc: &mut Document, map: &BsonDocument, ttl_field: Option<&str>) {
    if !matches!(doc.metadata.document_type, DocumentType::Ephemeral) {
        return;
    }
    let Some(key) = ttl_field else {
        return;
    };
    if let Some(val) = map.get(key)
        && let Some(secs) = match val {
            bson::Bson::Int32(i) => Some(i64::from(*i)),
            bson::Bson::Int64(i) => Some(*i),
            #[allow(clippy::cast_possible_truncation)]
            bson::Bson::Double(f) => Some(*f as i64),
            bson::Bson::String(s) => s.parse::<i64>().ok(),
            _ => None,
        }
    {
        #[allow(clippy::cast_sign_loss)]
        doc.set_ttl(std::time::Duration::from_secs(secs as u64));
    }
}

pub fn escape_json(s: &str) -> String {
    s.replace('"', "\\\"")
}

pub fn field_to_bson(field: &str, infer: bool) -> bson::Bson {
    if !infer {
        return bson::Bson::String(field.to_string());
    }
    if let Ok(i) = field.parse::<i64>() {
        return bson::Bson::Int64(i);
    }
    if let Ok(f) = field.parse::<f64>() {
        return bson::Bson::Double(f);
    }
    match field.to_lowercase().as_str() {
        "true" => bson::Bson::Boolean(true),
        "false" => bson::Bson::Boolean(false),
        _ => bson::Bson::String(field.to_string()),
    }
}
