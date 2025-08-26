use std::io;

/// Convert a serde_json::Value that must be an object into a bson::Document.
/// Returns io::Error with InvalidData on malformed input.
pub fn json_value_to_bson_document(val: &serde_json::Value) -> io::Result<bson::Document> {
    let obj = val
        .as_object()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "expected JSON object"))?;
    bson::Document::try_from(obj.clone()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Parse a JSON string into a bson::Document. The JSON must be a top-level object.
pub fn parse_json_to_bson_document(json: &str) -> io::Result<bson::Document> {
    let val: serde_json::Value =
        serde_json::from_str(json).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    json_value_to_bson_document(&val)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_to_bson_success() {
        let d = parse_json_to_bson_document("{\"a\":1,\"b\":\"x\"}").unwrap();
        assert_eq!(d.get_i32("a").unwrap(), 1);
        assert_eq!(d.get_str("b").unwrap(), "x");
    }

    #[test]
    fn json_to_bson_rejects_array() {
        let e = parse_json_to_bson_document("[1,2,3]").unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
    }
}
