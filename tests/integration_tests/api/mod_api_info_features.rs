use nexus_lite::api;

#[test]
fn info_reports_collections_and_features() {
    let tmp = std::env::temp_dir().join("api_info.wal");
    let engine = nexus_lite::engine::Engine::new(tmp).unwrap();
    let _ = engine.create_collection("users".into());
    let info = api::info(&engine);
    assert!(info.collections.iter().any(|c| c.name == "users"));
    assert!(!info.compiled_features.is_empty());
    assert!(info.package_name.len() > 0);
}

#[test]
fn feature_flags_list_get_set() {
    // list returns something
    let _list = api::feature_list();
    // set/get unknown -> error/none
    assert!(api::feature_set("__nope__", true).is_err());
    assert!(api::feature_get("__nope__").is_none());
    // init_from_env just returns list; not asserting content
    let _ = api::init_from_env();
}
