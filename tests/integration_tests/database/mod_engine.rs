// Engine tests for database module scope (mirrors src/database/engine.rs)
use nexuslite::engine::Engine;
use tempfile::tempdir;

#[tokio::test]
async fn test_engine_new() {
    let dir = tempdir().unwrap();
    let wasp_path = dir.path().join("test.wasp");
    let engine = Engine::new(wasp_path).unwrap();
    assert_eq!(engine.collections.read().len(), 1);
    assert!(engine.get_collection("_tempDocuments").is_some());
}

#[tokio::test]
async fn test_create_and_get_collection() {
    let dir = tempdir().unwrap();
    let wasp_path = dir.path().join("test.wasp");
    let engine = Engine::new(wasp_path).unwrap();
    let name = "users";
    engine.create_collection(name.to_string());
    let col = engine.get_collection(name).unwrap();
    assert_eq!(col.name_str(), name);
}

#[tokio::test]
async fn test_delete_collection() {
    let dir = tempdir().unwrap();
    let wasp_path = dir.path().join("test.wasp");
    let engine = Engine::new(wasp_path).unwrap();
    let name = "users";
    engine.create_collection(name.to_string());
    assert!(engine.delete_collection(name));
    assert!(engine.get_collection(name).is_none());
}

#[tokio::test]
async fn test_list_collection_names() {
    let dir = tempdir().unwrap();
    let wasp_path = dir.path().join("test.wasp");
    let engine = Engine::new(wasp_path).unwrap();
    engine.create_collection("users".to_string());
    engine.create_collection("products".to_string());
    let names = engine.list_collection_names();
    assert!(names.contains(&"_tempDocuments".to_string()));
    assert!(names.contains(&"users".to_string()));
    assert!(names.contains(&"products".to_string()));
}
