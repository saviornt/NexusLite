use nexus_lite::engine::Engine;

#[test]
fn test_engine_new() {
    let engine = Engine::new();
    assert!(engine.collections.read().unwrap().is_empty());
}

#[test]
fn test_create_collection() {
    let engine = Engine::new();
    let collection_name = "users".to_string();
    let collection = engine.create_collection(collection_name.clone());

    assert_eq!(collection.name, collection_name);
    assert_eq!(engine.collections.read().unwrap().len(), 1);
    assert!(engine.collections.read().unwrap().contains_key(&collection_name));
}

#[test]
fn test_get_collection() {
    let engine = Engine::new();
    let collection_name = "users".to_string();
    engine.create_collection(collection_name.clone());

    let found_collection = engine.get_collection(&collection_name).unwrap();
    assert_eq!(found_collection.name, collection_name);

    assert!(engine.get_collection("non_existent").is_none());
}

#[test]
fn test_delete_collection() {
    let engine = Engine::new();
    let collection_name = "users".to_string();
    engine.create_collection(collection_name.clone());

    let deleted = engine.delete_collection(&collection_name);
    assert!(deleted);
    assert!(engine.collections.read().unwrap().is_empty());
    assert!(engine.get_collection(&collection_name).is_none());

    let non_existent_name = "non_existent".to_string();
    let deleted_non_existent = engine.delete_collection(&non_existent_name);
    assert!(!deleted_non_existent);
}

#[test]
fn test_list_collection_names() {
    let engine = Engine::new();
    engine.create_collection("users".to_string());
    engine.create_collection("products".to_string());

    let names = engine.list_collection_names();
    assert_eq!(names.len(), 2);
    assert!(names.contains(&"users".to_string()));
    assert!(names.contains(&"products".to_string()));
}
