#[tokio::test]
async fn test_minimal_async() {
    // This is a minimal async test.
    // It should compile and run without errors if Tokio setup is correct.
    // Yield once to ensure the runtime is active.
    tokio::task::yield_now().await;
}