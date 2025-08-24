// Aggregator to run both integration and property tests in a single invocation.
// Usage: cargo test --test all
#![cfg(test)]
#[path = "integration_tests/mod.rs"]
mod integration_tests;
#[path = "prop_tests/mod.rs"]
mod prop_tests;
