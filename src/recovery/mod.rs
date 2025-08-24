#[path = "wasp/mod.rs"]
pub mod wasp;
pub mod recover;
// Preserve public API path: crate::wasp::* remains valid
pub use wasp::*;