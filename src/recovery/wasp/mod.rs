// WASP submodule: split into focused units but re-export a stable API surface.

pub mod cache;
pub mod consistency;
pub mod manifest;
pub mod page;
pub mod segment;
pub mod snapshot;
pub mod tree;
pub mod types;
pub mod wal;
pub mod wasp_engine;

// Re-export legacy API names so crate::wasp::* keeps working unchanged
pub use cache::{
    BlockCache, WasMetrics, optimize_manifest_updates, prefetch_pages, run_benchmarks,
};
pub use consistency::{
    ConsistencyChecker, ConsistencyReport, ManifestSlotDiagnostics, fuzz_test_corruption,
    recover_manifests, torn_write_protect, verify_page_checksum,
};
pub use manifest::{Manifest, WaspFile};
pub use page::{Page, PageHeader, WASP_PAGE_SIZE, WASP_SEGMENT_SIZE};
pub use segment::{SegmentFile, SegmentFooter};
pub use snapshot::{
    DbSnapshot, SNAPSHOT_CURRENT_VERSION, SNAPSHOT_MAGIC, SnapshotFile, decode_snapshot_from_bytes,
    encode_snapshot_file,
};
pub use tree::{BlockAllocator, CowTree};
pub use types::{DeltaKey, DeltaOp, IndexDelta, WaspFrame};
pub use wal::{TinyWal, WalRecord};
pub use wasp_engine::{StorageEngine, Wasp};
