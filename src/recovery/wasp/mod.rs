// WASP submodule: split into focused units but re-export a stable API surface.

pub mod types;
pub mod cache;
pub mod consistency;
pub mod wal;
pub mod segment;
pub mod page;
pub mod manifest;
pub mod tree;
pub mod snapshot;
pub mod wasp_engine;

// Re-export legacy API names so crate::wasp::* keeps working unchanged
pub use types::{IndexDelta, DeltaKey, DeltaOp, WaspFrame};
pub use cache::{BlockCache, WasMetrics, prefetch_pages, optimize_manifest_updates, run_benchmarks};
pub use consistency::{ConsistencyChecker, ConsistencyReport, ManifestSlotDiagnostics, verify_page_checksum, torn_write_protect, recover_manifests, fuzz_test_corruption};
pub use wal::{TinyWal, WalRecord};
pub use segment::{SegmentFile, SegmentFooter};
pub use page::{Page, PageHeader, WASP_PAGE_SIZE, WASP_SEGMENT_SIZE};
pub use manifest::{Manifest, WaspFile};
pub use tree::{CowTree, BlockAllocator};
pub use snapshot::{DbSnapshot, SnapshotFile, SNAPSHOT_MAGIC, SNAPSHOT_CURRENT_VERSION, encode_snapshot_file, decode_snapshot_from_bytes};
pub use wasp_engine::{Wasp, StorageEngine};
