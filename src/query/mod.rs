// Telemetry is a submodule of query
pub mod telemetry;

// Submodules for separation of concerns
mod cursor;
mod eval;
mod exec;
mod parse;
mod types;

// Public API re-exports (preserve original paths)
pub use cursor::Cursor;
pub use eval::eval_filter;
pub use exec::{
    apply_update, count_docs, count_docs_rate_limited, delete_many, delete_one, find_docs,
    find_docs_rate_limited, update_many, update_one,
};
pub use parse::{FilterSerde, UpdateDocSerde, parse_filter_json, parse_update_json};
pub use types::{
    CmpOp, DeleteReport, Filter, FindOptions, Order, SortSpec, UpdateDoc, UpdateReport,
};
