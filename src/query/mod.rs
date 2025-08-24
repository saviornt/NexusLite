// Telemetry is a submodule of query
pub mod telemetry;

// Submodules for separation of concerns
mod types;
mod parse;
mod eval;
mod exec;
mod cursor;

// Public API re-exports (preserve original paths)
pub use types::{CmpOp, DeleteReport, FindOptions, Filter, Order, SortSpec, UpdateDoc, UpdateReport};
pub use parse::{parse_filter_json, parse_update_json, FilterSerde, UpdateDocSerde};
pub use eval::eval_filter;
pub use exec::{count_docs, count_docs_rate_limited, delete_many, delete_one, find_docs, find_docs_rate_limited, update_many, update_one, apply_update};
pub use cursor::Cursor;
