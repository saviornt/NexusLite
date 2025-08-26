use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    pub slow_query_ms: u64,
    pub query_log_path: Option<PathBuf>,
    pub structured_json: bool,
    pub enable_audit: bool,
    pub max_result_limit: usize,
    pub current_db: Option<String>,
    pub per_collection_max: HashMap<String, usize>,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        let slow = std::env::var("NEXUS_SLOW_QUERY_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(500);
        Self {
            slow_query_ms: slow,
            query_log_path: None,
            structured_json: true,
            enable_audit: false,
            max_result_limit: 10_000,
            current_db: None,
            per_collection_max: HashMap::new(),
        }
    }
}

#[derive(Default)]
pub struct Metrics {
    pub queries_total: AtomicU64,
    pub queries_slow_total: AtomicU64,
    pub writes_total: AtomicU64,
    pub audits_total: AtomicU64,
    pub rate_limited_total: AtomicU64,
}

#[derive(Debug, Clone)]
struct TokenBucketCfg {
    capacity: f64,
    refill_per_sec: f64,
}

struct TokenBucketState {
    cfg: TokenBucketCfg,
    tokens: f64,
    last_refill: Instant,
}

#[derive(Default)]
pub struct Telemetry {
    pub cfg: RwLock<TelemetryConfig>,
    pub metrics: Metrics,
    // For tests we can capture audit lines in-memory
    audit_sink: RwLock<Option<Arc<RwLock<Vec<String>>>>>,
    // Basic per-collection token buckets for rate limiting
    rate_limits: RwLock<HashMap<String, TokenBucketState>>,
    default_rate: RwLock<Option<TokenBucketCfg>>,
}

pub(crate) static TELEMETRY: std::sync::LazyLock<Telemetry> =
    std::sync::LazyLock::new(Telemetry::default);

pub fn set_db_name(db: &str) {
    TELEMETRY.cfg.write().current_db = Some(db.to_string());
}
pub fn set_query_log(path: PathBuf, slow_query_ms: Option<u64>, structured_json: Option<bool>) {
    let mut w = TELEMETRY.cfg.write();
    w.query_log_path = Some(path);
    if let Some(ms) = slow_query_ms {
        w.slow_query_ms = ms;
    }
    if let Some(js) = structured_json {
        w.structured_json = js;
    }
}
pub fn set_slow_query_ms(ms: u64) {
    TELEMETRY.cfg.write().slow_query_ms = ms;
}
pub fn set_audit_enabled(enabled: bool) {
    TELEMETRY.cfg.write().enable_audit = enabled;
}
pub fn set_audit_sink_for_tests(sink: Arc<RwLock<Vec<String>>>) {
    *TELEMETRY.audit_sink.write() = Some(sink);
}

fn write_line(path: &PathBuf, line: &str) {
    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
        use std::io::Write;
        let _ = writeln!(f, "{line}");
    }
}

fn now_ts() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn sha256_hex(input: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(input.as_bytes());
    let out = h.finalize();
    hex::encode(out)
}

pub fn log_query(
    collection: &str,
    filter_dbg: &str,
    duration_ms: u128,
    limit: Option<usize>,
    skip: Option<usize>,
    user: Option<&str>,
) {
    TELEMETRY.metrics.queries_total.fetch_add(1, Ordering::Relaxed);
    let cfg = TELEMETRY.cfg.read().clone();
    let filter_hash = sha256_hex(filter_dbg);
    let slow = match u64::try_from(duration_ms) {
        Ok(ms) => ms >= cfg.slow_query_ms,
        Err(_) => true,
    };
    if slow {
        TELEMETRY.metrics.queries_slow_total.fetch_add(1, Ordering::Relaxed);
    }
    if crate::feature_flags::is_enabled("telemetry-adv")
        && let Some(path) = cfg.query_log_path.as_ref()
        {
            if cfg.structured_json {
                let line = serde_json::json!({
                    "ts": now_ts(),
                    "db": cfg.current_db.as_deref().unwrap_or("default"),
                    "collection": collection,
                    "filter_hash": filter_hash,
                    "duration_ms": u64::try_from(duration_ms).unwrap_or(u64::MAX),
                    "limit": limit,
                    "skip": skip,
                    "user": user,
                    "slow": slow
                })
                .to_string();
                write_line(path, &line);
            } else {
                let line = format!(
                    "ts={} db={} collection={} filter_hash={} duration_ms={} limit={:?} skip={:?} user={:?} slow={}",
                    now_ts(),
                    cfg.current_db.as_deref().unwrap_or("default"),
                    collection,
                    filter_hash,
                    duration_ms,
                    limit,
                    skip,
                    user,
                    slow
                );
                write_line(path, &line);
            }
        }
}

pub fn log_audit(op: &str, collection: &str, doc_id: &str, user: Option<&str>) {
    TELEMETRY.metrics.writes_total.fetch_add(1, Ordering::Relaxed);
    if !TELEMETRY.cfg.read().enable_audit {
        return;
    }
    TELEMETRY.metrics.audits_total.fetch_add(1, Ordering::Relaxed);
    let line = serde_json::json!({
    "ts": now_ts(), "db": TELEMETRY.cfg.read().current_db.clone().unwrap_or_else(||"default".into()),
        "op": op, "collection": collection, "doc_id": doc_id, "user": user
    }).to_string();
    let audit_clone = TELEMETRY.audit_sink.read().clone();
    if let Some(sink) = audit_clone {
        sink.write().push(line.clone());
    }
    if crate::feature_flags::is_enabled("telemetry-adv") {
        let log_path = TELEMETRY.cfg.read().query_log_path.clone();
        if let Some(path) = log_path.as_ref() {
            write_line(path, &line);
        }
    }
}

#[must_use]
pub fn metrics_text() -> String {
    // OpenMetrics/Prometheus exposition format (no types/HELP for brevity)
    let m = &TELEMETRY.metrics;
    format!(
        "nexus_queries_total {}\n\
         nexus_queries_slow_total {}\n\
         nexus_writes_total {}\n\
         nexus_audits_total {}\n\
         nexus_rate_limited_total {}\n",
        m.queries_total.load(Ordering::Relaxed),
        m.queries_slow_total.load(Ordering::Relaxed),
        m.writes_total.load(Ordering::Relaxed),
        m.audits_total.load(Ordering::Relaxed),
        m.rate_limited_total.load(Ordering::Relaxed),
    )
}

pub fn max_result_limit() -> usize {
    TELEMETRY.cfg.read().max_result_limit
}

pub fn set_max_result_limit_global(limit: usize) {
    TELEMETRY.cfg.write().max_result_limit = limit;
}
pub fn set_max_result_limit_for(collection: &str, limit: usize) {
    TELEMETRY.cfg.write().per_collection_max.insert(collection.to_string(), limit);
}
pub fn max_result_limit_for(collection: &str) -> usize {
    let cfg = TELEMETRY.cfg.read();
    cfg.per_collection_max.get(collection).copied().unwrap_or(cfg.max_result_limit)
}

// --- Rate limiting (basic token bucket, per collection) ---

/// Configure or update a per-collection token bucket.
/// capacity: max tokens; `refill_per_sec`: tokens added per second.
#[allow(
    clippy::significant_drop_tightening,
    clippy::cast_precision_loss,
    clippy::suboptimal_flops,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub fn configure_rate_limit(collection: &str, capacity: u64, refill_per_sec: u64) {
    #[allow(clippy::cast_precision_loss)]
    let cfg = TokenBucketCfg { capacity: capacity as f64, refill_per_sec: refill_per_sec as f64 };
    {
        let mut map = TELEMETRY.rate_limits.write();
        let state = map.entry(collection.to_string()).or_insert_with(|| TokenBucketState {
            cfg: cfg.clone(),
            tokens: cfg.capacity,
            last_refill: Instant::now(),
        });
        state.cfg = cfg;
        if state.tokens > state.cfg.capacity {
            state.tokens = state.cfg.capacity;
        }
    }
}

/// Remove a per-collection rate limit configuration.
pub fn remove_rate_limit(collection: &str) {
    TELEMETRY.rate_limits.write().remove(collection);
}

/// Try to consume N tokens from the collection's bucket. Returns true if allowed.
/// If no rate limit is configured, always returns true.
pub fn try_consume_token(collection: &str, n: u64) -> bool {
    #[allow(clippy::cast_precision_loss, clippy::suboptimal_flops)]
    {
        let mut map = TELEMETRY.rate_limits.write();
        // Create default bucket if missing based on resource availability
        if !map.contains_key(collection) {
            let cfg = default_bucket_cfg();
            map.insert(
                collection.to_string(),
                TokenBucketState {
                    cfg: cfg.clone(),
                    tokens: cfg.capacity,
                    last_refill: Instant::now(),
                },
            );
        }
        let Some(state) = map.get_mut(collection) else {
            return true;
        };
        // Refill based on elapsed time
        let now = Instant::now();
        let elapsed = now.duration_since(state.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            state.tokens =
                state.cfg.refill_per_sec.mul_add(elapsed, state.tokens).min(state.cfg.capacity);
            state.last_refill = now;
        }
        if state.tokens >= n as f64 {
            state.tokens -= n as f64;
            // Explicitly release the lock before returning to tighten Drop.
            drop(map);
            return true;
        }
        // Drop before falling through and recording rate_limited_total.
        drop(map);
    }
    TELEMETRY.metrics.rate_limited_total.fetch_add(1, Ordering::Relaxed);
    false
}

/// Peek at the bucket after a passive refill; returns true if request would be rate-limited.
pub fn would_limit(collection: &str, n: u64) -> bool {
    #[allow(clippy::cast_precision_loss, clippy::suboptimal_flops)]
    {
        let mut map = TELEMETRY.rate_limits.write();
        if !map.contains_key(collection) {
            let cfg = default_bucket_cfg();
            map.insert(
                collection.to_string(),
                TokenBucketState {
                    cfg: cfg.clone(),
                    tokens: cfg.capacity,
                    last_refill: Instant::now(),
                },
            );
        }
        let Some(state) = map.get_mut(collection) else {
            drop(map);
            return false;
        };
        let now = Instant::now();
        let elapsed = now.duration_since(state.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            state.tokens =
                state.cfg.refill_per_sec.mul_add(elapsed, state.tokens).min(state.cfg.capacity);
            state.last_refill = now;
        }
        let limited = state.tokens < n as f64;
        drop(map);
        limited
    }
}

/// Estimate milliseconds until enough tokens are available for `n`.
pub fn retry_after_ms(collection: &str, n: u64) -> u64 {
    #[allow(
        clippy::cast_precision_loss,
        clippy::suboptimal_flops,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    {
        let mut map = TELEMETRY.rate_limits.write();
        if !map.contains_key(collection) {
            let cfg = default_bucket_cfg();
            map.insert(
                collection.to_string(),
                TokenBucketState {
                    cfg: cfg.clone(),
                    tokens: cfg.capacity,
                    last_refill: Instant::now(),
                },
            );
        }
        if let Some(state) = map.get_mut(collection) {
            let now = Instant::now();
            let elapsed = now.duration_since(state.last_refill).as_secs_f64();
            if elapsed > 0.0 {
                state.tokens =
                    state.cfg.refill_per_sec.mul_add(elapsed, state.tokens).min(state.cfg.capacity);
                state.last_refill = now;
            }
            if state.tokens >= n as f64 {
                0
            } else {
                let need = n as f64 - state.tokens;
                if state.cfg.refill_per_sec <= 0.0 {
                    u64::MAX
                } else {
                    crate::utils::num::usize_to_u64(
                        ((need / state.cfg.refill_per_sec) * 1000.0).ceil() as usize,
                    )
                }
            }
        } else {
            0
        }
    }
}

/// Set a default rate limit used for collections without explicit config.
pub fn set_default_rate_limit(capacity: u64, refill_per_sec: u64) {
    #[allow(clippy::cast_precision_loss)]
    {
        *TELEMETRY.default_rate.write() = Some(TokenBucketCfg {
            capacity: capacity as f64,
            refill_per_sec: refill_per_sec as f64,
        });
    }
}

/// Log a rate-limited event for observability.
pub fn log_rate_limited(collection: &str, op: &str) {
    TELEMETRY.metrics.rate_limited_total.fetch_add(1, Ordering::Relaxed);
    if crate::feature_flags::is_enabled("telemetry-adv")
        && let Some(path) = TELEMETRY.cfg.read().query_log_path.as_ref()
        {
            let line = serde_json::json!({
                "ts": now_ts(),
                "db": TELEMETRY.cfg.read().current_db.clone().unwrap_or_else(||"default".into()),
                "collection": collection,
                "op": op,
                "rate_limited": true
            })
            .to_string();
            write_line(path, &line);
        }
}

fn default_bucket_cfg() -> TokenBucketCfg {
    // Compute once and cache; allow env overrides
    let value = TELEMETRY.default_rate.read().clone();
    if let Some(cfg) = value {
        return cfg;
    }
    let cap_env = std::env::var("NEXUS_DEFAULT_RATE_CAP").ok().and_then(|s| s.parse::<u64>().ok());
    let rps_env = std::env::var("NEXUS_DEFAULT_RATE_RPS").ok().and_then(|s| s.parse::<u64>().ok());
    let cores = std::thread::available_parallelism().map(std::num::NonZeroUsize::get).unwrap_or(4);
    let capacity = cap_env
        .unwrap_or_else(|| (crate::utils::num::usize_to_u64(cores)).saturating_mul(100).max(200));
    let refill = rps_env
        .unwrap_or_else(|| (crate::utils::num::usize_to_u64(cores)).saturating_mul(50).max(100));
    #[allow(clippy::cast_precision_loss)]
    let cfg = TokenBucketCfg { capacity: capacity as f64, refill_per_sec: refill as f64 };
    *TELEMETRY.default_rate.write() = Some(cfg.clone());
    cfg
}
