//! Developer logging with a custom "level 6" and a thread-local sink for deterministic tests.
//! This avoids global logger races and enables asserting on logs in unit/prop tests.

use std::cell::RefCell;

/// Pseudo-level for developer logs.
pub const DEV_LEVEL: u32 = 6;

thread_local! {
    static TL_SINK: RefCell<Option<Vec<String>>> = const { RefCell::new(None) };
}

/// Guard that disables the thread-local sink on drop.
pub struct DevSinkGuard;
impl Drop for DevSinkGuard {
    fn drop(&mut self) {
        TL_SINK.with(|s| *s.borrow_mut() = None);
    }
}

/// Enable the thread-local sink for the current thread. Returns a guard that will disable it on drop.
pub fn enable_thread_sink() -> DevSinkGuard {
    TL_SINK.with(|s| *s.borrow_mut() = Some(Vec::new()));
    DevSinkGuard
}

/// Push a message into the thread-local sink if enabled.
pub fn write_str(msg: &str) {
    TL_SINK.with(|s| {
        if let Some(buf) = s.borrow_mut().as_mut() {
            buf.push(msg.to_owned());
        }
    });
}

/// Drain and return the captured messages for the current thread. If disabled, returns an empty vec.
pub fn drain() -> Vec<String> {
    TL_SINK.with(|s| match s.borrow_mut().as_mut() {
        Some(buf) => {
            let out = buf.clone();
            buf.clear();
            out
        }
        None => Vec::new(),
    })
}

/// Peek at the current captured messages without clearing them.
pub fn snapshot() -> Vec<String> {
    TL_SINK.with(|s| s.borrow().as_ref().cloned().unwrap_or_default())
}

/// Emit a developer log (level 6) and capture it in the thread-local sink if enabled.
#[macro_export]
macro_rules! dev6 {
    ($($arg:tt)*) => {{
        let __s = format!($($arg)*);
        $crate::utils::devlog::write_str(&__s);
        // Also route through the global logger at TRACE with a dedicated target for optional file logging
        log::log!(target: "nexuslite::dev6", log::Level::Trace, "{}", __s);
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thread_local_sink_captures_messages() {
        let _g = enable_thread_sink();
        crate::dev6!("alpha {}", 1);
        crate::dev6!("beta");
        let snap = snapshot();
        assert!(snap.iter().any(|s| s.contains("alpha 1")));
        assert!(snap.iter().any(|s| s.contains("beta")));
        let drained = drain();
        assert!(drained.len() >= 2);
        assert!(snapshot().is_empty());
    }

    #[test]
    fn isolation_between_threads() {
        let _g = enable_thread_sink();
        crate::dev6!("main-thread");
        let handle = std::thread::spawn(|| {
            // No sink enabled in the spawned thread
            crate::dev6!("child-thread");
            snapshot()
        });
        let child_snap = handle.join().unwrap();
        assert!(child_snap.is_empty());
        let main_snap = snapshot();
        assert!(main_snap.iter().any(|s| s.contains("main-thread")));
    }
}
