//! Logging initialisation for Rucio binaries.
//!
//! Provides a single [`init`] function that all binaries call with their own
//! env-var prefix.  Resolution order (first match wins):
//!
//! 1. `RUST_LOG`           — standard tracing override, always honoured
//! 2. `<PREFIX>_LOG`       — fine-grained filter string, same syntax as RUST_LOG
//!    e.g. `RUCIOD_LOG=rucio_emule=debug,rucio_daemon=info`
//! 3. `<PREFIX>_LOG_LEVEL` — simple global level
//!    e.g. `RUCIOD_LOG_LEVEL=debug`
//! 4. Built-in defaults    — `info` for all `rucio_*` crates

use tracing_subscriber::EnvFilter;

/// Initialise the global tracing subscriber for a Rucio binary.
///
/// `prefix` is the upper-case env-var prefix for this binary:
/// - `"RUCIOD"` for the daemon  → reads `RUCIOD_LOG` / `RUCIOD_LOG_LEVEL`
/// - `"RUCIO"`  for the CLI     → reads `RUCIO_LOG`  / `RUCIO_LOG_LEVEL`
///
/// Call this exactly once, at the very start of `main` / `run`.
pub fn init(prefix: &str) {
    let filter = build_filter(prefix);
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn build_filter(prefix: &str) -> EnvFilter {
    // 1. RUST_LOG
    if let Ok(v) = std::env::var("RUST_LOG")
        && !v.is_empty()
    {
        return EnvFilter::new(v);
    }

    // 2. <PREFIX>_LOG  (fine-grained)
    if let Ok(v) = std::env::var(format!("{prefix}_LOG"))
        && !v.is_empty()
    {
        return EnvFilter::new(v);
    }

    // 3. <PREFIX>_LOG_LEVEL  (simple global level)
    if let Ok(v) = std::env::var(format!("{prefix}_LOG_LEVEL"))
        && !v.is_empty()
    {
        // Apply the level to all rucio crates and as the global default.
        return EnvFilter::new(format!(
            "{v},rucio_daemon={v},rucio_core={v},rucio_emule={v}"
        ));
    }

    // 4. Built-in defaults
    EnvFilter::new("rucio_daemon=info,rucio_core=info,rucio_emule=info")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    // Guard that removes env vars on drop, even if the test panics.
    struct EnvGuard(Vec<String>);
    impl EnvGuard {
        fn set(vars: &[(&str, &str)]) -> Self {
            // Clear well-known vars first so no previous test leaks in.
            for key in &[
                "RUST_LOG",
                "RUCIOD_LOG",
                "RUCIOD_LOG_LEVEL",
                "RUCIO_LOG",
                "RUCIO_LOG_LEVEL",
            ] {
                unsafe { std::env::remove_var(key) };
            }
            for (k, v) in vars {
                unsafe { std::env::set_var(k, v) };
            }
            Self(vars.iter().map(|(k, _)| k.to_string()).collect())
        }
    }
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for k in &self.0 {
                unsafe { std::env::remove_var(k) };
            }
        }
    }

    #[test]
    #[serial]
    fn rust_log_takes_priority() {
        let _g = EnvGuard::set(&[
            ("RUST_LOG", "warn"),
            ("RUCIOD_LOG", "debug"),
            ("RUCIOD_LOG_LEVEL", "trace"),
        ]);
        let f = build_filter("RUCIOD");
        let s = format!("{f:?}");
        assert!(s.contains("WARN"), "expected WARN in {s}");
        assert!(!s.contains("DEBUG"), "expected no DEBUG in {s}");
    }

    #[test]
    #[serial]
    fn prefix_log_takes_priority_over_level() {
        let _g = EnvGuard::set(&[
            ("RUCIOD_LOG", "rucio_daemon=debug"),
            ("RUCIOD_LOG_LEVEL", "error"),
        ]);
        let f = build_filter("RUCIOD");
        let s = format!("{f:?}");
        assert!(s.contains("DEBUG"), "expected DEBUG in {s}");
    }

    #[test]
    #[serial]
    fn log_level_expands_to_all_crates() {
        let _g = EnvGuard::set(&[("RUCIOD_LOG_LEVEL", "trace")]);
        let f = build_filter("RUCIOD");
        let s = format!("{f:?}");
        assert!(s.contains("TRACE"), "expected TRACE in {s}");
    }

    #[test]
    #[serial]
    fn cli_prefix_uses_rucio_vars() {
        let _g = EnvGuard::set(&[("RUCIO_LOG_LEVEL", "warn")]);
        let f = build_filter("RUCIO");
        let s = format!("{f:?}");
        assert!(s.contains("WARN"), "expected WARN in {s}");
    }

    #[test]
    #[serial]
    fn defaults_are_info() {
        let _g = EnvGuard::set(&[]);
        let f = build_filter("RUCIOD");
        let s = format!("{f:?}");
        assert!(s.contains("INFO"), "expected INFO in {s}");
    }
}
