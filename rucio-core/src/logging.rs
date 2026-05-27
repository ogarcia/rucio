//! Logging initialisation for Rucio binaries.
//!
//! Provides a single [`init`] function that all binaries call with their own
//! env-var prefix and built-in default filter.  Resolution order (first match wins):
//!
//! 1. `RUST_LOG`           — standard tracing override, always honoured
//! 2. `<PREFIX>_LOG`       — fine-grained filter string, same syntax as RUST_LOG
//!    e.g. `RUCIOD_LOG=rucio_emule=debug,rucio_daemon=info`
//! 3. `<PREFIX>_LOG_LEVEL` — simple global level applied to all known rucio crates
//!    e.g. `RUCIOD_LOG_LEVEL=debug`
//! 4. `default_filter`     — the built-in default passed by the caller; each
//!    binary is responsible for its own sensible default (CLIs typically pass
//!    `"off"` to stay silent unless the user opts in).

use tracing_subscriber::EnvFilter;

/// Initialise the global tracing subscriber for a Rucio binary.
///
/// - `prefix` is the upper-case env-var prefix (`"RUCIOD"`, `"RUCIO"`, …).
/// - `default_filter` is the fallback tracing filter used when no env var is
///   set.  Use the same syntax as `RUST_LOG`.  Pass `"off"` for binaries that
///   should be silent by default (e.g. the CLI).
///
/// Call this exactly once, at the very start of `main` / `run`.
pub fn init(prefix: &str, default_filter: &str) {
    let filter = build_filter(prefix, default_filter);
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn build_filter(prefix: &str, default_filter: &str) -> EnvFilter {
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

    // 3. <PREFIX>_LOG_LEVEL  (simple global level — applies to all rucio crates)
    if let Ok(v) = std::env::var(format!("{prefix}_LOG_LEVEL"))
        && !v.is_empty()
    {
        return EnvFilter::new(format!(
            "{v},rucio_bootstrap={v},rucio_daemon={v},rucio_core={v},rucio_emule={v},rucio_net={v}"
        ));
    }

    // 4. Binary-specific built-in default.
    EnvFilter::new(default_filter)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    const DAEMON_DEFAULT: &str =
        "rucio_daemon=info,rucio_core=info,rucio_emule=info,rucio_net=info";
    const BOOTSTRAP_DEFAULT: &str = "rucio_bootstrap=info,rucio_net=info,rucio_core=info";

    // Guard that removes env vars on drop, even if the test panics.
    struct EnvGuard(Vec<String>);
    impl EnvGuard {
        fn set(vars: &[(&str, &str)]) -> Self {
            for key in &[
                "RUST_LOG",
                "RUCIOD_LOG",
                "RUCIOD_LOG_LEVEL",
                "RUCIO_LOG",
                "RUCIO_LOG_LEVEL",
                "RUCIO_BOOTSTRAP_LOG",
                "RUCIO_BOOTSTRAP_LOG_LEVEL",
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
        let f = build_filter("RUCIOD", DAEMON_DEFAULT);
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
        let f = build_filter("RUCIOD", DAEMON_DEFAULT);
        let s = format!("{f:?}");
        assert!(s.contains("DEBUG"), "expected DEBUG in {s}");
    }

    #[test]
    #[serial]
    fn log_level_expands_to_all_crates() {
        let _g = EnvGuard::set(&[("RUCIOD_LOG_LEVEL", "trace")]);
        let f = build_filter("RUCIOD", DAEMON_DEFAULT);
        let s = format!("{f:?}");
        assert!(s.contains("TRACE"), "expected TRACE in {s}");
    }

    #[test]
    #[serial]
    fn cli_prefix_uses_rucio_vars() {
        let _g = EnvGuard::set(&[("RUCIO_LOG_LEVEL", "warn")]);
        let f = build_filter("RUCIO", "off");
        let s = format!("{f:?}");
        assert!(s.contains("WARN"), "expected WARN in {s}");
    }

    #[test]
    #[serial]
    fn cli_default_is_off() {
        let _g = EnvGuard::set(&[]);
        let f = build_filter("RUCIO", "off");
        let s = format!("{f:?}");
        assert!(s.contains("OFF"), "expected OFF in {s}");
    }

    #[test]
    #[serial]
    fn daemon_default_is_info() {
        let _g = EnvGuard::set(&[]);
        let f = build_filter("RUCIOD", DAEMON_DEFAULT);
        let s = format!("{f:?}");
        assert!(s.contains("INFO"), "expected INFO in {s}");
    }

    #[test]
    #[serial]
    fn bootstrap_default_is_info() {
        let _g = EnvGuard::set(&[]);
        let f = build_filter("RUCIO_BOOTSTRAP", BOOTSTRAP_DEFAULT);
        let s = format!("{f:?}");
        assert!(s.contains("INFO"), "expected INFO in {s}");
    }
}
