// Resolve the libp2p version from the workspace Cargo.lock and expose it as a
// compile-time env var (RUCIO_LIBP2P_VERSION). It feeds the libp2p/<ver> token
// of the Identify agent_version (see behaviour.rs), so the "engine" version is
// always the one actually linked instead of a literal kept in sync by hand.
use std::path::{Path, PathBuf};

fn main() {
    let version = locate_lockfile()
        .and_then(|path| parse_libp2p_version(&path).map(|v| (path, v)))
        .map(|(path, v)| {
            // Re-run only when the lock that fed the answer changes.
            println!("cargo:rerun-if-changed={}", path.display());
            v
        })
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=RUCIO_LIBP2P_VERSION={version}");
}

/// Walk up from this crate's manifest dir until a Cargo.lock turns up (it lives
/// at the workspace root, one level above rucio-net).
fn locate_lockfile() -> Option<PathBuf> {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").ok()?;
    let mut dir = Path::new(&manifest);
    loop {
        let candidate = dir.join("Cargo.lock");
        if candidate.is_file() {
            return Some(candidate);
        }
        dir = dir.parent()?;
    }
}

/// Extract the version of the exact `libp2p` package from a Cargo.lock. Each
/// entry is a `[[package]]` block with `name` followed by `version`; match the
/// name line exactly so `libp2p-core` and friends don't slip through.
fn parse_libp2p_version(path: &Path) -> Option<String> {
    let lock = std::fs::read_to_string(path).ok()?;
    let mut lines = lock.lines();
    while let Some(line) = lines.next() {
        if line.trim() == r#"name = "libp2p""# {
            for next in lines.by_ref() {
                if let Some(rest) = next.trim().strip_prefix("version = \"") {
                    return rest.strip_suffix('"').map(str::to_string);
                }
            }
        }
    }
    None
}
