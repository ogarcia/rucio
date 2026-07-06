use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    ensure_web_dist();
    emit_git_hash();
}

// Ensure rucio-web/dist/ exists so that rust-embed (web-ui feature) can
// compile even when trunk has not been run yet.  The directory may be empty
// (no assets embedded) — the daemon will then return 404 for every web
// request, which is acceptable for CI and development builds without the
// frontend.
fn ensure_web_dist() {
    let dist = Path::new("../rucio-web/dist");
    if !dist.exists() {
        std::fs::create_dir_all(dist).expect("failed to create rucio-web/dist");
    }
}

// Expose the short git commit hash as a compile-time env var (RUCIO_GIT_HASH)
// so the daemon can report it alongside the version in `GET /health` and
// `GET /api/v1/status` (the web "About" panel reads it from there — a single
// source of truth). This is a build-time-only touch that never fails the build:
// if git is unavailable the var is set to an empty string and the endpoints
// report the bare version.
//
// Resolution order:
//   1. An explicit RUCIO_GIT_HASH from the environment (lets CI or a container
//      build inject the hash when the .git dir isn't part of the build context).
//   2. `git rev-parse --short HEAD` run from the crate dir (dev builds, and CI
//      where the checkout keeps .git).
//   3. Empty — no hash reported.
fn emit_git_hash() {
    // Re-run when an injected hash changes.
    println!("cargo:rerun-if-env-changed=RUCIO_GIT_HASH");

    let hash = std::env::var("RUCIO_GIT_HASH")
        .ok()
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
        .or_else(git_short_hash)
        .unwrap_or_default();

    println!("cargo:rustc-env=RUCIO_GIT_HASH={hash}");
}

/// Run `git rev-parse --short HEAD` from the crate dir; return None on any
/// failure (git missing, not a repo, non-zero exit).
fn git_short_hash() -> Option<String> {
    let out = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let hash = String::from_utf8(out.stdout).ok()?.trim().to_string();
    if hash.is_empty() {
        return None;
    }
    // Rebuild when HEAD moves so the baked hash stays current across commits.
    watch_git_head();
    Some(hash)
}

/// Emit `cargo:rerun-if-changed` for `.git/HEAD` and the ref it points at, so a
/// new commit (or branch switch) invalidates the cached build-script output.
fn watch_git_head() {
    let Some(git_dir) = git_dir() else { return };
    let head = git_dir.join("HEAD");
    println!("cargo:rerun-if-changed={}", head.display());

    // If HEAD is a symbolic ref (`ref: refs/heads/...`), also watch that ref
    // file; committing updates it. packed-refs covers the packed case.
    if let Ok(contents) = std::fs::read_to_string(&head)
        && let Some(reference) = contents.strip_prefix("ref:").map(str::trim)
    {
        println!(
            "cargo:rerun-if-changed={}",
            git_dir.join(reference).display()
        );
    }
    let packed = git_dir.join("packed-refs");
    if packed.is_file() {
        println!("cargo:rerun-if-changed={}", packed.display());
    }
}

/// Absolute path to the `.git` directory, via `git rev-parse --git-dir`.
fn git_dir() -> Option<PathBuf> {
    let out = Command::new("git")
        .args(["rev-parse", "--absolute-git-dir"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let dir = String::from_utf8(out.stdout).ok()?.trim().to_string();
    if dir.is_empty() {
        return None;
    }
    Some(Path::new(&dir).to_path_buf())
}
