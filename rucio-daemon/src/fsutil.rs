//! Small filesystem helpers shared across the daemon.

use std::path::Path;

/// Move `src` to `dst`, falling back to copy+delete if they are on different
/// filesystems (the OS returns `EXDEV` / "Invalid cross-device link" for an
/// atomic rename across mount points — e.g. a `temp_dir` and a `download_dir`
/// on separate mounts).
///
/// The copy uses `tokio::fs::copy`, which is `sendfile(2)`-backed on Linux
/// (kernel-space data movement). The source is removed only after the copy
/// succeeds, so a partial write never loses data.
pub(crate) async fn move_file(src: &Path, dst: &Path) -> std::io::Result<()> {
    match tokio::fs::rename(src, dst).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::CrossesDevices => {
            tokio::fs::copy(src, dst).await?;
            tokio::fs::remove_file(src).await?;
            Ok(())
        }
        Err(e) => Err(e),
    }
}
