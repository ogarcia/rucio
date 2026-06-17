//! Local file hashing: chunk splitting, BLAKE3 root hash, MIME detection.
//!
//! This module is used both by the daemon (to index shared files) and by the
//! CLI (to generate magnet links without a running daemon).

use std::path::Path;

/// Result of hashing a single file.
pub struct FileHash {
    /// BLAKE3 root hash of the file — its canonical identifier and the root of
    /// the bao verified-streaming Merkle tree (== `blake3::hash` of the content).
    pub root_hash: [u8; 32],
    /// Total file size in bytes.
    pub size: u64,
    /// Pre-order bao outboard (the tree of inner hashes) for this file. Persist
    /// it to serve verifiable chunk slices later; regenerable from the file.
    pub outboard: Vec<u8>,
    /// Detected MIME type, if any.
    pub mime_type: Option<String>,
}

/// Read a file, compute its BLAKE3 root hash and bao verified-streaming outboard
/// (the Merkle tree of inner hashes), and sniff the MIME type. The root hash is
/// exactly `blake3::hash` of the content; the outboard lets us later serve any
/// chunk as a slice that a downloader verifies against that root.
///
/// This function performs blocking I/O and should be called inside
/// `tokio::task::spawn_blocking` when used from async code.
pub fn hash_file(path: &Path) -> anyhow::Result<FileHash> {
    use std::io::Read;

    // Defence in depth: hash only regular files. A character device such as
    // /dev/zero yields endless bytes, so the read would never reach EOF and
    // would spin forever. Indexing runs in a background task and the read
    // happens on a spawn_blocking thread, so this does not stall the daemon's
    // startup — but the indexing task walks files sequentially, so a single
    // never-ending read wedges *all* further indexing and leaks that
    // blocking-pool thread for good. Opening such a node succeeds, so fstat the
    // *open descriptor* (not the path — that avoids a TOCTOU) and reject
    // anything that isn't a regular file. The enumeration paths already filter
    // with `is_file()` (which is also why a shared directory's subdirectories
    // are simply walked, never hashed); this is the last-line guard for a path
    // that changed type after enumeration, or for any future caller.
    let mut file = std::fs::File::open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file() {
        anyhow::bail!(
            "refusing to hash {}: not a regular file — skipping to avoid an \
             unbounded read",
            path.display()
        );
    }

    // MIME sniff from the first bytes (magic numbers), before the outboard pass.
    let mut header = [0u8; 8192];
    let n = file.read(&mut header).unwrap_or(0);
    let mime_type = detect_mime(path, (n > 0).then_some(&header[..n]));
    drop(file);

    // Streaming bao pass: root hash + pre-order outboard (file is not loaded
    // into RAM; the outboard itself is small).
    let (root, outboard, size) = crate::protocol::bao::compute_outboard(path)?;

    Ok(FileHash {
        root_hash: *root.as_bytes(),
        size,
        outboard,
        mime_type,
    })
}

/// Recursively collect all regular non-hidden files under `root`.
pub fn collect_files(root: &Path) -> std::io::Result<Vec<std::path::PathBuf>> {
    let mut out = Vec::new();
    collect_recursive(root, &mut out)?;
    Ok(out)
}

fn collect_recursive(path: &Path, out: &mut Vec<std::path::PathBuf>) -> std::io::Result<()> {
    if path.is_file() {
        out.push(path.to_path_buf());
    } else if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let ft = entry.file_type()?;
            if ft.is_symlink() {
                continue;
            }
            collect_recursive(&entry.path(), out)?;
        }
    }
    Ok(())
}

/// Detect MIME type: magic bytes first, then file extension.
pub fn detect_mime(path: &Path, header: Option<&[u8]>) -> Option<String> {
    if let Some(kind) = header.and_then(infer::get) {
        return Some(kind.mime_type().to_string());
    }
    let ext = path.extension()?.to_str()?.to_lowercase();
    let mime = match ext.as_str() {
        "txt" => "text/plain",
        "html" | "htm" => "text/html",
        "css" => "text/css",
        "js" => "application/javascript",
        "json" => "application/json",
        "xml" => "application/xml",
        "pdf" => "application/pdf",
        "zip" => "application/zip",
        "gz" | "tgz" => "application/gzip",
        "tar" => "application/x-tar",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        "mp3" => "audio/mpeg",
        "ogg" => "audio/ogg",
        "wav" => "audio/wav",
        "flac" => "audio/flac",
        "mp4" => "video/mp4",
        "mkv" => "video/x-matroska",
        "webm" => "video/webm",
        "avi" => "video/x-msvideo",
        "rs" => "text/x-rust",
        "py" => "text/x-python",
        "toml" => "application/toml",
        "yaml" | "yml" => "application/yaml",
        _ => return None,
    };
    Some(mime.to_string())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn write_file(dir: &TempDir, name: &str, content: &[u8]) -> std::path::PathBuf {
        let path = dir.path().join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content).unwrap();
        path
    }

    #[test]
    fn hash_small_file_consistent() {
        let dir = TempDir::new().unwrap();
        let path = write_file(&dir, "hello.txt", b"hello world");
        let fh1 = hash_file(&path).unwrap();
        let fh2 = hash_file(&path).unwrap();
        assert_eq!(fh1.root_hash, fh2.root_hash);
        assert_eq!(fh1.size, 11);
        // Root is the standard blake3 of the content.
        assert_eq!(&fh1.root_hash, blake3::hash(b"hello world").as_bytes());
    }

    #[test]
    fn different_content_different_hash() {
        let dir = TempDir::new().unwrap();
        let a = write_file(&dir, "a.bin", b"content A");
        let b = write_file(&dir, "b.bin", b"content B");
        let ha = hash_file(&a).unwrap();
        let hb = hash_file(&b).unwrap();
        assert_ne!(ha.root_hash, hb.root_hash);
    }

    #[test]
    fn empty_file() {
        let dir = TempDir::new().unwrap();
        let path = write_file(&dir, "empty.bin", b"");
        let fh = hash_file(&path).unwrap();
        assert_eq!(fh.size, 0);
        assert_eq!(&fh.root_hash, blake3::hash(b"").as_bytes());
    }

    // A character device like /dev/zero never reaches EOF: opening it succeeds,
    // so only the post-open fstat saves the read loop from spinning forever.
    // hash_file must reject it instead of wedging the background indexing task.
    #[cfg(unix)]
    #[test]
    fn rejects_character_device() {
        let dev = std::path::Path::new("/dev/zero");
        if dev.exists() {
            let err = hash_file(dev).err().expect("should reject /dev/zero");
            assert!(
                err.to_string().contains("not a regular file"),
                "unexpected error: {err}"
            );
        }
    }

    #[test]
    fn collect_files_finds_files_recursively() {
        let dir = TempDir::new().unwrap();
        write_file(&dir, "root.txt", b"root");
        let sub = dir.path().join("sub");
        std::fs::create_dir(&sub).unwrap();
        let sub_path = sub.join("child.txt");
        std::fs::write(&sub_path, b"child").unwrap();

        let files = collect_files(dir.path()).unwrap();
        assert_eq!(files.len(), 2);
        // All entries should be files
        assert!(files.iter().all(|p| p.is_file()));
    }

    #[test]
    fn detect_mime_by_extension() {
        let path = std::path::Path::new("video.mkv");
        assert_eq!(detect_mime(path, None).as_deref(), Some("video/x-matroska"));
        assert_eq!(
            detect_mime(std::path::Path::new("doc.pdf"), None).as_deref(),
            Some("application/pdf")
        );
        assert!(detect_mime(std::path::Path::new("unknown.xyz"), None).is_none());
    }

    #[test]
    fn detect_mime_by_magic_bytes() {
        // PNG magic bytes
        let png_header = &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
        let path = std::path::Path::new("image.dat");
        assert_eq!(
            detect_mime(path, Some(png_header)).as_deref(),
            Some("image/png")
        );
    }
}
