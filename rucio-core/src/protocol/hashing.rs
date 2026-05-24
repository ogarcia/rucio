//! Local file hashing: chunk splitting, BLAKE3 root hash, MIME detection.
//!
//! This module is used both by the daemon (to index shared files) and by the
//! CLI (to generate magnet links without a running daemon).

use std::path::Path;

use crate::protocol::chunk::CHUNK_SIZE;

/// Result of hashing a single file.
pub struct FileHash {
    /// BLAKE3 Merkle root over all chunk hashes.
    pub root_hash: [u8; 32],
    /// Total file size in bytes.
    pub size: u64,
    /// Per-chunk metadata: `(index, hash, size_bytes)`.
    pub chunks: Vec<(u32, [u8; 32], u32)>,
    /// Detected MIME type, if any.
    pub mime_type: Option<String>,
}

/// Read a file, split into [`CHUNK_SIZE`] chunks, compute per-chunk BLAKE3
/// hashes and the Merkle root hash.  Also sniffs the MIME type.
///
/// This function performs blocking I/O and should be called inside
/// `tokio::task::spawn_blocking` when used from async code.
pub fn hash_file(path: &Path) -> anyhow::Result<FileHash> {
    use std::io::Read;

    let mut file = std::fs::File::open(path)?;
    let mut chunks: Vec<(u32, [u8; 32], u32)> = Vec::new();
    let mut file_size: u64 = 0;
    let mut idx: u32 = 0;

    let chunk_sz = CHUNK_SIZE as usize;
    let mut buf = vec![0u8; chunk_sz];
    let mut header_buf: Option<Vec<u8>> = None;

    loop {
        let mut bytes_read = 0;
        // Fill the buffer fully (or until EOF).
        loop {
            let n = file.read(&mut buf[bytes_read..])?;
            if n == 0 {
                break;
            }
            bytes_read += n;
            if bytes_read == chunk_sz {
                break;
            }
        }
        if bytes_read == 0 {
            break;
        }
        let chunk_data = &buf[..bytes_read];
        if header_buf.is_none() {
            header_buf = Some(chunk_data[..bytes_read.min(8192)].to_vec());
        }
        let hash = *blake3::hash(chunk_data).as_bytes();
        chunks.push((idx, hash, bytes_read as u32));
        file_size += bytes_read as u64;
        idx += 1;
    }

    // Root hash: BLAKE3 over the concatenation of all chunk hashes (Merkle-flat).
    let root_hash = if chunks.is_empty() {
        *blake3::hash(&[]).as_bytes()
    } else {
        let mut hasher = blake3::Hasher::new();
        for (_, chunk_hash, _) in &chunks {
            hasher.update(chunk_hash);
        }
        *hasher.finalize().as_bytes()
    };

    let mime_type = detect_mime(path, header_buf.as_deref());

    Ok(FileHash {
        root_hash,
        size: file_size,
        chunks,
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
        assert_eq!(fh1.chunks.len(), 1);
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
        assert_eq!(fh.chunks.len(), 0);
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
