use base64::{Engine, engine::general_purpose};
use serde::{Deserialize, Serialize};

#[async_trait::async_trait]
pub trait AssetFile {
    type FileModel;
    /// Checklist for implementers:
    /// - Synchronize all dependency assets first by calling their `synchronize().await` methods.
    /// - Decide whether current output is stale:
    ///   - tracking file missing;
    ///   - target file missing while tracking exists;
    ///   - tracking file schema version exists and does not match current schema version;
    ///   - dependency hash mismatch versus tracking.
    /// - Regenerate output only when stale:
    ///   - call dependency `fetch().await` only in the regeneration path;
    ///   - write target file atomically from regenerated content;
    ///   - compute and return current file hash.
    /// - Update/create tracking file so it records dependency hashes used for regeneration.
    /// - Panic/assert on semantic invariants (for example, impossible metadata/state layout),
    ///   instead of silently falling back.
    ///
    /// Reference implementations:
    /// `AssetFileTrees`, `AssetFileAdvantageComposition`,
    /// `AssetFileTrainingFormatted`, `AssetFileTrainingTokenized`, `AssetFileEmFit`.
    async fn synchronize(&self) -> Base64Hash;

    /// Checklist for implementers:
    /// - Call `self.synchronize().await` first to ensure output is up-to-date.
    /// - Read and deserialize the target file into `Self::FileModel`.
    /// - Panic/assert if the synchronized file cannot be read or parsed as expected.
    /// - Avoid hidden fallback behavior; fetch should reflect the synchronized source of truth.
    async fn fetch(&self) -> Self::FileModel;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Base64Hash(String);

pub fn hash_file(file_path: impl AsRef<std::path::Path>) -> Result<Base64Hash, String> {
    let file = std::fs::File::open(file_path.as_ref())
        .map_err(|e| format!("Cannot open file {}: {}", file_path.as_ref().display(), e))?;
    let mut reader = std::io::BufReader::new(file);
    let mut hasher = blake3::Hasher::new();
    std::io::copy(&mut reader, &mut hasher).map_err(|e| {
        format!(
            "Failed to read file {}: {}",
            file_path.as_ref().display(),
            e
        )
    })?;
    let hash = hasher.finalize();
    Ok(Base64Hash(
        general_purpose::STANDARD.encode(hash.as_bytes()),
    ))
}
