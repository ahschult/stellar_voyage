// stellar_preprocessor/src/checkpoint.rs

use anyhow::{Context, Result};
use std::path::Path;

/// Write `data` to `path` atomically via a `.tmp` rename.
pub fn write_atomic(path: &Path, data: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, data)
        .with_context(|| format!("Failed to write tmp file: {:?}", tmp))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("Failed to rename {:?} → {:?}", tmp, path))?;
    Ok(())
}

/// Returns `true` if the sentinel checkpoint file for `stage_name` exists.
pub fn stage_complete(output_dir: &Path, stage_name: &str) -> bool {
    output_dir
        .join(format!("{}.checkpoint", stage_name))
        .exists()
}

/// Write the sentinel file that marks a stage as successfully complete.
pub fn mark_complete(output_dir: &Path, stage_name: &str) -> Result<()> {
    let path = output_dir.join(format!("{}.checkpoint", stage_name));
    std::fs::write(&path, b"done")
        .with_context(|| format!("Failed to write checkpoint for stage '{}'", stage_name))
}