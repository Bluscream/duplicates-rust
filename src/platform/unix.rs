use anyhow::Result;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

pub fn get_file_index(path: &Path) -> Result<Option<u64>> {
    let metadata = std::fs::metadata(path)?;
    Ok(Some(metadata.ino()))
}

pub fn create_symlink(target: &Path, link: &Path) -> Result<()> {
    std::os::unix::fs::symlink(target, link)?;
    Ok(())
}

pub fn is_reparse_point(path: &Path) -> bool {
    // On Unix, check if it's a symlink
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        metadata.file_type().is_symlink()
    } else {
        false
    }
}
