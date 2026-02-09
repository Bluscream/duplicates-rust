#[cfg(windows)]
pub mod windows;
#[cfg(unix)]
pub mod unix;

use anyhow::Result;
use std::path::Path;

pub fn get_file_index(path: &Path) -> Result<Option<u64>> {
    #[cfg(windows)]
    return windows::get_file_index(path);
    #[cfg(unix)]
    return unix::get_file_index(path);
    #[cfg(not(any(windows, unix)))]
    Ok(None)
}

pub fn create_symlink(target: &Path, link: &Path) -> Result<()> {
    #[cfg(windows)]
    return windows::create_symlink(target, link);
    #[cfg(unix)]
    return unix::create_symlink(target, link);
    #[cfg(not(any(windows, unix)))]
    anyhow::bail!("Symlinks not supported on this platform")
}

/// Check if a file is a reparse point (symlink, junction, hardlink, etc.)
pub fn is_reparse_point(path: &Path) -> bool {
    #[cfg(windows)]
    return windows::is_reparse_point(path);
    #[cfg(unix)]
    return unix::is_reparse_point(path);
    #[cfg(not(any(windows, unix)))]
    false
}
