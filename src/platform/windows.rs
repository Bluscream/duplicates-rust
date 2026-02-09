use anyhow::Result;
use std::fs::File;
use std::os::windows::io::AsRawHandle;
use std::path::Path;
use winapi::um::fileapi::{GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION};

pub fn get_file_index(path: &Path) -> Result<Option<u64>> {
    let file = File::open(path)?;
    let handle = file.as_raw_handle();
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    if unsafe { GetFileInformationByHandle(handle as *mut _, &mut info) } != 0 {
        let index = ((info.nFileIndexHigh as u64) << 32) | (info.nFileIndexLow as u64);
        Ok(Some(index))
    } else {
        Ok(None)
    }
}

pub fn create_symlink(target: &Path, link: &Path) -> Result<()> {
    std::os::windows::fs::symlink_file(target, link)?;
    Ok(())
}

pub fn is_reparse_point(path: &Path) -> bool {
    use std::os::windows::fs::MetadataExt;
    
    if let Ok(metadata) = std::fs::metadata(path) {
        // FILE_ATTRIBUTE_REPARSE_POINT = 0x400
        // This catches symlinks, junctions, and other reparse points
        metadata.file_attributes() & 0x400 != 0
    } else {
        false
    }
}
