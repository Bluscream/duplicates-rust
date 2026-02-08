use sysinfo::Disks;
use std::path::Path;

pub fn get_raw_disk_info(path: &Path, disks: &Disks) -> Option<(u64, u64)> {
    let path_str = path.to_string_lossy();
    let normalized_path = if path_str.starts_with(r"\\?\") {
        &path_str[4..]
    } else {
        &path_str
    };
    let normalized_path = Path::new(normalized_path);

    for disk in disks {
        if normalized_path.starts_with(disk.mount_point()) {
            return Some((disk.available_space(), disk.total_space()));
        }
    }
    None
}

pub fn format_disk_info(free: u64, total: u64) -> String {
    let percent = if total > 0 {
        (free as f64 / total as f64) * 100.0
    } else {
        0.0
    };
    format!(
        "{:.2}/{:.2}GB ({:.1}%)",
        free as f64 / 1_073_741_824.0,
        total as f64 / 1_073_741_824.0,
        percent
    )
}
