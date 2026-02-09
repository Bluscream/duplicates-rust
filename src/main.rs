mod cache;
mod hashing;
mod models;
mod platform;
mod utils;

use anyhow::{Context, Result};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::time::UNIX_EPOCH;
use sysinfo::Disks;
use walkdir::WalkDir;

use crate::cache::HashCache;
use crate::hashing::calculate_hash;
use crate::models::{Algorithm, Args, FileInfo, HashEntry, KeepCriteria, Mode};
use crate::platform::{create_symlink, get_file_index};
use crate::utils::{format_disk_info, get_raw_disk_info};

fn format_size(bytes: u64) -> String {
    if bytes == u64::MAX {
        return "∞".to_string();
    }
    const TB: u64 = 1024 * 1024 * 1024 * 1024;
    const GB: u64 = 1024 * 1024 * 1024;
    const MB: u64 = 1024 * 1024;
    const KB: u64 = 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}


fn main() -> Result<()> {
    let args = Args::parse();

    if let Some(t) = args.threads {
        rayon::ThreadPoolBuilder::new().num_threads(t).build_global()?;
    }

    let abs_path = fs::canonicalize(&args.path).context("Failed to canonicalize path")?;
    let log_file_path = abs_path.join("duplicates.log");
    let cache_file_path = abs_path.join("duplicates.hashes.csv");
    let mut log_file = File::create(&log_file_path)?;

    macro_rules! log {
        ($($arg:tt)*) => {
            let msg = format!($($arg)*);
            let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
            let line = format!("[{}] {}\n", timestamp, msg);
            print!("{}", line);
            log_file.write_all(line.as_bytes())?;
        };
    }

    log!(
        "Settings: Path={:?} | Keep={:?} | Mode={:?} | Algorithm={:?} | Recursive={}",
        abs_path,
        args.keep,
        args.mode,
        args.algorithm,
        args.recursive
    );

    let mut disks = Disks::new_with_refreshed_list();
    let initial_disk_stats = get_raw_disk_info(&abs_path, &disks);
    log!(
        "Free space before: {}",
        initial_disk_stats
            .map(|(f, t)| format_disk_info(f, t))
            .unwrap_or_else(|| "Unknown".to_string())
    );

    // 1. Discovery with hash CSV loading
    log!("Scanning directory...");
    let mut files = Vec::new();
    let mut hash_csv_files = Vec::new();
    let ignores: HashSet<&str> = args.ignore.split(',').collect();

    let walker = WalkDir::new(&abs_path)
        .max_depth(if args.recursive { usize::MAX } else { 1 })
        .into_iter()
        .filter_entry(|e| {
            let name = e.file_name().to_string_lossy();
            !ignores.contains(name.as_ref()) && name != "duplicates.log"
        });

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner().template("{spinner:.green} Discovered {pos} files in {msg} folders...")?,
    );
    let mut folder_count = 0;

    for entry in walker {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        if entry.file_type().is_dir() {
            folder_count += 1;
            pb.set_message(folder_count.to_string());
            continue;
        }
        if !entry.file_type().is_file() {
            continue;
        }

        let path = entry.path().to_path_buf();
        
        // Check if this is a hash CSV file
        if path.file_name().and_then(|n| n.to_str()) == Some("duplicates.hashes.csv") {
            hash_csv_files.push(path);
            continue;
        }
        
        let metadata = match fs::metadata(&path) {
            Ok(m) => m,
            Err(_) => continue,
        };

        // Skip symlinks (hardcoded, not configurable)
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileTypeExt;
            if entry.file_type().is_symlink() {
                continue;
            }
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            if metadata.file_attributes() & 0x400 != 0 {  // FILE_ATTRIBUTE_REPARSE_POINT
                continue;
            }
        }

        let rel_path = path.strip_prefix(&abs_path)?.to_string_lossy().into_owned();
        let mtime = metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        let inode = get_file_index(&path).unwrap_or(None);

        files.push(FileInfo {
            path,
            rel_path,
            size: metadata.len(),
            mtime,
            inode,
        });
        pb.inc(1);
    }
    pb.finish_and_clear();
    log!("Found {} total files in {} folders.", files.len(), folder_count);

    // Load all discovered hash CSV files
    let mut hash_cache = HashCache::new(cache_file_path.clone(), abs_path.clone());
    if !hash_csv_files.is_empty() {
        log!("Loading {} hash CSV file(s)...", hash_csv_files.len());
        let mut total_loaded = 0;
        for csv_path in &hash_csv_files {
            if let Ok(loaded) = hash_cache.load_csv(csv_path) {
                total_loaded += loaded;
            }
        }
        if total_loaded > 0 {
            log!("Loaded {} cached hashes from {} file(s)", total_loaded, hash_csv_files.len());
        }
    }

    // 2. Filter hardlinks
    log!("Filtering hardlinks...");
    let mut seen_inodes = HashSet::new();
    let mut unique_files = Vec::new();
    for f in files {
        if let Some(ino) = f.inode {
            if ino != 0 && !seen_inodes.insert((ino, f.size)) {
                continue;
            }
        }
        unique_files.push(f);
    }
    log!("Unique files to process: {}", unique_files.len());

    // 3. Filter by size
    let before_size_filter = unique_files.len();
    unique_files.retain(|f| f.size >= args.min_size && f.size <= args.max_size);
    let filtered_count = before_size_filter - unique_files.len();
    if filtered_count > 0 {
        log!(
            "Filtered {} files outside size range ({} - {})",
            filtered_count,
            format_size(args.min_size),
            format_size(args.max_size)
        );
    }
    log!("Files after size filter: {}", unique_files.len());

    // 4. Hashing
    let groups = if args.algorithm == Algorithm::Name {
        let mut groups: HashMap<String, Vec<FileInfo>> = HashMap::new();
        for f in unique_files {
            let name = f
                .path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned();
            groups.entry(name).or_default().push(f);
        }
        groups
    } else if args.algorithm == Algorithm::Size {
        let mut groups: HashMap<u64, Vec<FileInfo>> = HashMap::new();
        for f in unique_files {
            groups.entry(f.size).or_default().push(f);
        }
        groups
            .into_values()
            .filter(|v| v.len() > 1)
            .map(|v| (v[0].size.to_string(), v))
            .collect()
    } else {
        // Use the cache loaded during discovery
        let mut cache_hits = 0;

        log!("Pre-grouping by size...");
        let mut size_groups: HashMap<u64, Vec<FileInfo>> = HashMap::new();
        for f in unique_files {
            size_groups.entry(f.size).or_default().push(f);
        }
        let all_candidates: Vec<FileInfo> = size_groups
            .into_values()
            .filter(|v| v.len() > 1)
            .flatten()
            .collect();

        // 4. Separate cached from uncached files
        let mut cached_files: Vec<(FileInfo, String)> = Vec::new();
        let mut files_to_hash: Vec<FileInfo> = Vec::new();

        for f in all_candidates {
            if let Some(hash) = hash_cache.get(&f.rel_path, f.size, f.mtime, args.algorithm) {
                cached_files.push((f, hash.clone()));
                cache_hits += 1;
            } else {
                files_to_hash.push(f);
            }
        }

        // Sort by size: smallest first for better progress perception
        files_to_hash.sort_by_key(|f| f.size);

        let total_bytes: u64 = files_to_hash.iter().map(|f| f.size).sum();

        log!(
            "Cache: {} hits, {} files ({:.2} GB) need hashing",
            cache_hits,
            files_to_hash.len(),
            total_bytes as f64 / 1_073_741_824.0
        );

        // 5. Hash files with live CSV appending (progress based on bytes)
        let pb = ProgressBar::new(total_bytes);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
            .unwrap()
            .progress_chars("#>-"));

        let algo = args.algorithm;
        let hash_cache_ref = std::sync::Arc::new(std::sync::Mutex::new(hash_cache));
        let newly_hashed: Vec<(FileInfo, String)> = files_to_hash
            .into_par_iter()
            .filter_map(|f| {
                let hash = calculate_hash(&f.path, algo).unwrap_or_else(|_| String::new());
                
                // Validate hash before using it
                if !crate::hashing::validate_hash(&hash, algo) {
                    pb.inc(f.size);
                    return None;
                }

                // Live append to CSV using HashCache
                let entry = HashEntry {
                    path: f.rel_path.clone(),
                    size: f.size,
                    time: f.mtime,
                    algo,
                    hash: hash.clone(),
                };

                if let Ok(cache) = hash_cache_ref.lock() {
                    let _ = cache.append(&entry);
                }

                pb.inc(f.size);
                Some((f, hash))
            })
            .collect();
        pb.finish_and_clear();

        // 7. Combine cached and newly hashed results
        let mut all_hashed = cached_files;
        all_hashed.extend(newly_hashed);

        let mut groups: HashMap<String, Vec<FileInfo>> = HashMap::new();
        for (f, h) in all_hashed {
            if !h.is_empty() {
                groups.entry(h).or_default().push(f);
            }
        }
        groups
    };

    // 5. Handling
    log!("Processing groups...");
    for (hash, mut group) in groups {
        if group.len() <= 1 {
            continue;
        }

        // Sort
        match args.keep {
            KeepCriteria::Latest => group.sort_by(|a, b| b.mtime.cmp(&a.mtime)),
            KeepCriteria::Oldest => group.sort_by(|a, b| a.mtime.cmp(&b.mtime)),
            KeepCriteria::Highest => group.sort_by(|a, b| a.rel_path.len().cmp(&b.rel_path.len())),
            KeepCriteria::Deepest => group.sort_by(|a, b| b.rel_path.len().cmp(&a.rel_path.len())),
            KeepCriteria::First => group.sort_by(|a, b| a.rel_path.cmp(&b.rel_path)),
            KeepCriteria::Last => group.sort_by(|a, b| b.rel_path.cmp(&a.rel_path)),
        }

        let keep_file = &group[0];
        log!("Group {}: Keeping {}", hash, keep_file.rel_path);

        for dup in &group[1..] {
            if args.dry_run {
                log!("  [DRY RUN] {} -> {:?}", dup.rel_path, args.mode);
                continue;
            }

            match args.mode {
                Mode::Delete => {
                    fs::remove_file(&dup.path)?;
                    log!("  Deleted {}", dup.rel_path);
                }
                Mode::Symlink => {
                    fs::remove_file(&dup.path)?;
                    create_symlink(&keep_file.path, &dup.path)?;
                    log!("  Symlinked {}", dup.rel_path);
                }
                Mode::Hardlink => {
                    fs::remove_file(&dup.path)?;
                    fs::hard_link(&keep_file.path, &dup.path)?;
                    log!("  Hardlinked {}", dup.rel_path);
                }
            }
        }
    }

    disks.refresh_list();
    let final_disk_stats = get_raw_disk_info(&abs_path, &disks);
    log!(
        "Free space after: {}",
        final_disk_stats
            .map(|(f, t)| format_disk_info(f, t))
            .unwrap_or_else(|| "Unknown".to_string())
    );

    if let (Some((f1, t)), Some((f2, _))) = (initial_disk_stats, final_disk_stats) {
        let freed = f2.saturating_sub(f1);
        let freed_gb = freed as f64 / 1_073_741_824.0;
        let freed_percent = if t > 0 {
            (freed as f64 / t as f64) * 100.0
        } else {
            0.0
        };
        log!(
            "Total space freed: {:.2} GB ({:.2}%)",
            freed_gb,
            freed_percent
        );
    }

    log!("Done.");
    Ok(())
}
