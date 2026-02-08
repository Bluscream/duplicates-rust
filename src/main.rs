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

use crate::hashing::calculate_hash;
use crate::models::{Algorithm, Args, FileInfo, HashEntry, KeepCriteria, Mode};
use crate::platform::{create_symlink, get_file_index};
use crate::utils::{format_disk_info, get_raw_disk_info};

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

    // 1. Discovery
    log!("Scanning directory...");
    let mut files = Vec::new();
    let ignores: HashSet<&str> = args.ignore.split(',').collect();

    let walker = WalkDir::new(&abs_path)
        .max_depth(if args.recursive { usize::MAX } else { 1 })
        .into_iter()
        .filter_entry(|e| {
            let name = e.file_name().to_string_lossy();
            !ignores.contains(name.as_ref())
                && name != "duplicates.log"
                && name != "duplicates.hashes.csv"
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
        let metadata = match fs::metadata(&path) {
            Ok(m) => m,
            Err(_) => continue,
        };

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

    // 3. Load Cache
    let mut cache: HashMap<String, String> = HashMap::new();
    if cache_file_path.exists() {
        log!("Loading cache...");
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(b';')
            .from_path(&cache_file_path)?;
        for result in rdr.deserialize() {
            let entry: HashEntry = match result {
                Ok(e) => e,
                Err(_) => continue,
            };
            // Key: path|size|time|algo
            let key = format!(
                "{}|{}|{}|{:?}",
                entry.path, entry.size, entry.time, entry.algo
            );
            cache.insert(key, entry.hash);
        }
    }

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
        log!("Pre-grouping by size...");
        let mut size_groups: HashMap<u64, Vec<FileInfo>> = HashMap::new();
        for f in unique_files {
            size_groups.entry(f.size).or_default().push(f);
        }
        let candidates: Vec<FileInfo> = size_groups
            .into_values()
            .filter(|v| v.len() > 1)
            .flatten()
            .collect();

        log!("Hashing {} candidates...", candidates.len());
        let pb = ProgressBar::new(candidates.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")?
            .progress_chars("#>-"));

        let hashed_results: Vec<(FileInfo, String)> = candidates
            .into_par_iter()
            .map(|f| {
                let key = format!("{}|{}|{}|{:?}", f.rel_path, f.size, f.mtime, args.algorithm);
                let hash = if let Some(h) = cache.get(&key) {
                    h.clone()
                } else {
                    calculate_hash(&f.path, args.algorithm).unwrap_or_else(|_| String::new())
                };
                pb.inc(1);
                (f, hash)
            })
            .collect();
        pb.finish_and_clear();

        let mut new_entries = Vec::new();
        for (f, h) in &hashed_results {
            let key = format!("{}|{}|{}|{:?}", f.rel_path, f.size, f.mtime, args.algorithm);
            if !cache.contains_key(&key) {
                new_entries.push(HashEntry {
                    path: f.rel_path.clone(),
                    size: f.size,
                    time: f.mtime,
                    algo: args.algorithm,
                    hash: h.clone(),
                });
            }
        }

        if !new_entries.is_empty() {
            let file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&cache_file_path)?;
            let mut wtr = csv::WriterBuilder::new()
                .delimiter(b';')
                .has_headers(!cache_file_path.exists() || fs::metadata(&cache_file_path)?.len() == 0)
                .from_writer(file);
            for entry in new_entries {
                wtr.serialize(entry)?;
            }
            wtr.flush()?;
        }

        let mut groups: HashMap<String, Vec<FileInfo>> = HashMap::new();
        for (f, h) in hashed_results {
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
