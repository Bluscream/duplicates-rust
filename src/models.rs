use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Algorithm {
    Md5,
    Sha256,
    Sha512,
    Crc32,
    Size,
    Name,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
pub enum KeepCriteria {
    Latest,
    Oldest,
    Highest,
    Deepest,
    First,
    Last,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
pub enum Mode {
    Delete,
    Symlink,
    Hardlink,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short, long, default_value = ".")]
    pub path: PathBuf,

    #[arg(short, long)]
    pub recursive: bool,

    #[arg(short, long)]
    pub dry_run: bool,

    #[arg(short, long, value_enum)]
    pub keep: KeepCriteria,

    #[arg(short, long, value_enum, default_value = "symlink")]
    pub mode: Mode,

    #[arg(short, long, value_enum, default_value = "md5")]
    pub algorithm: Algorithm,

    #[arg(short, long, default_value = ".lnk,.url")]
    pub ignore: String,

    #[arg(short, long)]
    pub threads: Option<usize>,

    #[arg(long, default_value = "1MB", value_parser = parse_size)]
    pub min_size: u64,

    #[arg(long, default_value = "1TB", value_parser = parse_size)]
    pub max_size: u64,
}

fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    
    // Handle -1 as infinite (u64::MAX)
    if s == "-1" {
        return Ok(u64::MAX);
    }
    
    // Extract number and unit
    let (num_str, unit) = if let Some(pos) = s.find(|c: char| c.is_alphabetic()) {
        (&s[..pos], &s[pos..])
    } else {
        (s, "")
    };
    
    let num: f64 = num_str.parse().map_err(|_| format!("Invalid number: {}", num_str))?;
    
    let multiplier = match unit.to_uppercase().as_str() {
        "" | "B" => 1u64,
        "KB" | "K" => 1024u64,
        "MB" | "M" => 1024u64 * 1024,
        "GB" | "G" => 1024u64 * 1024 * 1024,
        "TB" | "T" => 1024u64 * 1024 * 1024 * 1024,
        _ => return Err(format!("Unknown unit: {}. Use B, KB, MB, GB, or TB", unit)),
    };
    
    Ok((num * multiplier as f64) as u64)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HashEntry {
    pub path: String,
    pub size: u64,
    pub time: u64,
    pub algo: Algorithm,
    pub hash: String,
}

pub struct FileInfo {
    pub path: PathBuf,
    pub rel_path: String,
    pub size: u64,
    pub mtime: u64,
    pub inode: Option<u64>,
}
