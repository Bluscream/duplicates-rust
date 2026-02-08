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

    #[arg(short, long, default_value = "symlink,.lnk,.url")]
    pub ignore: String,

    #[arg(short, long)]
    pub threads: Option<usize>,
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
