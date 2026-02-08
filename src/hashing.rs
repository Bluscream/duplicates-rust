use crate::models::Algorithm;
use anyhow::Result;
use crc32fast::Hasher;
use md5::Md5;
use sha2::{Digest, Sha256, Sha512};
use std::fs::File;
use std::io::Read;
use std::path::Path;

pub fn calculate_hash(path: &Path, algo: Algorithm) -> Result<String> {
    let mut file = File::open(path)?;
    let mut buffer = [0; 8192];

    match algo {
        Algorithm::Md5 => {
            let mut context = Md5::new();
            loop {
                let count = file.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                context.update(&buffer[..count]);
            }
            Ok(hex::encode(context.finalize()))
        }
        Algorithm::Sha256 => {
            let mut context = Sha256::new();
            loop {
                let count = file.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                context.update(&buffer[..count]);
            }
            Ok(hex::encode(context.finalize()))
        }
        Algorithm::Sha512 => {
            let mut context = Sha512::new();
            loop {
                let count = file.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                context.update(&buffer[..count]);
            }
            Ok(hex::encode(context.finalize()))
        }
        Algorithm::Crc32 => {
            let mut hasher = Hasher::new();
            loop {
                let count = file.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                hasher.update(&buffer[..count]);
            }
            Ok(format!("{:08x}", hasher.finalize()))
        }
        _ => Ok(String::new()),
    }
}
