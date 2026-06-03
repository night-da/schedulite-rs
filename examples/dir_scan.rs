//! Mixed IO + CPU workload: recursively scan a directory and hash every file.
//!
//! Usage:
//!   cargo run --example dir_scan -- [path] [workers]
//!
//! Examples:
//!   cargo run --example dir_scan
//!   cargo run --example dir_scan -- C:\src 8
//!
//! Workers default to 4. The root path defaults to the current directory.

use std::env;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use schedulite_rs::SchedulitePool;

fn collect_paths(root: &Path) -> Vec<String> {
    let mut paths = Vec::new();
    let mut dirs = vec![root.to_path_buf()];

    while let Some(dir) = dirs.pop() {
        if let Ok(entries) = fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    dirs.push(path);
                } else if path.is_file() {
                    if let Some(s) = path.to_str() {
                        paths.push(s.to_owned());
                    }
                }
            }
        }
    }

    paths
}

fn hash_file(path: String) -> Result<(u64, u64), String> {
    let mut file = fs::File::open(&path).map_err(|e| format!("{path}: {e}"))?;
    let mut buf = [0u8; 8192];
    let mut total = 0u64;

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    loop {
        let n = file.read(&mut buf).map_err(|e| format!("{path}: {e}"))?;
        if n == 0 {
            break;
        }
        total += n as u64;
        buf[..n].hash(&mut hasher);
    }

    Ok((total, hasher.finish()))
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let root = args.get(1).map(Path::new).unwrap_or_else(|| Path::new("."));
    let workers: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(4);

    let root_name = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    println!("schedulite-rs dir_scan");
    println!("----------------------");
    println!("path:     {}", root_name.display());
    println!("collecting file list...");

    let paths = collect_paths(root);
    let file_count = paths.len();
    if file_count == 0 {
        println!("no files found.");
        return;
    }

    println!("files:    {file_count}");
    println!("workers:  {workers}");
    println!("hashing files...");

    let checksum = Arc::new(AtomicU64::new(0));
    let total_bytes = Arc::new(AtomicU64::new(0));
    let mut pool = SchedulitePool::new(workers);

    let start = Instant::now();

    for path in paths {
        let checksum = Arc::clone(&checksum);
        let total_bytes = Arc::clone(&total_bytes);
        pool.submit(move || match hash_file(path) {
            Ok((bytes, hash)) => {
                total_bytes.fetch_add(bytes, Ordering::Relaxed);
                checksum.fetch_xor(hash, Ordering::Relaxed);
            }
            Err(e) => eprintln!("warning: {e}"),
        })
        .expect("failed to submit hash task");
    }

    pool.shutdown().expect("failed to shutdown pool");
    let metrics = pool.metrics_snapshot();
    let elapsed = start.elapsed();
    let secs = elapsed.as_secs_f64();

    let bytes = total_bytes.load(Ordering::Relaxed);
    let mib = bytes as f64 / (1024.0 * 1024.0);

    println!();
    println!("elapsed:       {secs:.3}s");
    println!("files:         {file_count}");
    println!("total size:    {mib:.1} MiB");
    println!("throughput:    {:.1} files/s", file_count as f64 / secs);
    if secs > 0.0 {
        println!("IO throughput: {:.1} MiB/s", mib / secs);
    }
    println!("checksum:      {:#x}", checksum.load(Ordering::Relaxed));
    println!("submitted:     {}", metrics.submitted);
    println!("completed:     {}", metrics.completed);
    println!("panicked:      {}", metrics.panicked);
}
