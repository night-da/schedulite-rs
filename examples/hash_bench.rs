//! CPU-intensive workload demo for `SchedulitePool`.
//!
//! Usage:
//!   cargo run --example hash_bench -- [workers] [tasks] [rounds]

use std::collections::hash_map::DefaultHasher;
use std::env;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use schedulite_rs::SchedulitePool;

fn parse_usize(args: &[String], index: usize, default: usize) -> usize {
    args.get(index)
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn hash_work(seed: u64, rounds: u32) -> u64 {
    let mut acc = seed;
    for i in 0..rounds {
        let mut hasher = DefaultHasher::new();
        acc.hash(&mut hasher);
        i.hash(&mut hasher);
        acc = hasher.finish();
    }
    acc
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let workers = parse_usize(&args, 1, 4);
    let tasks = parse_usize(&args, 2, 1000);
    let rounds = parse_usize(&args, 3, 1000) as u32;

    assert!(workers > 0 && tasks > 0 && rounds > 0);

    let checksum = Arc::new(AtomicU64::new(0));
    let mut pool = SchedulitePool::new(workers);
    let start = Instant::now();

    for task_id in 0..tasks {
        let checksum = Arc::clone(&checksum);
        pool.submit(move || {
            let result = hash_work(task_id as u64, rounds);
            checksum.fetch_xor(result, Ordering::Relaxed);
        })
        .expect("failed to submit hash task");
    }

    pool.shutdown().expect("failed to shutdown pool");

    let elapsed = start.elapsed();
    let secs = elapsed.as_secs_f64();
    let throughput = tasks as f64 / secs;

    println!("schedulite-rs hash_bench");
    println!("------------------------");
    println!("workers:    {workers}");
    println!("tasks:      {tasks}");
    println!("rounds:     {rounds}");
    println!("elapsed:    {secs:.3}s");
    println!("throughput: {throughput:.0} tasks/s");
    println!("checksum:   {:#x}", checksum.load(Ordering::Relaxed));
}
