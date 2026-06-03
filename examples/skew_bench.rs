//! Skewed workload demo comparing FIFO vs work-stealing schedulers.
//!
//! Usage:
//!   cargo run --example skew_bench -- [workers] [tasks] [skew]
//!
//! Examples:
//!   cargo run --example skew_bench
//!   cargo run --example skew_bench -- 4 2000 0.9
//!
//! The example runs both FIFO and Steal modes so you can compare the perf
//! impact of work-stealing under a skewed workload.
//!
//! A `skew` of 0.9 means 90% of tasks are submitted to worker 0's local
//! queue (steal-only) and the remaining 10% go to the global injector.
//! Under FIFO this has no effect — all tasks go to the shared channel.

use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use schedulite_rs::{SchedulerMode, SchedulitePool};

fn parse_usize(args: &[String], index: usize, default: usize) -> usize {
    args.get(index)
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn parse_f64(args: &[String], index: usize, default: f64) -> f64 {
    args.get(index)
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn run_workload(workers: usize, tasks: usize, skew: f64, mode: SchedulerMode) -> (f64, usize) {
    let skewed_tasks = ((tasks as f64) * skew).round() as usize;
    let mut pool = SchedulitePool::with_mode(workers, mode);
    let counter = Arc::new(AtomicU64::new(0));

    let start = Instant::now();

    for task_id in 0..tasks {
        let counter = Arc::clone(&counter);
        let job = move || {
            let rounds = (task_id % 100) as u64 * 500 + 100;
            let mut acc = task_id as u64;
            for _ in 0..rounds {
                acc = acc.wrapping_mul(6364136223846793005).wrapping_add(1);
            }
            counter.fetch_add(acc, Ordering::Relaxed);
        };

        if task_id < skewed_tasks && mode == SchedulerMode::Steal {
            pool.submit_to_worker(0, job)
                .expect("failed to submit skewed task");
        } else {
            pool.submit(job).expect("failed to submit task");
        }
    }

    pool.shutdown().expect("failed to shutdown pool");
    let metrics = pool.metrics_snapshot();
    let elapsed = start.elapsed().as_secs_f64();

    (elapsed, metrics.stolen as usize)
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let workers = parse_usize(&args, 1, 4);
    let tasks = parse_usize(&args, 2, 2000);
    let skew = parse_f64(&args, 3, 0.9);

    assert!(workers > 0);
    assert!(tasks > 0);
    assert!((0.0..1.0).contains(&skew));

    println!("schedulite-rs skew_bench");
    println!("------------------------");
    println!("workers:  {workers}");
    println!("tasks:    {tasks}");
    println!("skew:     {skew:.2}");
    println!();

    println!(
        "{:<10} {:>10} {:>10} {:>10}",
        "mode", "elapsed", "tasks/s", "stolen"
    );
    println!("{}", "-".repeat(44));

    let (fifo_elapsed, fifo_stolen) = run_workload(workers, tasks, skew, SchedulerMode::Fifo);
    println!(
        "{:<10} {:>9.3}s {:>9.0} {:>10}",
        "fifo",
        fifo_elapsed,
        tasks as f64 / fifo_elapsed,
        fifo_stolen
    );

    let (steal_elapsed, steal_stolen) = run_workload(workers, tasks, skew, SchedulerMode::Steal);
    println!(
        "{:<10} {:>9.3}s {:>9.0} {:>10}",
        "steal",
        steal_elapsed,
        tasks as f64 / steal_elapsed,
        steal_stolen
    );

    println!();
    if steal_elapsed < fifo_elapsed {
        let ratio = fifo_elapsed / steal_elapsed;
        println!(
            "steal is {ratio:.2}x faster than fifo under {skew:.0} skew ({steal_stolen} jobs stolen)."
        );
    } else {
        println!("no significant steal advantage detected (stolen: {steal_stolen}).");
    }
}
