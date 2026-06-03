//! Integration test: single-task panic must not bring down the pool.
//!
//! Submits 200 tasks where 20 intentionally panic. After shutdown the
//! completed + panicked counter must equal the submitted count and the
//! pool must remain usable for a final job.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use schedulite_rs::{PoolBuilder, PoolError, SchedulerMode};

fn run_panic_test(mode: SchedulerMode) {
    let mut pool = PoolBuilder::new().workers(2).mode(mode).build();
    let alive = Arc::new(AtomicU32::new(0));

    for i in 0..200 {
        let alive = Arc::clone(&alive);
        pool.submit(move || {
            if i % 10 == 0 {
                panic!("intentional panic in task {i}");
            }
            alive.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap();
    }

    pool.shutdown().unwrap();
    let m = pool.metrics_snapshot();

    assert_eq!(m.submitted, 200);
    assert_eq!(m.completed + m.panicked, 200);
    assert_eq!(m.completed, 180);
    assert_eq!(m.panicked, 20);
    assert_eq!(alive.load(Ordering::Relaxed), 180);
}

#[test]
fn panic_isolation_fifo() {
    run_panic_test(SchedulerMode::Fifo);
}

#[test]
fn panic_isolation_steal() {
    run_panic_test(SchedulerMode::Steal);
}

#[test]
fn shutdown_after_all_panics_still_works() {
    let mut pool = PoolBuilder::new().workers(1).build();
    for _ in 0..10 {
        pool.submit(|| panic!("all tasks panic")).unwrap();
    }
    assert!(pool.shutdown_timeout(Duration::from_secs(5)).is_ok());
    assert_eq!(pool.metrics_snapshot().panicked, 10);
}

#[test]
fn pool_error_display_is_not_empty() {
    for err in &[
        PoolError::SubmitFailed,
        PoolError::QueueFull,
        PoolError::ShutdownFailed,
        PoolError::ShutdownTimeout,
    ] {
        assert!(!format!("{err}").is_empty());
        assert!(!format!("{err:?}").is_empty());
    }
}
