//! Integration test: FIFO and Steal modes must produce equivalent results
//! under the same workload, and stealing must occur under skewed submissions.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use schedulite_rs::{PoolBuilder, PoolError, SchedulerMode};

#[test]
fn fifo_and_steal_produce_identical_counter() {
    let modes = [SchedulerMode::Fifo, SchedulerMode::Steal];

    for &mode in &modes {
        let mut pool = PoolBuilder::new().workers(4).mode(mode).build();
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..200 {
            let c = Arc::clone(&counter);
            pool.submit(move || {
                c.fetch_add(1, Ordering::Relaxed);
            })
            .unwrap();
        }

        pool.shutdown().unwrap();
        assert_eq!(counter.load(Ordering::Relaxed), 200);
        assert_eq!(pool.metrics_snapshot().completed, 200);
    }
}

#[test]
fn steal_skewed_submission_stolen_greater_than_zero() {
    let mut pool = PoolBuilder::new()
        .workers(4)
        .mode(SchedulerMode::Steal)
        .build();
    let counter = Arc::new(AtomicUsize::new(0));

    let total = 300;
    let skewed = (total as f64 * 0.9) as usize;

    for i in 0..total {
        let c = Arc::clone(&counter);
        let job = move || {
            for _ in 0..200 {
                c.fetch_add(1, Ordering::Relaxed);
            }
        };
        if i < skewed {
            pool.submit_to_worker(0, job).unwrap();
        } else {
            pool.submit(job).unwrap();
        }
    }

    pool.shutdown().unwrap();
    let m = pool.metrics_snapshot();

    assert_eq!(counter.load(Ordering::Relaxed), total * 200);
    assert_eq!(m.completed, total as u64);
    assert!(m.stolen > 0, "expected stolen > 0, got {}", m.stolen);
}

#[test]
fn fifo_mode_never_steals() {
    let mut pool = PoolBuilder::new()
        .workers(2)
        .mode(SchedulerMode::Fifo)
        .build();

    for _ in 0..100 {
        pool.submit(|| {}).unwrap();
    }

    pool.shutdown().unwrap();
    assert_eq!(pool.metrics_snapshot().stolen, 0);
}

#[test]
fn bounded_queue_backpressure_integration() {
    let pool = PoolBuilder::new().workers(1).queue_capacity(2).build();

    pool.submit(|| {}).unwrap();
    pool.submit(|| {}).unwrap();
    let err = pool.submit(|| {}).unwrap_err();

    assert!(matches!(err, schedulite_rs::PoolError::QueueFull));
}

#[test]
fn shutdown_timeout_detaches_slow_workers() {
    use std::time::Duration;

    let mut pool = PoolBuilder::new().workers(1).build();
    pool.submit(|| std::thread::sleep(Duration::from_secs(10)))
        .unwrap();

    let result = pool.shutdown_timeout(Duration::from_millis(10));
    assert!(matches!(result, Err(PoolError::ShutdownTimeout)));
}
