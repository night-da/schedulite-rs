//! Task types and panic-safe execution.

use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::pool::PoolMetrics;

/// A closure that can be sent to and executed on a worker thread.
pub type Job = Box<dyn FnOnce() + Send + 'static>;

/// Result of running one job inside the pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskOutcome {
    Completed,
    Panicked,
}

/// Runs `job` on the current thread and records the outcome in `metrics`.
///
/// Worker threads stay alive even when `job` panics (`catch_unwind`).
pub fn run_job_safely(job: Job, metrics: &PoolMetrics) -> TaskOutcome {
    match catch_unwind(AssertUnwindSafe(job)) {
        Ok(()) => {
            metrics.record_completed();
            TaskOutcome::Completed
        }
        Err(_) => {
            metrics.record_panicked();
            TaskOutcome::Panicked
        }
    }
}
