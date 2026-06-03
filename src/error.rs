//! Error types for the thread pool.

use std::error::Error;
use std::fmt;

/// Errors returned by [`SchedulitePool`](crate::SchedulitePool).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolError {
    /// Job could not be enqueued (pool shut down or channel closed).
    SubmitFailed,
    /// Bounded queue is full; callers should back off and retry.
    QueueFull,
    /// Worker threads could not be stopped cleanly.
    ShutdownFailed,
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            PoolError::SubmitFailed => "failed to submit job",
            PoolError::QueueFull => "bounded queue is full",
            PoolError::ShutdownFailed => "failed to shutdown pool",
        })
    }
}

impl Error for PoolError {}
