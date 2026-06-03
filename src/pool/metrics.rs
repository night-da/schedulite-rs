//! Execution counters shared by all scheduler backends.

use std::sync::atomic::{AtomicU64, Ordering};

/// Atomic runtime counters updated by workers and the submit path.
#[derive(Debug, Default)]
pub struct PoolMetrics {
    pub(crate) submitted: AtomicU64,
    pub(crate) completed: AtomicU64,
    pub(crate) panicked: AtomicU64,
    pub(crate) stolen: AtomicU64,
}

/// Point-in-time copy of [`PoolMetrics`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolMetricsSnapshot {
    pub submitted: u64,
    pub completed: u64,
    pub panicked: u64,
    pub stolen: u64,
}

impl PoolMetrics {
    pub(crate) fn record_submitted(&self) {
        self.submitted.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn record_completed(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn record_panicked(&self) {
        self.panicked.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn record_stolen(&self) {
        self.stolen.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> PoolMetricsSnapshot {
        PoolMetricsSnapshot {
            submitted: self.submitted.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            panicked: self.panicked.load(Ordering::Relaxed),
            stolen: self.stolen.load(Ordering::Relaxed),
        }
    }
}
