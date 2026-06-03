//! Schedulite: lightweight FIFO / work-stealing thread pool.
//!
//! Run `cargo test` or `cargo run --example hash_bench -- 4 1000`.

mod config;
mod error;
mod pool;
mod scheduler;
mod task;

pub use config::{PoolConfig, SchedulerMode};
pub use error::PoolError;
pub use pool::{PoolBuilder, PoolMetrics, PoolMetricsSnapshot, SchedulitePool};
pub use task::TaskOutcome;
