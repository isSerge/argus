use std::sync::Arc;

use tokio::sync::RwLock;

/// A struct to hold application metrics.
#[derive(Debug, Clone)]
pub struct Metrics {
    /// The time the application started.
    pub start_time: tokio::time::Instant,
    /// The latest block number that has been processed.
    pub latest_processed_block: u64,
    /// The timestamp of the latest processed block in seconds.
    pub latest_processed_block_timestamp_secs: u64,
    /// Number of chain reorgs detected since startup.
    pub reorgs_detected: u64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            start_time: tokio::time::Instant::now(),
            latest_processed_block: 0,
            latest_processed_block_timestamp_secs: 0,
            reorgs_detected: 0,
        }
    }
}

/// Shared application metrics for the HTTP server.
#[derive(Clone, Default)]
pub struct AppMetrics {
    /// Shared metrics.
    pub metrics: Arc<RwLock<Metrics>>,
}
