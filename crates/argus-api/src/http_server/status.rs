//! Represents the `/status` endpoint handler and response structure.
//! Provides application status and metrics.

use argus_core::models::NetworkId;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
};
use serde::Serialize;

use super::{ApiError, ApiState};

/// Represents the response from the `/status` endpoint.
#[derive(Debug, Serialize, Clone)]
pub struct StatusResponse {
    /// The version of the application.
    pub version: String,
    /// The network ID the application is connected to.
    pub network_id: NetworkId,
    /// The uptime of the application in seconds.
    pub uptime_secs: u64,
    /// The latest block number that has been processed.
    pub latest_processed_block: u64,
    /// The timestamp of the latest processed block in seconds.
    pub latest_processed_block_timestamp_secs: u64,
    /// Number of chain reorgs detected since startup.
    pub reorgs_detected: u64,
}

/// Retrieves application status and metrics.
pub async fn status(State(state): State<ApiState>) -> Result<impl IntoResponse, ApiError> {
    let metrics = state.app_metrics.metrics.read().await;
    let response = StatusResponse {
        version: env!("CARGO_PKG_VERSION").to_string(),
        network_id: state.config.network_id.clone(),
        uptime_secs: metrics.start_time.elapsed().as_secs(),
        latest_processed_block: metrics.latest_processed_block,
        latest_processed_block_timestamp_secs: metrics.latest_processed_block_timestamp_secs,
        reorgs_detected: metrics.reorgs_detected,
    };
    Ok((StatusCode::OK, Json(response)))
}
