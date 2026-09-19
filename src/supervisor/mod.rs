//! The Supervisor module manages the lifecycle of the Argus application.
//!
//! This module implements the **Supervisor Pattern**, a design pattern used to
//! manage the lifecycle of multiple, concurrent, long-running services. It acts
//! as the top-level owner of all major components of the application, such as
//! the data source, block processor, and filtering engine.
//!
//! ## Responsibilities
//!
//! - **Initialization**: The `SupervisorBuilder` constructs and "wires" all
//!   services together, injecting necessary dependencies like configuration and
//!   database connections.
//! - **Lifecycle Management**: The `Supervisor` starts all services and manages
//!   their lifetimes.
//! - **Graceful Shutdown**: It listens for shutdown signals (like Ctrl+C or
//!   SIGTERM) and orchestrates a clean shutdown of all managed services.
//! - **Task Supervision**: It monitors the health of each service. If a
//!   critical service fails (panics or returns an error), the supervisor will
//!   shut down all other services to ensure the application exits cleanly
//!   rather than continuing in a partially-functional state.

mod builder;

use std::sync::Arc;

use argus_api::http_server;
use argus_core::{
    config::AppConfig,
    models::{BlockData, CorrelatedBlockData, monitor::Monitor, monitor_match::MonitorMatch},
    persistence::{
        error::PersistenceError,
        traits::{AppRepository, KeyValueStore},
    },
    providers::traits::{DataSource, DataSourceError},
};
use argus_dispatch::{ActionDispatcher, error::ActionDispatcherError};
use argus_engine::{
    alert_manager::AlertManager, block_ingestor::BlockIngestor, block_processor::BlockProcessor,
    filtering::FilteringEngine, outbox_processor::OutboxProcessor,
};
use argus_monitor::{MonitorManager, MonitorValidationError, MonitorValidator};
use argus_providers::rpc::ProviderError;
use builder::SupervisorBuilder;
use thiserror::Error;
use tokio::{signal, sync::mpsc};

use crate::context::AppMetrics;

/// Represents the set of errors that can occur during the supervisor's
/// operation.
#[derive(Debug, Error)]
pub enum SupervisorError {
    /// A required configuration was not provided to the `SupervisorBuilder`.
    #[error("Missing configuration for Supervisor")]
    MissingConfig,

    /// A state repository was not provided to the `SupervisorBuilder`.
    #[error("Missing state repository for Supervisor")]
    MissingStateRepository,

    /// An app metrics was not provided to the `SupervisorBuilder`.
    #[error("Missing app metrics for Supervisor")]
    MissingAppMetrics,

    /// An ABI service was not provided to the `SupervisorBuilder`.
    #[error("Missing ABI service for Supervisor")]
    MissingAbiService,

    /// A notification service was not provided to the `SupervisorBuilder`.
    #[error("Missing notification service for Supervisor")]
    MissingNotificationService,

    /// A data source was not provided to the `SupervisorBuilder`.
    #[error("Missing data source for Supervisor")]
    MissingDataSource,

    /// An error occurred while trying to load monitors from the state
    /// repository.
    #[error("Failed to load monitors from state repository: {0}")]
    MonitorLoadError(#[from] PersistenceError),

    /// A critical error occurred in the data source during block fetching.
    #[error("Data source error: {0}")]
    DataSourceError(#[from] DataSourceError),

    /// The channel for communicating with a downstream service was closed
    /// unexpectedly.
    #[error("Channel closed")]
    ChannelClosed,

    /// An error occurred during monitor validation.
    #[error("Monitor validation error: {0}")]
    MonitorValidationError(#[from] MonitorValidationError),

    /// An error occurred due to an invalid configuration.
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    /// A script compiler was not provided to the `SupervisorBuilder`.
    #[error("Missing script compiler for Supervisor")]
    MissingScriptCompiler,

    /// An error occurred while trying to create a provider.
    #[error("Provider creation failed: {0}")]
    ProviderError(#[from] ProviderError),

    /// A provider was not provided to the `SupervisorBuilder`.
    #[error("Missing provider for Supervisor")]
    MissingProvider,

    /// An error occurred in the action dispatcher.
    #[error("Action dispatcher error: {0}")]
    ActionDispatcher(#[from] ActionDispatcherError),
}

/// The primary runtime manager for the application.
///
/// The Supervisor owns all the major components (services) and is responsible
/// for their startup, shutdown, and health monitoring. Once `run` is called, it
/// becomes
///  the main process loop for the entire application.
pub struct Supervisor<T: AppRepository + KeyValueStore + 'static> {
    /// Shared application configuration.
    config: Arc<AppConfig>,

    /// The persistent state repository for managing application state.
    state: Arc<T>,

    /// The shared application metrics.
    app_metrics: AppMetrics,

    /// The data source for fetching new blockchain data (e.g., from an RPC
    /// endpoint).
    data_source: Arc<dyn DataSource>,

    /// The service responsible for matching decoded data against user-defined
    /// monitors.
    filtering: Arc<dyn FilteringEngine>,

    /// The alert manager that handles sending alerts based on
    /// matched monitors.
    alert_manager: Arc<AlertManager<T>>,

    /// A token used to signal a graceful shutdown to all supervised tasks.
    cancellation_token: tokio_util::sync::CancellationToken,

    /// A set of all spawned tasks that the supervisor is actively managing.
    join_set: tokio::task::JoinSet<()>,

    /// The manager responsible for handling monitors.
    monitor_manager: Arc<MonitorManager>,

    /// Shared monitor validator for validating monitor configs in HTTP
    /// handlers.
    monitor_validator: Arc<MonitorValidator>,

    /// The action dispatcher responsible for executing actions based on monitor
    /// matches.
    action_dispatcher: Arc<ActionDispatcher>,
}

impl<T: AppRepository + KeyValueStore + Send + Sync + 'static> Supervisor<T> {
    /// Creates a new Supervisor instance with all its required components.
    ///
    /// This is typically called by the `SupervisorBuilder` after it has
    /// assembled all the necessary dependencies.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: AppConfig,
        state: Arc<T>,
        app_metrics: AppMetrics,
        data_source: Box<dyn DataSource>,
        filtering: Arc<dyn FilteringEngine>,
        alert_manager: Arc<AlertManager<T>>,
        monitor_manager: Arc<MonitorManager>,
        monitor_validator: Arc<MonitorValidator>,
        action_dispatcher: Arc<ActionDispatcher>,
    ) -> Self {
        Self {
            config: Arc::new(config),
            state,
            app_metrics,
            data_source: Arc::from(data_source),
            filtering,
            alert_manager,
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            join_set: tokio::task::JoinSet::new(),
            monitor_manager,
            monitor_validator,
            action_dispatcher,
        }
    }

    /// Starts the supervisor and all its managed services.
    ///
    /// This method is the main entry point for the application's runtime. It
    /// performs the following steps:
    /// 1. Spawns a signal handler to listen for `SIGINT` (Ctrl+C) and
    ///    `SIGTERM`.
    /// 2. Spawns the `FilteringEngine` as a long-running background task.
    /// 3. Enters the main `select!` loop, which concurrently:
    ///    - Listens for the shutdown signal.
    ///    - Monitors the health of all spawned tasks via the `JoinSet`.
    ///    - Periodically calls `monitor_cycle` to perform the main
    ///      block-fetching logic.
    /// 4. Upon shutdown, it waits for all tasks to complete and performs
    ///    graceful cleanup of resources like the database connection.
    pub async fn run(mut self) -> Result<(), SupervisorError> {
        // Clone the token for the signal handler task.
        let cancellation_token = self.cancellation_token.clone();

        // Spawn a task to listen for shutdown signals.
        self.join_set.spawn(async move {
            let ctrl_c = signal::ctrl_c();
            #[cfg(unix)]
            let terminate = async {
                signal::unix::signal(signal::unix::SignalKind::terminate())
                    .expect("Failed to register SIGTERM handler")
                    .recv()
                    .await;
            };
            #[cfg(not(unix))]
            let terminate = std::future::pending::<()>();

            tokio::select! {
                _ = ctrl_c => tracing::info!("SIGINT (Ctrl+C) received, initiating graceful shutdown."),
                _ = terminate => tracing::info!("SIGTERM received, initiating graceful shutdown."),
            }

            // Notify all other tasks to begin shutting down.
            cancellation_token.cancel();
        });

        // --- Service Initialization ---

        // Create the channel that connects the BlockIngestor to the BlockProcessor.
        let (raw_blocks_tx, raw_blocks_rx) =
            mpsc::channel::<BlockData>(self.config.block_chunk_size as usize * 2);

        // Create the channel that connects the BlockProcessor to the FilteringEngine.
        let (correlated_blocks_tx, correlated_blocks_rx) =
            mpsc::channel::<CorrelatedBlockData>(self.config.block_chunk_size as usize * 2);

        // Create the channel that connects the FilteringEngine to the AlertManager.
        let (monitor_matches_tx, mut monitor_matches_rx) =
            mpsc::channel::<Vec<MonitorMatch>>(self.config.notification_channel_capacity as usize);

        // Create the channel for configuration updates.
        let (config_tx, mut config_rx) = tokio::sync::watch::channel(());

        // Spawn the HTTP server as a background task if enabled.
        if self.config.server.api_key.is_some() {
            tracing::info!("Starting HTTP server...");
            let server_config_clone = Arc::clone(&self.config);
            let http_cancellation_token = self.cancellation_token.clone();
            let server_repo_clone = Arc::clone(&self.state);
            let monitor_validator_clone = Arc::clone(&self.monitor_validator);
            let app_metrics_clone = self.app_metrics.clone();
            self.join_set.spawn(async move {
                tokio::select! {
                    _ = http_server::run_server_from_config(server_config_clone, server_repo_clone, app_metrics_clone, config_tx, monitor_validator_clone) => {},
                    _ = http_cancellation_token.cancelled() => {
                        tracing::info!("HTTP server received shutdown signal.");
                    }
                }
            });
        } else {
            tracing::warn!("HTTP server is disabled because no API key is configured.");
        }

        // --- Task Spawning ---

        // Spawn the BlockIngestor service.
        let block_ingestor = BlockIngestor::new(
            Arc::clone(&self.config),
            Arc::clone(&self.state),
            self.app_metrics.clone(),
            Arc::clone(&self.data_source),
            Arc::clone(&self.filtering),
            raw_blocks_tx,
            self.cancellation_token.clone(),
        );
        self.join_set.spawn(async move {
            block_ingestor.run().await;
        });

        // Spawn the BlockProcessor service.
        let block_processor = BlockProcessor::new(
            Arc::clone(&self.config),
            Arc::clone(&self.state),
            self.app_metrics.clone(),
            self.monitor_manager.clone(),
            raw_blocks_rx,
            correlated_blocks_tx,
            self.cancellation_token.clone(),
        );
        self.join_set.spawn(async move {
            block_processor.run().await;
        });

        // Spawn the FilteringEngine service.
        let filtering_engine_clone = Arc::clone(&self.filtering);
        self.join_set.spawn(async move {
            filtering_engine_clone.run(correlated_blocks_rx, monitor_matches_tx).await;
        });

        // Spawn the AlertManager's main processing loop.
        let alert_manager_clone = Arc::clone(&self.alert_manager);
        self.join_set.spawn(async move {
            while let Some(matches) = monitor_matches_rx.recv().await {
                if let Err(e) = alert_manager_clone.process_matches_batch(&matches).await {
                    tracing::error!("Failed to process monitor matches batch: {}", e);
                }
            }
        });

        // Spawn the AlertManager's background aggregation dispatcher.
        let dispatcher_alert_manager = Arc::clone(&self.alert_manager);
        let aggregation_check_interval = self.config.aggregation_check_interval_secs;
        self.join_set.spawn(async move {
            dispatcher_alert_manager.run_aggregation_dispatcher(aggregation_check_interval).await;
        });

        // Spawn the OutboxProcessor service
        let outbox_processor = Arc::new(OutboxProcessor::new(
            self.state.clone(),
            self.action_dispatcher.clone(),
            self.config.outbox.clone(),
        ));

        let op_clone = outbox_processor.clone();
        let op_token = self.cancellation_token.clone();

        self.join_set.spawn(async move {
            op_clone.run(op_token).await;
        });

        // --- Main Supervisor Loop ---
        // This loop is now only responsible for monitoring task health and shutdown
        // signals.

        loop {
            tokio::select! {
                maybe_result = self.join_set.join_next() => {
                    match maybe_result {
                        Some(Ok(_)) => {
                            // Task completed successfully, continue monitoring.
                        }
                        Some(Err(e)) => {
                            tracing::error!("A critical task failed: {:?}. Initiating shutdown.", e);
                            self.cancellation_token.cancel();
                        }
                        None => {
                            // All tasks have completed.
                            break;
                        }
                    }
                }
                _ = self.cancellation_token.cancelled() => {
                    // Cancellation requested externally, break the loop.
                    break;
                }
                // Listen for configuration change signals to reload monitors.
                _ = config_rx.changed() => {
                    tracing::info!("Configuration change signal received, reloading monitors...");
                    match self.state.get_monitors(&self.config.network_id).await {
                        Ok(monitors) => {
                            self.monitor_manager.update(monitors);
                            tracing::info!("Monitors reloaded successfully.");
                        }
                        Err(e) => {
                            tracing::error!("Failed to reload monitors from database: {}", e);
                        }
                    }
                }
            }
        }

        // --- Graceful Shutdown ---

        // Ensure all spawned tasks are properly awaited before cleanup.
        self.join_set.shutdown().await;
        tracing::info!("All supervised tasks have completed.");

        // Perform final cleanup of resources, with a timeout.
        tracing::info!("Starting graceful resource cleanup...");
        let shutdown_timeout = self.config.shutdown_timeout;

        let cleanup_logic = async {
            // Shutdown the alert manager to flush any pending notifications.
            self.alert_manager.shutdown().await;

            // Drain the outbox to attempt delivery of any pending notifications before
            // exit.
            tracing::info!("Performing final outbox drain...");
            match outbox_processor.drain_queue().await {
                Ok(count) => tracing::info!("Final drain complete. Sent {} notifications.", count),
                Err(e) => tracing::error!("Failed to perform final outbox drain: {}", e),
            }

            // Flush/Close Network Connections (Kafka, NATS, etc.)
            self.action_dispatcher.shutdown().await;

            if let Err(e) = self.state.flush().await {
                tracing::error!(error = %e, "Failed to flush pending writes, but continuing cleanup.");
            }
            if let Err(e) = self.state.cleanup().await {
                tracing::error!(error = %e, "Failed to perform state repository cleanup, but continuing.");
            }
            match self.state.get_last_processed_block(&self.config.network_id).await {
                Ok(Some(last_block)) => tracing::info!(
                    last_processed_block = last_block,
                    "Final state: last processed block recorded."
                ),
                Ok(None) => tracing::info!("Final state: no blocks have been processed yet."),
                Err(e) => {
                    tracing::warn!(error = %e, "Could not retrieve final state during cleanup.")
                }
            }
        };

        if tokio::time::timeout(shutdown_timeout, cleanup_logic).await.is_err() {
            tracing::warn!(
                "Cleanup did not complete within the timeout of {:?}. Continuing shutdown.",
                shutdown_timeout
            );
        } else {
            tracing::info!("Cleanup completed successfully.");
        }

        tracing::info!("Supervisor shutdown complete.");
        Ok(())
    }

    /// Returns a new `SupervisorBuilder` instance.
    ///
    /// This is the public entry point for creating a supervisor.
    pub fn builder() -> SupervisorBuilder<(), ()> {
        SupervisorBuilder::new()
    }

    /// Updates the monitors managed by the `MonitorManager`
    pub fn update_monitors(&self, monitors: Vec<Monitor>) {
        self.monitor_manager.update(monitors)
    }
}
