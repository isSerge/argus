//! Implementation of the AppRepository trait for SqliteStateRepository

use alloy::primitives::B256;
use argus_core::{
    action_dispatcher::ActionPayload,
    models::{
        ActionId, NetworkId,
        action::ActionConfig,
        monitor::{Monitor, MonitorConfig, MonitorStatus},
    },
    persistence::{
        error::PersistenceError,
        traits::{AppRepository, OutboxItem},
    },
};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::{QueryBuilder, Sqlite};

use crate::SqliteStateRepository;

fn parse_block_hash(s: &str) -> Result<B256, PersistenceError> {
    s.strip_prefix("0x").unwrap_or(s).parse::<B256>().map_err(|error| {
        PersistenceError::OperationFailed(format!("invalid persisted block hash: {error}"))
    })
}

// Helper struct for mapping from the database row
#[derive(sqlx::FromRow)]
struct MonitorRow {
    monitor_id: i64,
    name: String,
    network: String,
    address: Option<String>,
    abi_name: Option<String>,
    filter_script: String,
    actions: String,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    status: MonitorStatus,
}

// Helper struct for mapping from the database row
#[derive(sqlx::FromRow)]
struct AbiRow {
    abi: String,
}

// Helper struct for mapping from the database row
#[derive(sqlx::FromRow)]
struct ActionRow {
    action_id: ActionId,
    name: String,
    config: String,
}

// Helper struct for mapping from the database row
#[derive(sqlx::FromRow)]
struct OutboxRow {
    id: i64,
    action_name: String,
    payload: String,
    retries: i64,
}

impl TryFrom<OutboxRow> for OutboxItem {
    type Error = PersistenceError;

    fn try_from(row: OutboxRow) -> Result<Self, Self::Error> {
        let payload = serde_json::from_str(&row.payload).map_err(|e| {
            PersistenceError::SerializationError(format!("Corrupt outbox item {}: {}", row.id, e))
        })?;

        let retries = i32::try_from(row.retries).map_err(|_| {
            PersistenceError::OperationFailed(format!(
                "Outbox item {} has invalid retries value {} (out of range for i32)",
                row.id, row.retries
            ))
        })?;

        Ok(Self { id: row.id, action_name: row.action_name, payload, retries })
    }
}

/// The maximum number of SQL variables allowed in a single SQLite statement.
const SQLITE_MAX_VARIABLES: usize = 999;

/// Columns bound per outbox insert row (`action_name`, `payload`,
/// `idempotency_key`). Keep in sync with `push_values` in
/// `enqueue_outbox_batch`.
const OUTBOX_INSERT_COLUMNS: usize = 3;

#[async_trait]
impl AppRepository for SqliteStateRepository {
    /// Retrieves the last processed block number for a given network.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_last_processed_block(
        &self,
        network_id: &NetworkId,
    ) -> Result<Option<u64>, PersistenceError> {
        let result = self
            .execute_query_with_error_handling(
                "query last processed block",
                sqlx::query!(
                    "SELECT block_number FROM processed_blocks WHERE network_id = ?",
                    network_id
                )
                .fetch_optional(&self.pool),
            )
            .await?;

        match result {
            Some(record) => {
                let block_number: i64 = record.block_number;
                match block_number.try_into() {
                    Ok(block_number_u64) => {
                        tracing::debug!(
                            network_id = %network_id,
                            block_number = block_number_u64,
                            "Last processed block found."
                        );
                        Ok(Some(block_number_u64))
                    }
                    Err(error) => {
                        tracing::error!(error = %error, network_id = %network_id, "Failed to convert block_number from i64 to u64.");
                        Err(PersistenceError::OperationFailed(error.to_string()))
                    }
                }
            }
            None => {
                tracing::debug!(network_id = %network_id, "No last processed block found.");
                Ok(None)
            }
        }
    }

    /// Retrieves the last processed block number and hash for a given network.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_last_processed_block_tip(
        &self,
        network_id: &NetworkId,
    ) -> Result<Option<(u64, B256)>, PersistenceError> {
        let result = self
            .execute_query_with_error_handling(
                "query last processed block tip",
                sqlx::query!(
                    "SELECT block_number, block_hash FROM processed_blocks WHERE network_id = ?",
                    network_id
                )
                .fetch_optional(&self.pool),
            )
            .await?;

        match result {
            Some(record) => match record.block_hash {
                Some(hash_str) => {
                    let block_number = u64::try_from(record.block_number)
                        .map_err(|error| PersistenceError::OperationFailed(error.to_string()))?;
                    let block_hash = parse_block_hash(&hash_str)?;
                    Ok(Some((block_number, block_hash)))
                }
                None => Ok(None),
            },
            None => Ok(None),
        }
    }

    /// Sets the last processed block number and hash for a given network.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn set_last_processed_block(
        &self,
        network_id: &NetworkId,
        block_number: u64,
        block_hash: Option<B256>,
    ) -> Result<(), PersistenceError> {
        let block_number_i64 = i64::try_from(block_number).map_err(|error| {
            tracing::error!(error = %error, block_number = %block_number, "Failed to convert block_number to i64 for database insertion.");
            PersistenceError::InvalidInput(error.to_string())
        })?;

        let block_hash_str = block_hash.map(|hash| format!("{hash:#x}"));

        self.execute_query_with_error_handling(
            "set last processed block",
            sqlx::query!(
                "INSERT OR REPLACE INTO processed_blocks (network_id, block_number, block_hash) VALUES (?, ?, ?)",
                network_id,
                block_number_i64,
                block_hash_str
            )
            .execute(&self.pool),
        )
        .await?;

        tracing::info!(network_id = %network_id, block_number = %block_number, "Last processed block set successfully.");
        Ok(())
    }

    /// Performs any necessary cleanup operations before shutdown.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn cleanup(&self) -> Result<(), PersistenceError> {
        // Force a checkpoint to ensure all WAL data is written to the main database
        // file
        self.checkpoint_wal("TRUNCATE").await?;

        tracing::debug!("State repository cleanup completed.");
        Ok(())
    }

    /// Ensures all pending writes are flushed to disk.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn flush(&self) -> Result<(), PersistenceError> {
        // Temporarily set synchronous mode to FULL for maximum durability
        self.set_synchronous_mode("FULL").await?;

        // Force a checkpoint to flush WAL to main database
        self.checkpoint_wal("TRUNCATE").await?;

        // Revert synchronous mode to NORMAL for better performance during normal
        // operations
        self.set_synchronous_mode("NORMAL").await?;

        tracing::debug!("Pending writes flushed successfully.");
        Ok(())
    }

    /// Saves emergency state during shutdown (e.g., partial progress).
    #[tracing::instrument(skip(self), level = "debug")]
    async fn save_emergency_state(
        &self,
        network_id: &NetworkId,
        block_number: u64,
        note: &str,
    ) -> Result<(), PersistenceError> {
        tracing::warn!(
            network_id = %network_id,
            block_number = %block_number,
            note = %note,
            "Saving emergency state during shutdown."
        );

        // Save the current state and flush to ensure it's persisted
        self.set_last_processed_block(network_id, block_number, None).await?;
        self.flush().await?;

        tracing::info!(
            network_id = %network_id,
            block_number = %block_number,
            note = %note,
            "Emergency state saved and flushed successfully."
        );

        Ok(())
    }

    // Monitor management operations

    /// Retrieves all monitors for a specific network.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_monitors(&self, network_id: &NetworkId) -> Result<Vec<Monitor>, PersistenceError> {
        let monitor_rows = self
            .execute_query_with_error_handling("query monitors", async {
                sqlx::query_as!(
                    MonitorRow,
                    r#"
                SELECT 
                    monitor_id as "monitor_id!", 
                    name, 
                    network, 
                    address, 
                    abi_name, 
                    filter_script, 
                    actions,
                    created_at as "created_at!", 
                    updated_at as "updated_at!",
                    status as "status!"
                FROM monitors 
                WHERE network = ?
                "#,
                    network_id
                )
                .fetch_all(&self.pool)
                .await
            })
            .await?;

        let monitors = monitor_rows
            .into_iter()
            .map(|row| {
                let actions: Vec<String> = serde_json::from_str(&row.actions)
                    .map_err(|e| PersistenceError::SerializationError(e.to_string()))?;

                let created_at = DateTime::<Utc>::from_naive_utc_and_offset(row.created_at, Utc);
                let updated_at = DateTime::<Utc>::from_naive_utc_and_offset(row.updated_at, Utc);

                Ok(Monitor {
                    id: row.monitor_id,
                    name: row.name,
                    network: NetworkId::from(row.network),
                    address: row.address,
                    abi_name: row.abi_name,
                    filter_script: row.filter_script,
                    actions,
                    created_at,
                    updated_at,
                    status: row.status,
                })
            })
            .collect::<Result<Vec<_>, PersistenceError>>()?;

        tracing::debug!(
            network_id = %network_id,
            monitor_count = monitors.len(),
            "Monitors retrieved successfully."
        );
        Ok(monitors)
    }

    /// Retrieves a specific monitor by its ID for a given network.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_monitor_by_id(
        &self,
        network_id: &NetworkId,
        monitor_id: &str,
    ) -> Result<Option<Monitor>, PersistenceError> {
        let monitor_id_num: i64 = monitor_id.parse().map_err(|e| {
            let msg = format!("Invalid monitor_id '{}': {}", monitor_id, e);
            tracing::error!(error = %e, monitor_id = %monitor_id, "Failed to parse monitor_id.");
            PersistenceError::InvalidInput(msg)
        })?;

        let monitor_row = self
            .execute_query_with_error_handling("query monitor by id", async {
                sqlx::query_as!(
                    MonitorRow,
                    r#"
                SELECT 
                    monitor_id as "monitor_id!", 
                    name, 
                    network, 
                    address, 
                    abi_name, 
                    filter_script, 
                    actions,
                    created_at as "created_at!", 
                    updated_at as "updated_at!",
                    status as "status!"
                FROM monitors 
                WHERE network = ? AND monitor_id = ?
                "#,
                    network_id,
                    monitor_id_num
                )
                .fetch_optional(&self.pool)
                .await
            })
            .await?;

        if let Some(row) = monitor_row {
            let actions: Vec<String> = serde_json::from_str(&row.actions)
                .map_err(|e| PersistenceError::SerializationError(e.to_string()))?;

            let created_at = DateTime::<Utc>::from_naive_utc_and_offset(row.created_at, Utc);
            let updated_at = DateTime::<Utc>::from_naive_utc_and_offset(row.updated_at, Utc);

            let monitor = Monitor {
                id: row.monitor_id,
                name: row.name,
                network: NetworkId::from(row.network),
                address: row.address,
                abi_name: row.abi_name,
                filter_script: row.filter_script,
                actions,
                created_at,
                updated_at,
                status: row.status,
            };

            tracing::debug!(network_id = %network_id, monitor_id = %monitor_id, "Monitor found.");
            Ok(Some(monitor))
        } else {
            tracing::debug!(network_id = %network_id, monitor_id = %monitor_id, "No monitor found with given ID.");
            Ok(None)
        }
    }

    /// Adds multiple monitors for a specific network.
    #[tracing::instrument(skip(self, monitors), level = "debug")]
    async fn add_monitors(
        &self,
        network_id: &NetworkId,
        monitors: Vec<MonitorConfig>,
    ) -> Result<(), PersistenceError> {
        // Validate that all monitors belong to the correct network
        for monitor in &monitors {
            if monitor.network != *network_id {
                let msg = format!(
                    "Monitor '{}' has network '{}' but expected '{}'",
                    monitor.name, monitor.network, network_id
                );
                tracing::error!(
                    expected_network = %network_id,
                    actual_network = %monitor.network,
                    monitor_name = %monitor.name,
                    "Monitor network mismatch."
                );
                return Err(PersistenceError::InvalidInput(msg));
            }
        }

        // Insert monitors in a transaction for atomicity
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| PersistenceError::OperationFailed(e.to_string()))?;

        for monitor in monitors {
            let actions_str = serde_json::to_string(&monitor.actions)
                .map_err(|e| PersistenceError::SerializationError(e.to_string()))?;
            let status = monitor.status;
            let network = &monitor.network;

            sqlx::query!(
                "INSERT INTO monitors (name, network, address, abi_name, filter_script, actions, \
                 status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                monitor.name,
                network,
                monitor.address,
                monitor.abi_name,
                monitor.filter_script,
                actions_str,
                status,
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| PersistenceError::OperationFailed(e.to_string()))?;
        }

        tx.commit().await.map_err(|e| PersistenceError::OperationFailed(e.to_string()))?;

        tracing::info!(network_id = %network_id, "Monitors added successfully.");
        Ok(())
    }

    /// Clears all monitors for a specific network.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn clear_monitors(&self, network_id: &NetworkId) -> Result<(), PersistenceError> {
        let result = self
            .execute_query_with_error_handling(
                "clear monitors",
                sqlx::query!("DELETE FROM monitors WHERE network = ?", network_id)
                    .execute(&self.pool),
            )
            .await?;

        let deleted_count = result.rows_affected();
        tracing::info!(network_id = %network_id, deleted_count, "Monitors cleared successfully.");
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn delete_monitor(
        &self,
        network_id: &NetworkId,
        monitor_id: &str,
    ) -> Result<(), PersistenceError> {
        let result = self
            .execute_query_with_error_handling(
                "delete monitor",
                sqlx::query!(
                    "DELETE FROM monitors WHERE network = ? AND monitor_id = ?",
                    network_id,
                    monitor_id
                )
                .execute(&self.pool),
            )
            .await?;

        if result.rows_affected() == 0 {
            tracing::warn!(network_id = %network_id, monitor_id = %monitor_id, "Monitor not found for deletion.");
            return Err(PersistenceError::NotFound);
        }

        tracing::info!(network_id = %network_id, monitor_id = %monitor_id, "Monitor deleted successfully.");
        Ok(())
    }

    #[tracing::instrument(skip(self, monitor), level = "debug")]
    async fn update_monitor(
        &self,
        network_id: &NetworkId,
        monitor_id: &str,
        monitor: MonitorConfig,
    ) -> Result<(), PersistenceError> {
        let actions_json = serde_json::to_string(&monitor.actions)
            .map_err(|e| PersistenceError::SerializationError(e.to_string()))?;

        let result = self
            .execute_query_with_error_handling(
                "update monitor",
                sqlx::query!(
                    r#"
                UPDATE monitors 
                SET 
                    address = ?, 
                    abi_name = ?, 
                    filter_script = ?, 
                    actions = ?
                WHERE network = ? AND monitor_id = ?
                "#,
                    monitor.address,
                    monitor.abi_name,
                    monitor.filter_script,
                    actions_json,
                    network_id,
                    monitor_id,
                )
                .execute(&self.pool),
            )
            .await?;

        if result.rows_affected() == 0 {
            tracing::warn!(network_id = %network_id, monitor_id = %monitor_id, "Monitor not found for update.");
            return Err(PersistenceError::NotFound);
        }

        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn update_monitor_status(
        &self,
        network_id: &NetworkId,
        monitor_id: &str,
        status: MonitorStatus,
    ) -> Result<(), PersistenceError> {
        let result = self
            .execute_query_with_error_handling(
                "update monitor status",
                sqlx::query!(
                    r#"
                UPDATE monitors 
                SET 
                    status = ?
                WHERE network = ? AND monitor_id = ?
                "#,
                    status,
                    network_id,
                    monitor_id,
                )
                .execute(&self.pool),
            )
            .await?;

        if result.rows_affected() == 0 {
            tracing::warn!(network_id = %network_id, monitor_id = %monitor_id, "Monitor not found for status update.");
            return Err(PersistenceError::NotFound);
        }

        tracing::info!(network_id = %network_id, monitor_id = %monitor_id, ?status, "Monitor status updated successfully.");
        Ok(())
    }

    /// Creates a new ABI.
    #[tracing::instrument(skip(self, abi), level = "debug")]
    async fn create_abi(&self, name: &str, abi: &str) -> Result<(), PersistenceError> {
        self.execute_query_with_error_handling(
            "create abi",
            sqlx::query!("INSERT INTO abis (name, abi) VALUES (?, ?)", name, abi)
                .execute(&self.pool),
        )
        .await?;

        tracing::info!(name, "ABI created successfully.");
        Ok(())
    }

    /// Retrieves an ABI by its name.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_abi(&self, name: &str) -> Result<Option<String>, PersistenceError> {
        let abi_row = self
            .execute_query_with_error_handling("query abi by name", async {
                sqlx::query_as!(AbiRow, "SELECT abi FROM abis WHERE name = ?", name)
                    .fetch_optional(&self.pool)
                    .await
            })
            .await?;

        Ok(abi_row.map(|row| row.abi))
    }

    /// Lists all available ABI names.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_abis(&self) -> Result<Vec<String>, PersistenceError> {
        let abi_names = self
            .execute_query_with_error_handling(
                "list abi names",
                sqlx::query!("SELECT name FROM abis").fetch_all(&self.pool),
            )
            .await?
            .into_iter()
            .map(|record| record.name)
            .collect::<Vec<_>>();

        tracing::debug!(count = abi_names.len(), "ABI names retrieved successfully.");
        Ok(abi_names)
    }

    /// Deletes an ABI by its name.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn delete_abi(&self, name: &str) -> Result<(), PersistenceError> {
        // Check if any monitors are using this ABI and get their names
        let monitors_using_abi = self
            .execute_query_with_error_handling(
                "check monitors using abi",
                sqlx::query!("SELECT name FROM monitors WHERE abi_name = ?", name)
                    .fetch_all(&self.pool),
            )
            .await?
            .into_iter()
            .map(|row| row.name)
            .collect::<Vec<_>>();

        if !monitors_using_abi.is_empty() {
            tracing::warn!(
                name,
                monitors = ?monitors_using_abi,
                "ABI deletion blocked: ABI in use."
            );
            return Err(PersistenceError::AbiInUse(monitors_using_abi));
        }

        let result = self
            .execute_query_with_error_handling(
                "delete abi",
                sqlx::query!("DELETE FROM abis WHERE name = ?", name).execute(&self.pool),
            )
            .await?;

        if result.rows_affected() == 0 {
            tracing::warn!(name, "ABI not found for deletion.");
            return Err(PersistenceError::NotFound);
        }

        tracing::info!(name, "ABI deleted successfully.");
        Ok(())
    }

    /// Retrieves all ABIs as a vector of (name, abi_json_string) tuples.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_all_abis(&self) -> Result<Vec<(String, String)>, PersistenceError> {
        let abis = self
            .execute_query_with_error_handling(
                "get all abis",
                sqlx::query!("SELECT name, abi FROM abis").fetch_all(&self.pool),
            )
            .await?
            .into_iter()
            .map(|row| (row.name, row.abi))
            .collect::<Vec<_>>();
        tracing::info!(count = abis.len(), "Successfully fetched all ABIs.");
        Ok(abis)
    }

    // Action management operations

    /// Retrieves all actions for a specific network.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_actions(
        &self,
        network_id: &NetworkId,
    ) -> Result<Vec<ActionConfig>, PersistenceError> {
        let action_rows = self
            .execute_query_with_error_handling(
                "query actions",
                sqlx::query_as!(
                    ActionRow,
                    "SELECT action_id, name, config FROM actions WHERE network_id = ?",
                    network_id
                )
                .fetch_all(&self.pool),
            )
            .await?;

        let actions = action_rows
            .into_iter()
            .map(|row| {
                let mut action: ActionConfig = serde_json::from_str(&row.config)
                    .map_err(|e| PersistenceError::SerializationError(e.to_string()))?;
                action.id = Some(row.action_id);
                action.name = row.name;
                Ok(action)
            })
            .collect::<Result<Vec<_>, PersistenceError>>()?;

        tracing::debug!(
            network_id = %network_id,
            action_count = actions.len(),
            "actions retrieved successfully."
        );
        Ok(actions)
    }

    /// Retrieves a specific action by its ID for a given network.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_action_by_id(
        &self,
        network_id: &NetworkId,
        action_id: &ActionId,
    ) -> Result<Option<ActionConfig>, PersistenceError> {
        let action_row = self
            .execute_query_with_error_handling(
                "query action by id",
                sqlx::query_as!(
                    ActionRow,
                    "SELECT action_id, name, config FROM actions WHERE network_id = ? AND \
                     action_id = ?",
                    network_id,
                    action_id
                )
                .fetch_optional(&self.pool),
            )
            .await?;

        if let Some(row) = action_row {
            let mut action: ActionConfig = serde_json::from_str(&row.config)
                .map_err(|e| PersistenceError::SerializationError(e.to_string()))?;
            action.id = Some(row.action_id);
            action.name = row.name;
            tracing::debug!(network_id = %network_id, action_id = %action_id, "Action found.");
            Ok(Some(action))
        } else {
            tracing::debug!(network_id = %network_id, action_id = %action_id, "No action found with given ID.");
            Ok(None)
        }
    }

    /// Retrieves a specific action by its name for a given network.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_action_by_name(
        &self,
        network_id: &NetworkId,
        name: &str,
    ) -> Result<Option<ActionConfig>, PersistenceError> {
        let action_row = self
            .execute_query_with_error_handling(
                "query action by name",
                sqlx::query_as!(
                    ActionRow,
                    r#"SELECT action_id as "action_id!", name, config FROM actions WHERE network_id = ? AND name = ?"#,
                    network_id,
                    name
                )
                .fetch_optional(&self.pool),
            )
            .await?;

        if let Some(row) = action_row {
            let mut action: ActionConfig = serde_json::from_str(&row.config)
                .map_err(|e| PersistenceError::SerializationError(e.to_string()))?;
            action.id = Some(row.action_id);
            action.name = row.name;
            tracing::debug!(network_id = %network_id, name, "Action found.");
            Ok(Some(action))
        } else {
            tracing::debug!(network_id = %network_id, name, "No action found with given name.");
            Ok(None)
        }
    }

    /// Creates a new action for a specific network.
    #[tracing::instrument(skip(self, action), level = "debug")]
    async fn create_action(
        &self,
        network_id: &NetworkId,
        action: ActionConfig,
    ) -> Result<ActionConfig, PersistenceError> {
        let config = serde_json::to_string(&action)
            .map_err(|e| PersistenceError::SerializationError(e.to_string()))?;

        let result = self
            .execute_query_with_error_handling(
                "create action",
                sqlx::query!(
                    "INSERT INTO actions (name, network_id, config) VALUES (?, ?, ?)",
                    action.name,
                    network_id,
                    config
                )
                .execute(&self.pool),
            )
            .await?;

        let new_id = result.last_insert_rowid();
        let mut new_action = action;
        new_action.id = Some(ActionId::from(new_id));

        tracing::info!(
            network_id = %network_id,
            action_name = new_action.name,
            action_id = %new_id,
            "Action created successfully."
        );
        Ok(new_action)
    }

    /// Clears all actions for a specific network.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn clear_actions(&self, network_id: &NetworkId) -> Result<(), PersistenceError> {
        let result = self
            .execute_query_with_error_handling(
                "clear actions",
                sqlx::query!("DELETE FROM actions WHERE network_id = ?", network_id)
                    .execute(&self.pool),
            )
            .await?;

        let deleted_count = result.rows_affected();
        tracing::info!(network_id = %network_id, deleted_count, "actions cleared successfully.");
        Ok(())
    }

    /// Updates an existing action for a specific network.
    #[tracing::instrument(skip(self, action), level = "debug")]
    async fn update_action(
        &self,
        network_id: &NetworkId,
        action: ActionConfig,
    ) -> Result<ActionConfig, PersistenceError> {
        let action_id = action.id.as_ref().ok_or_else(|| {
            PersistenceError::InvalidInput("Action ID is required for update".to_string())
        })?;
        tracing::debug!(network_id = %network_id, action_id = %action_id, "Updating action.");

        let config = serde_json::to_string(&action)
            .map_err(|e| PersistenceError::SerializationError(e.to_string()))?;

        let result = self
            .execute_query_with_error_handling(
                "update action",
                sqlx::query!(
                    "UPDATE actions SET name = ?, config = ? WHERE network_id = ? AND action_id = \
                     ?",
                    action.name,
                    config,
                    network_id,
                    action_id
                )
                .execute(&self.pool),
            )
            .await?;

        if result.rows_affected() == 0 {
            return Err(PersistenceError::NotFound);
        }

        tracing::info!(network_id = %network_id, action_id = %action_id, "Action updated successfully.");
        Ok(action)
    }

    /// Deletes an action by its ID for a specific network.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn delete_action(
        &self,
        network_id: &NetworkId,
        action_id: &ActionId,
    ) -> Result<(), PersistenceError> {
        tracing::debug!(network_id = %network_id, action_id = %action_id, "Deleting action.");

        let result = self
            .execute_query_with_error_handling(
                "delete action",
                sqlx::query!(
                    "DELETE FROM actions WHERE network_id = ? AND action_id = ?",
                    network_id,
                    action_id
                )
                .execute(&self.pool),
            )
            .await?;

        if result.rows_affected() == 0 {
            return Err(PersistenceError::NotFound);
        }

        tracing::info!(network_id = %network_id, action_id = %action_id, "Action deleted successfully.");
        Ok(())
    }

    /// Retrieves all monitors that are associated with a specific action.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_monitors_by_action_id(
        &self,
        network_id: &NetworkId,
        action_id: &ActionId,
    ) -> Result<Vec<MonitorConfig>, PersistenceError> {
        tracing::debug!(network_id = %network_id, action_id = %action_id, "Querying for monitors by action ID.");

        let action_name = self
            .get_action_by_id(network_id, action_id)
            .await?
            .ok_or(PersistenceError::NotFound)?
            .name;

        // Escape LIKE wildcards to prevent incorrect matching.
        let escaped_action_name = action_name.replace('%', "\\%").replace('_', "\\_");
        let like_clause = format!("%\"{}\"%", escaped_action_name);

        let monitor_rows = self
            .execute_query_with_error_handling("query monitors by action id", async {
                sqlx::query_as!(
                    MonitorRow,
                    r#"
                    SELECT 
                        monitor_id as "monitor_id!", 
                        name, 
                        network, 
                        address, 
                        abi_name, 
                        filter_script, 
                        actions,
                        created_at as "created_at!", 
                        updated_at as "updated_at!",
                        status as "status!"
                    FROM monitors 
                    WHERE network = ? AND actions LIKE ? ESCAPE '\'
                    "#,
                    network_id,
                    like_clause
                )
                .fetch_all(&self.pool)
                .await
            })
            .await?;

        let monitors = monitor_rows
            .into_iter()
            .map(|row| {
                let actions: Vec<String> = serde_json::from_str(&row.actions)
                    .map_err(|e| PersistenceError::SerializationError(e.to_string()))?;

                Ok(MonitorConfig {
                    name: row.name,
                    network: NetworkId::from(row.network),
                    address: row.address,
                    abi_name: row.abi_name,
                    filter_script: row.filter_script,
                    actions,
                    status: row.status,
                })
            })
            .collect::<Result<Vec<_>, PersistenceError>>()?;

        tracing::debug!(
            network_id = %network_id,
            action_id = %action_id,
            monitor_count = monitors.len(),
            "Monitors retrieved successfully."
        );
        Ok(monitors)
    }

    async fn enqueue_outbox_batch(
        &self,
        items: Vec<(String, ActionPayload)>,
    ) -> Result<(), PersistenceError> {
        if items.is_empty() {
            return Ok(());
        }

        tracing::debug!(count = items.len(), "Enqueuing outbox batch.");

        // Use a transaction to ensure atomicity and better performance for batch
        // inserts
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| PersistenceError::OperationFailed(e.to_string()))?;

        for chunk in items.chunks(SQLITE_MAX_VARIABLES / OUTBOX_INSERT_COLUMNS) {
            let mut serialized_items = Vec::with_capacity(chunk.len());
            for (name, payload) in chunk {
                let json = serde_json::to_string(payload)
                    .map_err(|e| PersistenceError::SerializationError(e.to_string()))?;
                // Use the idempotency key from the payload to ensure uniqueness
                let idempotency_key = payload.idempotency_key();
                serialized_items.push((name, json, idempotency_key));
            }

            let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(
                "INSERT OR IGNORE INTO outbox (action_name, payload, idempotency_key) ",
            );

            query_builder.push_values(
                serialized_items,
                |mut b, (action_name, payload_json, idempotency_key)| {
                    b.push_bind(action_name).push_bind(payload_json).push_bind(idempotency_key);
                },
            );

            let query = query_builder.build();
            let inserted = query
                .execute(&mut *tx)
                .await
                .map_err(|e| PersistenceError::OperationFailed(e.to_string()))?;

            // Check how many rows were ignored due to the idempotency key constraint
            let ignored = chunk.len() as u64 - inserted.rows_affected();
            if ignored > 0 {
                tracing::debug!(ignored, "Dropped duplicate outbox items by idempotency key.");
            }
        }

        tx.commit().await.map_err(|e| PersistenceError::OperationFailed(e.to_string()))?;

        Ok(())
    }

    async fn get_pending_outbox(&self, limit: i64) -> Result<Vec<OutboxItem>, PersistenceError> {
        self.execute_query_with_error_handling(
            "fetch pending outbox",
            sqlx::query_as!(
                OutboxRow,
                r#"
            SELECT 
                id as "id!", 
                action_name as "action_name!", 
                payload as "payload!", 
                retries as "retries!"
            FROM outbox 
            ORDER BY id ASC 
            LIMIT ?
            "#,
                limit
            )
            .fetch_all(&self.pool),
        )
        .await?
        .into_iter()
        .map(OutboxItem::try_from)
        .collect()
    }

    async fn delete_outbox_items_batch(&self, ids: &[i64]) -> Result<(), PersistenceError> {
        if ids.is_empty() {
            return Ok(());
        }

        tracing::debug!(count = ids.len(), "Deleting outbox batch.");

        // Use a transaction to ensure atomicity and better performance for batch
        // deletes
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| PersistenceError::OperationFailed(e.to_string()))?;

        for chunk in ids.chunks(SQLITE_MAX_VARIABLES) {
            let mut query_builder: QueryBuilder<Sqlite> =
                QueryBuilder::new("DELETE FROM outbox WHERE id IN (");

            let mut separated = query_builder.separated(", ");
            for id in chunk {
                separated.push_bind(*id);
            }
            separated.push_unseparated(")");

            let query = query_builder.build();
            query
                .execute(&mut *tx)
                .await
                .map_err(|e| PersistenceError::OperationFailed(e.to_string()))?;
        }

        tx.commit().await.map_err(|e| PersistenceError::OperationFailed(e.to_string()))?;

        Ok(())
    }

    async fn increment_outbox_retries(&self, id: i64) -> Result<(), PersistenceError> {
        self.execute_query_with_error_handling(
            "increment outbox retry",
            sqlx::query!(
                "UPDATE outbox SET retries = retries + 1, last_attempt = CURRENT_TIMESTAMP WHERE \
                 id = ?",
                id
            )
            .execute(&self.pool),
        )
        .await?;
        Ok(())
    }
}
