//! This module contains the persistence traits for the Argus application.

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};

use super::error::PersistenceError;
use crate::{
    action_dispatcher::ActionPayload,
    models::{
        ActionId, NetworkId,
        action::ActionConfig,
        monitor::{Monitor, MonitorConfig, MonitorStatus},
    },
};
use alloy::primitives::B256;

/// Represents the application's persistence layer interface.
#[cfg_attr(any(test, feature = "test-utils"), mockall::automock)]
#[async_trait]
pub trait AppRepository: Send + Sync {
    /// Retrieves the last processed block number for a given network.
    async fn get_last_processed_block(
        &self,
        network_id: &NetworkId,
    ) -> Result<Option<u64>, PersistenceError>;

    /// Retrieves the last processed block number and hash for a given network.
    /// Returns `None` when no block was processed or the persisted state
    /// predates hash tracking.
    async fn get_last_processed_block_tip(
        &self,
        network_id: &NetworkId,
    ) -> Result<Option<(u64, B256)>, PersistenceError>;

    /// Sets the last processed block number and hash for a given network.
    async fn set_last_processed_block(
        &self,
        network_id: &NetworkId,
        block_number: u64,
        block_hash: Option<B256>,
    ) -> Result<(), PersistenceError>;

    /// Performs any necessary cleanup operations before shutdown.
    async fn cleanup(&self) -> Result<(), PersistenceError>;

    /// Ensures all pending writes are flushed to disk.
    async fn flush(&self) -> Result<(), PersistenceError>;

    /// Saves emergency state during shutdown (e.g., partial progress).
    async fn save_emergency_state(
        &self,
        network_id: &NetworkId,
        block_number: u64,
        note: &str,
    ) -> Result<(), PersistenceError>;

    // Monitor management operations:

    /// Retrieves all monitors for a specific network.
    async fn get_monitors(&self, network_id: &NetworkId) -> Result<Vec<Monitor>, PersistenceError>;

    /// Retrieves a specific monitor by its ID for a given network.
    async fn get_monitor_by_id(
        &self,
        network_id: &NetworkId,
        monitor_id: &str,
    ) -> Result<Option<Monitor>, PersistenceError>;

    /// Adds multiple monitors for a specific network.
    async fn add_monitors(
        &self,
        network_id: &NetworkId,
        monitors: Vec<MonitorConfig>,
    ) -> Result<(), PersistenceError>;

    /// Clears all monitors for a specific network.
    async fn clear_monitors(&self, network_id: &NetworkId) -> Result<(), PersistenceError>;

    /// Deletes a monitor by its ID for a specific network.
    async fn delete_monitor(
        &self,
        network_id: &NetworkId,
        monitor_id: &str,
    ) -> Result<(), PersistenceError>;

    /// Updates an existing monitor for a specific network.
    async fn update_monitor(
        &self,
        network_id: &NetworkId,
        monitor_id: &str,
        monitor: MonitorConfig,
    ) -> Result<(), PersistenceError>;

    /// Updates the status of a monitor for a specific network.
    async fn update_monitor_status(
        &self,
        network_id: &NetworkId,
        monitor_id: &str,
        status: MonitorStatus,
    ) -> Result<(), PersistenceError>;

    // ABI management operations:

    /// Creates a new ABI.
    async fn create_abi(&self, name: &str, abi: &str) -> Result<(), PersistenceError>;

    /// Retrieves an ABI by its name.
    async fn get_abi(&self, name: &str) -> Result<Option<String>, PersistenceError>;

    /// Lists all available ABI names.
    async fn list_abis(&self) -> Result<Vec<String>, PersistenceError>;

    /// Deletes an ABI by its name.
    async fn delete_abi(&self, name: &str) -> Result<(), PersistenceError>;

    /// Retrieves all ABIs as a vector of (name, abi_json_string) tuples.
    async fn get_all_abis(&self) -> Result<Vec<(String, String)>, PersistenceError>;

    // Action management operations:

    /// Retrieves all actions for a specific network.
    async fn get_actions(
        &self,
        network_id: &NetworkId,
    ) -> Result<Vec<ActionConfig>, PersistenceError>;

    /// Retrieves a specific action by its ID for a given network.
    async fn get_action_by_id(
        &self,
        network_id: &NetworkId,
        action_id: &ActionId,
    ) -> Result<Option<ActionConfig>, PersistenceError>;

    /// Retrieves a specific action by its name for a given network.
    async fn get_action_by_name(
        &self,
        network_id: &NetworkId,
        name: &str,
    ) -> Result<Option<ActionConfig>, PersistenceError>;

    /// Creates a new action for a specific network.
    async fn create_action(
        &self,
        network_id: &NetworkId,
        action: ActionConfig,
    ) -> Result<ActionConfig, PersistenceError>;

    /// Clears all actions for a specific network.
    async fn clear_actions(&self, network_id: &NetworkId) -> Result<(), PersistenceError>;

    /// Updates an existing action for a specific network.
    async fn update_action(
        &self,
        network_id: &NetworkId,
        action: ActionConfig,
    ) -> Result<ActionConfig, PersistenceError>;

    /// Deletes an action by its ID for a specific network.
    async fn delete_action(
        &self,
        network_id: &NetworkId,
        action_id: &ActionId,
    ) -> Result<(), PersistenceError>;

    /// Retrieves all monitors that are associated with a specific action.
    async fn get_monitors_by_action_id(
        &self,
        network_id: &NetworkId,
        action_id: &ActionId,
    ) -> Result<Vec<crate::models::monitor::MonitorConfig>, PersistenceError>;

    // Outbox management operations:

    /// Enqueues a batch of actions to be executed later by adding them to the
    /// outbox.
    async fn enqueue_outbox_batch(
        &self,
        items: Vec<(String, ActionPayload)>,
    ) -> Result<(), PersistenceError>;

    /// Retrieves pending outbox items up to a specified limit.
    async fn get_pending_outbox(&self, limit: i64) -> Result<Vec<OutboxItem>, PersistenceError>;

    /// Deletes a batch of outbox items by their IDs.
    async fn delete_outbox_items_batch(&self, ids: &[i64]) -> Result<(), PersistenceError>;

    /// Increments the retry count for an outbox item by its ID.
    async fn increment_outbox_retries(&self, id: i64) -> Result<(), PersistenceError>;
}

// TODO: probably move elsewhere
/// Represents an item in the outbox.
pub struct OutboxItem {
    /// Unique identifier for the outbox item.
    pub id: i64, // TODO: double check if i64 is the right type for this ID
    /// Name of the action to be executed.
    pub action_name: String,
    /// JSON-serializable payload for the action.
    pub payload: ActionPayload,
    /// Number of times this outbox item has been retried.
    pub retries: i32,
}

/// Represents a generic key-value store for JSON-serializable objects.
#[cfg_attr(any(test, feature = "test-utils"), mockall::automock)]
#[async_trait]
pub trait KeyValueStore: Send + Sync {
    /// Retrieves a JSON-serializable state object by its key.
    async fn get_json_state<T: DeserializeOwned + Send + Sync + 'static>(
        &self,
        key: &str,
    ) -> Result<Option<T>, PersistenceError>;

    /// Sets or updates a JSON-serializable state object by its key.
    async fn set_json_state<T: Serialize + Send + Sync + 'static>(
        &self,
        key: &str,
        value: &T,
    ) -> Result<(), PersistenceError>;

    /// Retrieves all JSON-serializable state objects matching a key prefix.
    async fn get_all_json_states_by_prefix<T: DeserializeOwned + Send + Sync + 'static>(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, T)>, PersistenceError>;
}
