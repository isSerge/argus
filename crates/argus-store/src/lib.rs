//! SQLite-backed persistence implementation for Argus.

use std::str::FromStr;

use sqlx::{SqlitePool, sqlite::SqliteConnectOptions};

pub mod app_repository;
pub mod key_value_store;

use argus_core::persistence::error::PersistenceError;

/// A concrete implementation of the AppRepository using SQLite.
pub struct SqliteStateRepository {
    /// The SQLite connection pool used for database operations.
    pool: SqlitePool,
}

impl SqliteStateRepository {
    /// Creates a new instance of SqliteStateRepository with the provided
    /// database URL. This will create the database file if it does not
    /// exist.
    #[tracing::instrument(level = "info")]
    pub async fn new(database_url: &str) -> Result<Self, PersistenceError> {
        let options = SqliteConnectOptions::from_str(database_url)
            .map_err(|e| PersistenceError::InvalidInput(e.to_string()))?
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await.map_err(|e| {
            PersistenceError::OperationFailed(format!("Failed to connect to database: {}", e))
        })?;
        tracing::info!(database_url, "Successfully connected to SQLite database.");
        Ok(Self { pool })
    }

    /// Runs database migrations.
    #[tracing::instrument(skip(self), level = "info")]
    pub async fn run_migrations(&self) -> Result<(), PersistenceError> {
        sqlx::migrate!("../../migrations").run(&self.pool).await.map_err(|e| {
            tracing::error!(error = %e, "Failed to run database migrations.");
            PersistenceError::MigrationError(e.to_string())
        })?;
        tracing::info!("Database migrations completed successfully.");
        Ok(())
    }

    /// Gets access to the underlying connection pool for advanced operations.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Closes the connection pool gracefully.
    #[tracing::instrument(skip(self), level = "info")]
    pub async fn close(&self) {
        self.pool.close().await;
        tracing::info!("SQLite connection pool closed successfully.");
    }

    /// Internal helper to execute a PRAGMA command with error handling
    async fn execute_pragma(&self, pragma: &str, operation: &str) -> Result<(), PersistenceError> {
        sqlx::query(pragma)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, pragma = %pragma, operation = %operation, "Failed to execute PRAGMA command.");
                PersistenceError::OperationFailed(e.to_string())
            })?;
        Ok(())
    }

    /// Performs a WAL checkpoint with the specified mode
    async fn checkpoint_wal(&self, mode: &str) -> Result<(), PersistenceError> {
        let allowed_modes = ["PASSIVE", "TRUNCATE", "RESTART", "RESTART_OR_TRUNCATE"];
        if !allowed_modes.contains(&mode) {
            return Err(PersistenceError::InvalidInput(format!(
                "Invalid WAL checkpoint mode: {}",
                mode
            )));
        }
        let pragma = format!("PRAGMA wal_checkpoint({mode})");
        self.execute_pragma(&pragma, &format!("WAL checkpoint {mode}")).await
    }

    /// Sets the synchronous mode
    async fn set_synchronous_mode(&self, mode: &str) -> Result<(), PersistenceError> {
        let allowed_modes = ["OFF", "NORMAL", "FULL"];
        if !allowed_modes.contains(&mode) {
            return Err(PersistenceError::InvalidInput(format!(
                "Invalid synchronous mode: {}",
                mode
            )));
        }
        let pragma = format!("PRAGMA synchronous = {mode}");
        self.execute_pragma(&pragma, &format!("set synchronous mode to {mode}")).await
    }

    /// Helper to execute database queries with consistent error handling
    async fn execute_query_with_error_handling<F, T, E>(
        &self,
        operation: &str,
        query_fn: F,
    ) -> Result<T, PersistenceError>
    where
        F: std::future::Future<Output = Result<T, E>>,
        E: std::error::Error,
    {
        query_fn.await.map_err(|e| {
            tracing::error!(error = %e, operation = %operation, "Database operation failed.");
            PersistenceError::OperationFailed(e.to_string())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use alloy::primitives::{Address, B256, TxHash};
    use argus_core::{
        action_dispatcher::ActionPayload,
        models::{
            NetworkId,
            action::{ActionPolicy, AggregationPolicy, ThrottlePolicy},
            monitor::MonitorConfig,
            monitor_match::{LogDetails, MonitorMatch},
            notification::NotificationMessage,
        },
        persistence::traits::{AppRepository, KeyValueStore},
        test_utils::ActionBuilder,
    };
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use super::*;

    async fn setup_test_db() -> SqliteStateRepository {
        let repo = SqliteStateRepository::new("sqlite::memory:")
            .await
            .expect("Failed to connect to in-memory db");
        repo.run_migrations().await.expect("Failed to run migrations");
        repo
    }

    #[tokio::test]
    async fn test_get_and_set_last_processed_block() {
        let repo = setup_test_db().await;
        let network = &NetworkId::from("mainnet");

        // Initially, should be None
        let block = repo.get_last_processed_block(network).await.unwrap();
        assert!(block.is_none());
        assert!(repo.get_last_processed_block_tip(network).await.unwrap().is_none());

        // Set a block number without a hash
        repo.set_last_processed_block(network, 12345, None).await.unwrap();

        // Retrieve it again
        let block = repo.get_last_processed_block(network).await.unwrap();
        assert_eq!(block, Some(12345));
        assert!(repo.get_last_processed_block_tip(network).await.unwrap().is_none());

        // Update it with a hash
        let hash = B256::from([0xAA; 32]);
        repo.set_last_processed_block(network, 54321, Some(hash)).await.unwrap();

        // Retrieve the updated value
        let block = repo.get_last_processed_block(network).await.unwrap();
        assert_eq!(block, Some(54321));
        assert_eq!(repo.get_last_processed_block_tip(network).await.unwrap(), Some((54321, hash)));
    }

    #[tokio::test]
    async fn test_cleanup_and_flush_operations() {
        let repo = setup_test_db().await;
        let network = &NetworkId::from("testnet");

        // Set some data
        repo.set_last_processed_block(network, 100, None).await.unwrap();

        // Test flush operation
        repo.flush().await.unwrap();

        // Test cleanup operation
        repo.cleanup().await.unwrap();

        // Verify data integrity after cleanup
        let block = repo.get_last_processed_block(network).await.unwrap();
        assert_eq!(block, Some(100));
    }

    #[tokio::test]
    async fn test_emergency_state_saving() {
        let repo = setup_test_db().await;
        let network = &NetworkId::from("emergency_test");

        // Save emergency state
        repo.save_emergency_state(network, 555, "Test emergency shutdown").await.unwrap();

        // Verify the state was saved
        let block = repo.get_last_processed_block(network).await.unwrap();
        assert_eq!(block, Some(555));
    }

    #[tokio::test]
    async fn test_pragma_helper_methods() {
        let repo = setup_test_db().await;

        // Test checkpoint WAL (should not fail even on empty database)
        repo.checkpoint_wal("PASSIVE").await.unwrap();
        repo.checkpoint_wal("TRUNCATE").await.unwrap();

        // Test synchronous mode changes
        repo.set_synchronous_mode("FULL").await.unwrap();
        repo.set_synchronous_mode("NORMAL").await.unwrap();
        repo.set_synchronous_mode("OFF").await.unwrap();
    }

    #[tokio::test]
    async fn test_emergency_state_with_no_prior_state() {
        let repo = setup_test_db().await;
        let network = &NetworkId::from("fresh_network");

        // Verify no prior state exists
        let initial_state = repo.get_last_processed_block(network).await.unwrap();
        assert!(initial_state.is_none());

        // Save emergency state during first run
        // This simulates a scenario where the application is starting fresh
        // and needs to save an emergency state immediately.
        repo.save_emergency_state(network, 42, "Emergency during first run").await.unwrap();

        // Verify the emergency state was saved
        let saved_state = repo.get_last_processed_block(network).await.unwrap();
        assert_eq!(saved_state, Some(42));
    }

    #[tokio::test]
    async fn test_monitor_management_operations() {
        let repo = setup_test_db().await;
        let network_id = NetworkId::from("ethereum");

        // Create ABIs for testing
        repo.create_abi("usdc", "[]").await.unwrap();
        repo.create_abi("test", "[]").await.unwrap();

        // Initially, should have no monitors
        let monitors = repo.get_monitors(&network_id).await.unwrap();
        assert!(monitors.is_empty());

        // Create test monitors
        let test_monitors = vec![
            MonitorConfig::from_config(
                "USDC Transfer Monitor".to_string(),
                network_id.clone(),
                Some("0xa0b86a33e6441b38d4b5e5bfa1bf7a5eb70c5b1e".to_string()),
                Some("usdc".to_string()),
                r#"log.name == "Transfer" && parse_bigint(log.params.value) > parse_bigint("1000000000")"#
                    .to_string(),
                vec!["test-action".to_string()],
            ),
            MonitorConfig::from_config(
                "Simple Transfer Monitor".to_string(),
                network_id.clone(),
                Some("0x7a250d5630b4cf539739df2c5dacb4c659f2488d".to_string()),
                Some("test".to_string()),
                r#"log.name == "Transfer""#.to_string(),
                vec![],
            ),
            MonitorConfig::from_config(
                "Native ETH Monitor".to_string(),
                network_id.clone(),
                None, // No address for transaction-level monitor
                None,
                r#"parse_bigint(tx.value) > parse_bigint("1000000000000000000")"#.to_string(),
                vec!["eth-action".to_string(), "another-action".to_string()],
            ),
        ];

        // Add monitors
        repo.add_monitors(&network_id, test_monitors.clone()).await.unwrap();

        // Retrieve monitors and verify
        let stored_monitors = repo.get_monitors(&network_id).await.unwrap();
        assert_eq!(stored_monitors.len(), 3);

        // Check USDC monitor
        let usdc_monitor =
            stored_monitors.iter().find(|m| m.name == "USDC Transfer Monitor").unwrap();
        assert_eq!(usdc_monitor.network, network_id);
        assert_eq!(
            usdc_monitor.address,
            Some("0xa0b86a33e6441b38d4b5e5bfa1bf7a5eb70c5b1e".to_string())
        );
        assert!(usdc_monitor.id > 0);
        assert_eq!(usdc_monitor.actions, vec!["test-action".to_string()]);

        // Check Native ETH monitor
        let eth_monitor = stored_monitors.iter().find(|m| m.name == "Native ETH Monitor").unwrap();
        assert_eq!(eth_monitor.network, network_id);
        assert_eq!(eth_monitor.address, None);
        assert!(eth_monitor.id > 0);
        assert_eq!(
            eth_monitor.actions,
            vec!["eth-action".to_string(), "another-action".to_string()]
        );

        // Clear monitors
        repo.clear_monitors(&network_id).await.unwrap();

        // Verify monitors are cleared
        let monitors_after_clear = repo.get_monitors(&network_id).await.unwrap();
        assert!(monitors_after_clear.is_empty());
    }

    #[tokio::test]
    async fn test_monitor_network_isolation() {
        let repo = setup_test_db().await;
        let network1 = &NetworkId::from("ethereum");
        let network2 = &NetworkId::from("polygon");

        // Create ABI for testing
        repo.create_abi("test", "[]").await.unwrap();

        // Create monitors for different networks
        let ethereum_monitors = vec![MonitorConfig::from_config(
            "Ethereum Monitor".to_string(),
            network1.clone(),
            Some("0x1111111111111111111111111111111111111111".to_string()),
            Some("test".to_string()),
            "true".to_string(),
            vec![],
        )];

        let polygon_monitors = vec![MonitorConfig::from_config(
            "Polygon Monitor".to_string(),
            network2.clone(),
            Some("0x2222222222222222222222222222222222222222".to_string()),
            Some("test".to_string()),
            "true".to_string(),
            vec![],
        )];

        // Add monitors to different networks
        repo.add_monitors(network1, ethereum_monitors).await.unwrap();
        repo.add_monitors(network2, polygon_monitors).await.unwrap();

        // Verify network isolation
        let eth_monitors = repo.get_monitors(network1).await.unwrap();
        let poly_monitors = repo.get_monitors(network2).await.unwrap();

        assert_eq!(eth_monitors.len(), 1);
        assert_eq!(poly_monitors.len(), 1);
        assert_eq!(eth_monitors[0].name, "Ethereum Monitor");
        assert_eq!(poly_monitors[0].name, "Polygon Monitor");

        // Clear one network shouldn't affect the other
        repo.clear_monitors(network1).await.unwrap();

        let eth_monitors_after_clear = repo.get_monitors(network1).await.unwrap();
        let poly_monitors_after_clear = repo.get_monitors(network2).await.unwrap();

        assert!(eth_monitors_after_clear.is_empty());
        assert_eq!(poly_monitors_after_clear.len(), 1);
    }

    #[tokio::test]
    async fn test_monitor_network_validation() {
        let repo = setup_test_db().await;
        let network_id = &NetworkId::from("ethereum");

        // Create ABI for testing
        repo.create_abi("test", "[]").await.unwrap();

        // Create monitor with wrong network
        let wrong_network_monitors = vec![MonitorConfig::from_config(
            "Wrong Network Monitor".to_string(),
            NetworkId::from("polygon"), // Different from network_id
            Some("0x1111111111111111111111111111111111111111".to_string()),
            Some("test".to_string()),
            "true".to_string(),
            vec![],
        )];

        // Should fail due to network mismatch
        let result = repo.add_monitors(network_id, wrong_network_monitors).await;
        assert!(result.is_err());

        // Verify error message contains network information
        let error = result.unwrap_err();
        match error {
            PersistenceError::InvalidInput(msg) => {
                assert!(msg.contains("Wrong Network Monitor"));
                assert!(msg.contains("polygon"));
                assert!(msg.contains("ethereum"));
            }
            _ => panic!("Expected InvalidInput error with network mismatch message"),
        }

        // Verify no monitors were added
        let monitors = repo.get_monitors(network_id).await.unwrap();
        assert!(monitors.is_empty());
    }

    #[tokio::test]
    async fn test_monitor_empty_operations() {
        let repo = setup_test_db().await;
        let network_id = &NetworkId::default();

        // Test adding empty vector
        repo.add_monitors(network_id, vec![]).await.unwrap();
        let monitors = repo.get_monitors(network_id).await.unwrap();
        assert!(monitors.is_empty());

        // Test clearing when no monitors exist
        repo.clear_monitors(network_id).await.unwrap();
        let monitors_after_clear = repo.get_monitors(network_id).await.unwrap();
        assert!(monitors_after_clear.is_empty());
    }

    #[tokio::test]
    async fn test_monitor_transaction_atomicity() {
        let repo = setup_test_db().await;
        let network_id = &NetworkId::from("ethereum");

        // Create ABI for testing
        repo.create_abi("test", "[]").await.unwrap();

        // Create a mix of valid and invalid monitors (invalid due to network mismatch)
        let mixed_monitors = vec![
            MonitorConfig::from_config(
                "Valid Monitor".to_string(),
                network_id.clone(),
                Some("0x1111111111111111111111111111111111111111".to_string()),
                Some("test".to_string()),
                "true".to_string(),
                vec![],
            ),
            MonitorConfig::from_config(
                "Invalid Monitor".to_string(),
                NetworkId::from("wrong_network"), // This will cause failure
                Some("0x2222222222222222222222222222222222222222".to_string()),
                Some("test".to_string()),
                "true".to_string(),
                vec![],
            ),
        ];

        // Should fail due to network validation
        let result = repo.add_monitors(network_id, mixed_monitors).await;
        assert!(result.is_err());

        // Verify no monitors were added (transaction rolled back)
        let monitors = repo.get_monitors(network_id).await.unwrap();
        assert!(monitors.is_empty());
    }

    #[tokio::test]
    async fn test_monitor_large_script_handling() {
        let repo = setup_test_db().await;
        let network_id = &NetworkId::from("ethereum");

        // Create ABI for testing
        repo.create_abi("test", "[]").await.unwrap();

        // Create monitor with large filter script
        let large_script = "a".repeat(10000); // 10KB script
        let monitor_with_large_script = vec![MonitorConfig::from_config(
            "Large Script Monitor".to_string(),
            network_id.clone(),
            Some("0x1111111111111111111111111111111111111111".to_string()),
            Some("test".to_string()),
            large_script.clone(),
            vec![],
        )];

        // Should handle large scripts
        repo.add_monitors(network_id, monitor_with_large_script).await.unwrap();

        // Verify the script was stored correctly
        let stored_monitors = repo.get_monitors(network_id).await.unwrap();
        assert_eq!(stored_monitors.len(), 1);
        assert_eq!(stored_monitors[0].filter_script, large_script);
        assert_eq!(stored_monitors[0].filter_script.len(), 10000);
    }

    #[tokio::test]
    async fn test_action_management_operations() {
        let repo = setup_test_db().await;
        let network_id = &NetworkId::from("ethereum");

        // Initially, should have no actions
        let actions = repo.get_actions(network_id).await.unwrap();
        assert!(actions.is_empty());

        // Create test actions
        let action = ActionBuilder::new("Test Slack")
            .slack_config("https://hooks.slack.com/services/123")
            .build();

        // Add action to the repository
        repo.create_action(network_id, action).await.unwrap();

        // Retrieve actions and verify
        let stored_actions = repo.get_actions(network_id).await.unwrap();
        assert_eq!(stored_actions.len(), 1);
        assert_eq!(stored_actions[0].name, "Test Slack");

        // Clear actions
        repo.clear_actions(network_id).await.unwrap();

        // Verify actions are cleared
        let actions_after_clear = repo.get_actions(network_id).await.unwrap();
        assert!(actions_after_clear.is_empty());
    }

    #[tokio::test]
    async fn test_action_network_isolation() {
        let repo = setup_test_db().await;
        let network1 = &NetworkId::from("ethereum");
        let network2 = &NetworkId::from("polygon");

        // Create actions for different networks
        let ethereum_action = ActionBuilder::new("Ethereum Slack")
            .slack_config("https://hooks.slack.com/services/123")
            .build();

        let polygon_action = ActionBuilder::new("Polygon Discord")
            .discord_config("https://discord.com/api/webhooks/poly")
            .build();

        // Add actions to different networks
        repo.create_action(network1, ethereum_action).await.unwrap();
        repo.create_action(network2, polygon_action).await.unwrap();

        // Verify network isolation
        let eth_actions = repo.get_actions(network1).await.unwrap();
        let poly_actions = repo.get_actions(network2).await.unwrap();

        assert_eq!(eth_actions.len(), 1);
        assert_eq!(poly_actions.len(), 1);
        assert_eq!(eth_actions[0].name, "Ethereum Slack");
        assert_eq!(poly_actions[0].name, "Polygon Discord");

        // Clear one network, should not affect the other
        repo.clear_actions(network1).await.unwrap();
        let eth_actions_after_clear = repo.get_actions(network1).await.unwrap();
        let poly_actions_after_clear = repo.get_actions(network2).await.unwrap();

        assert!(eth_actions_after_clear.is_empty());
        assert_eq!(poly_actions_after_clear.len(), 1);
    }

    #[tokio::test]
    async fn test_action_transaction_atomicity() {
        let repo = setup_test_db().await;
        let network_id = &NetworkId::from("ethereum");

        // Create a batch of actions where one has a name that is too long, causing a
        // DB constraint error. This test is a bit contrived as we can't easily
        // create an invalid JSON, but we can simulate a constraint violation.
        // Here, we'll rely on the UNIQUE constraint.
        let actions1 = [
            ActionBuilder::new("Unique Action")
                .slack_config("https://hooks.slack.com/services/unique")
                .build(),
            ActionBuilder::new("Another Unique Action")
                .slack_config("https://hooks.slack.com/services/another")
                .build(),
        ];
        let actions2 = [
            ActionBuilder::new("Third Action")
                .slack_config("https://hooks.slack.com/services/third")
                .build(),
            ActionBuilder::new("Unique Action") // Duplicate name to violate UNIQUE constraint
                .slack_config("https://hooks.slack.com/services/duplicate")
                .build(),
        ];

        // This should succeed
        // repo.add_actions(network_id, actions1).await.unwrap();
        repo.create_action(network_id, actions1[0].clone()).await.unwrap();
        repo.create_action(network_id, actions1[1].clone()).await.unwrap();
        assert_eq!(repo.get_actions(network_id).await.unwrap().len(), 2);

        // This should fail due to the duplicate name violating the UNIQUE constraint
        let result = repo.create_action(network_id, actions2[1].clone()).await;
        assert!(result.is_err());

        // Verify no new actions were added (transaction rolled back)
        // The count should still be 2 from the first successful insert.
        assert_eq!(repo.get_actions(network_id).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_action_policy_persistence() {
        let repo = setup_test_db().await;
        let network_id = &NetworkId::from("testnet_policy");

        let aggregation_policy = ActionPolicy::Aggregation(AggregationPolicy {
            window_secs: Duration::from_secs(60),
            template: NotificationMessage {
                title: "Aggregated Alert".to_string(),
                body: "Multiple events occurred.".to_string(),
            },
        });

        let test_action = ActionBuilder::new("Policy Action")
            .slack_config("https://hooks.slack.com/services/policy")
            .policy(aggregation_policy.clone())
            .build();

        // Add action with policy
        repo.create_action(network_id, test_action.clone()).await.unwrap();

        // Retrieve and verify
        let stored_actions = repo.get_actions(network_id).await.unwrap();
        assert_eq!(stored_actions.len(), 1);
        let retrieved_action = &stored_actions[0];

        assert_eq!(retrieved_action.name, test_action.name);
        assert!(retrieved_action.policy.is_some());
        assert_eq!(retrieved_action.policy.as_ref().unwrap(), &aggregation_policy);

        // Test with a throttle policy as well
        let throttle_policy = ActionPolicy::Throttle(ThrottlePolicy {
            max_count: 5,
            time_window_secs: Duration::from_secs(10),
        });

        let throttled_action = ActionBuilder::new("Throttled Action")
            .discord_config("https://discord.com/api/webhooks/throttle")
            .policy(throttle_policy.clone())
            .build();

        repo.create_action(network_id, throttled_action).await.unwrap();

        let stored_actions_after_throttle = repo.get_actions(network_id).await.unwrap();
        assert_eq!(stored_actions_after_throttle.len(), 2);

        let retrieved_throttled_action = stored_actions_after_throttle
            .into_iter()
            .find(|n| n.name == "Throttled Action")
            .unwrap();

        assert!(retrieved_throttled_action.policy.is_some());
        assert_eq!(retrieved_throttled_action.policy.unwrap(), throttle_policy);
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    struct TestJsonState {
        id: u32,
        message: String,
        is_active: bool,
    }

    #[tokio::test]
    async fn test_json_state_persistence() {
        let repo = setup_test_db().await;
        let key = "test_generic_state";

        // Initially, should be None
        let retrieved: Option<TestJsonState> = repo.get_json_state(key).await.unwrap();
        assert!(retrieved.is_none());

        // Set a state
        let original_state =
            TestJsonState { id: 1, message: "Hello, World!".to_string(), is_active: true };
        repo.set_json_state(key, &original_state).await.unwrap();

        // Retrieve it again
        let retrieved_state: Option<TestJsonState> = repo.get_json_state(key).await.unwrap();
        assert_eq!(retrieved_state, Some(original_state.clone()));

        // Update it
        let updated_state =
            TestJsonState { id: 1, message: "Updated message.".to_string(), is_active: false };
        repo.set_json_state(key, &updated_state).await.unwrap();

        // Retrieve the updated value
        let retrieved_updated_state: Option<TestJsonState> =
            repo.get_json_state(key).await.unwrap();
        assert_eq!(retrieved_updated_state, Some(updated_state.clone()));

        // Test a different key, should be None
        let non_existent: Option<TestJsonState> =
            repo.get_json_state("non_existent_key").await.unwrap();
        assert!(non_existent.is_none());
    }

    #[tokio::test]
    async fn test_outbox_dedup_drops_duplicate_idempotency_key() {
        let repo = setup_test_db().await;

        let make_payload = |tx_hash: TxHash| {
            let mm = MonitorMatch::builder(1, "m".into(), "a".into(), 10, tx_hash)
                .log_match(
                    LogDetails {
                        address: Address::ZERO,
                        log_index: 0,
                        name: "Transfer".into(),
                        params: json!({}),
                    },
                    json!({}),
                )
                .decoded_call(None)
                .build();
            ActionPayload::Single(mm)
        };

        let payload = make_payload(TxHash::with_last_byte(1));

        // Enqueue the identical payload twice (two separate ingestor cycles).
        repo.enqueue_outbox_batch(vec![("a".into(), payload.clone())]).await.unwrap();
        repo.enqueue_outbox_batch(vec![("a".into(), payload.clone())]).await.unwrap();

        let pending = repo.get_pending_outbox(100).await.unwrap();
        assert_eq!(pending.len(), 1, "duplicate idempotency key should be dropped");
    }

    #[tokio::test]
    async fn test_outbox_dedup_keeps_distinct_keys() {
        let repo = setup_test_db().await;

        let make_payload = |tx_hash: TxHash| {
            let mm = MonitorMatch::builder(1, "m".into(), "a".into(), 10, tx_hash)
                .log_match(
                    LogDetails {
                        address: Address::ZERO,
                        log_index: 0,
                        name: "Transfer".into(),
                        params: json!({}),
                    },
                    json!({}),
                )
                .decoded_call(None)
                .build();
            ActionPayload::Single(mm)
        };

        // Distinct transaction hashes -> distinct deterministic ids.
        let items = vec![
            ("a".into(), make_payload(TxHash::with_last_byte(1))),
            ("a".into(), make_payload(TxHash::with_last_byte(2))),
        ];
        repo.enqueue_outbox_batch(items).await.unwrap();

        let pending = repo.get_pending_outbox(100).await.unwrap();
        assert_eq!(pending.len(), 2, "distinct keys should both be kept");
    }
}
