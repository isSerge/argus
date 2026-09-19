//! Integration tests for the persistence layer

use alloy::primitives::{B256, TxHash};
use argus_core::{
    action_dispatcher::ActionPayload,
    models::{
        NetworkId, action::ActionConfig, monitor::MonitorConfig, monitor_match::MonitorMatch,
    },
    persistence::traits::AppRepository,
    test_utils::ActionBuilder,
};
use argus_store::SqliteStateRepository;

async fn setup_db() -> SqliteStateRepository {
    let repo = SqliteStateRepository::new("sqlite::memory:")
        .await
        .expect("Failed to set up in-memory database");
    repo.run_migrations().await.expect("Failed to run migrations");
    repo
}

static TX_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn create_test_monitor_match(action_name: &str) -> MonitorMatch {
    let n = TX_NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut bytes = [0u8; 32];
    bytes[24..].copy_from_slice(&n.to_be_bytes());
    MonitorMatch::builder(
        1,
        "test-monitor".to_string(),
        action_name.to_string(),
        100,
        TxHash::from(bytes),
    )
    .transaction_match(serde_json::json!({"test": "data"}))
    .build()
}

fn create_test_monitor(name: &str, network: NetworkId) -> MonitorConfig {
    MonitorConfig::from_config(
        name.to_string(),
        network,
        Some("0x123".to_string()),
        Some("test".to_string()),
        "log.name == \"Test\"".to_string(),
        vec![],
    )
}

fn create_test_action(name: &str) -> ActionConfig {
    ActionBuilder::new(name).discord_config("https://discord.com/api/webhooks/test").build()
}

#[tokio::test]
async fn test_monitor_lifecycle() {
    let repo = setup_db().await;
    repo.create_abi("test", "[]").await.unwrap();
    let network_id = NetworkId::from("ethereum");

    // 1. Initially, no monitors should exist
    let initial_monitors = repo.get_monitors(&network_id).await.unwrap();
    assert!(initial_monitors.is_empty());

    // 2. Add monitors
    let monitors_to_add = vec![
        create_test_monitor("Monitor 1", network_id.clone()),
        create_test_monitor("Monitor 2", network_id.clone()),
    ];
    repo.add_monitors(&network_id, monitors_to_add.clone()).await.unwrap();

    // 3. Get monitors and verify they were added
    let stored_monitors = repo.get_monitors(&network_id).await.unwrap();
    assert_eq!(stored_monitors.len(), 2);
    assert_eq!(stored_monitors[0].name, "Monitor 1");
    assert_eq!(stored_monitors[1].name, "Monitor 2");

    // 4. Clear monitors
    repo.clear_monitors(&network_id).await.unwrap();
    let cleared_monitors = repo.get_monitors(&network_id).await.unwrap();
    assert!(cleared_monitors.is_empty());
}

#[tokio::test]
async fn test_action_lifecycle() {
    let repo = setup_db().await;
    let network_id = NetworkId::from("ethereum");

    // 1. Initially, no actions should exist
    let initial_actions = repo.get_actions(&network_id).await.unwrap();
    assert!(initial_actions.is_empty());

    // 2. Add actions
    let actions_to_add = [create_test_action("Action 1"), create_test_action("Action 2")];
    repo.create_action(&network_id, actions_to_add[0].clone()).await.unwrap();
    repo.create_action(&network_id, actions_to_add[1].clone()).await.unwrap();

    // 3. Get actions and verify they were added
    let stored_actions = repo.get_actions(&network_id).await.unwrap();
    assert_eq!(stored_actions.len(), 2);
    assert_eq!(stored_actions[0].name, "Action 1");
    assert_eq!(stored_actions[1].name, "Action 2");

    // 4. Clear actions
    repo.clear_actions(&network_id).await.unwrap();
    let cleared_actions = repo.get_actions(&network_id).await.unwrap();
    assert!(cleared_actions.is_empty());
}

#[tokio::test]
async fn test_processed_block_management() {
    let repo = setup_db().await;
    let network_id = NetworkId::from("ethereum");

    // 1. Initially, last processed block should be None
    let initial_block = repo.get_last_processed_block(&network_id).await.unwrap();
    assert!(initial_block.is_none());

    // 2. Set and get the last processed block
    repo.set_last_processed_block(&network_id, 12345, None).await.unwrap();
    let retrieved_block = repo.get_last_processed_block(&network_id).await.unwrap();
    assert_eq!(retrieved_block, Some(12345));

    // 3. Update the last processed block
    let hash = B256::from([0xBB; 32]);
    repo.set_last_processed_block(&network_id, 54321, Some(hash)).await.unwrap();
    let updated_block = repo.get_last_processed_block(&network_id).await.unwrap();
    assert_eq!(updated_block, Some(54321));
    assert_eq!(repo.get_last_processed_block_tip(&network_id).await.unwrap(), Some((54321, hash)));
}

#[tokio::test]
async fn test_network_isolation() {
    let repo = setup_db().await;
    repo.create_abi("test", "[]").await.unwrap();
    let eth_network = NetworkId::from("ethereum");
    let poly_network = NetworkId::from("polygon");

    // Add monitors and actions to both networks
    repo.add_monitors(&eth_network, vec![create_test_monitor("ETH Monitor", eth_network.clone())])
        .await
        .unwrap();
    repo.add_monitors(
        &poly_network,
        vec![create_test_monitor("Polygon Monitor", poly_network.clone())],
    )
    .await
    .unwrap();
    repo.create_action(&eth_network, create_test_action("ETH Action")).await.unwrap();
    repo.create_action(&poly_network, create_test_action("Polygon Action")).await.unwrap();

    // Verify data for Ethereum
    let eth_monitors = repo.get_monitors(&eth_network).await.unwrap();
    let eth_actions = repo.get_actions(&eth_network).await.unwrap();
    assert_eq!(eth_monitors.len(), 1);
    assert_eq!(eth_monitors[0].name, "ETH Monitor");
    assert_eq!(eth_actions.len(), 1);
    assert_eq!(eth_actions[0].name, "ETH Action");

    // Verify data for Polygon
    let poly_monitors = repo.get_monitors(&poly_network).await.unwrap();
    let poly_actions = repo.get_actions(&poly_network).await.unwrap();
    assert_eq!(poly_monitors.len(), 1);
    assert_eq!(poly_monitors[0].name, "Polygon Monitor");
    assert_eq!(poly_actions.len(), 1);
    assert_eq!(poly_actions[0].name, "Polygon Action");

    // Clear Ethereum and verify it doesn't affect Polygon
    repo.clear_monitors(&eth_network).await.unwrap();
    repo.clear_actions(&eth_network).await.unwrap();

    assert!(repo.get_monitors(&eth_network).await.unwrap().is_empty());
    assert!(repo.get_actions(&eth_network).await.unwrap().is_empty());
    assert_eq!(repo.get_monitors(&poly_network).await.unwrap().len(), 1);
    assert_eq!(repo.get_actions(&poly_network).await.unwrap().len(), 1);
}

#[tokio::test]
async fn test_outbox_batch_ops() {
    let repo = setup_db().await;
    let action_name = "test_action";
    // Two distinct matches (distinct ids) so the dedup UNIQUE index keeps both.
    let batch = vec![
        (action_name.to_string(), ActionPayload::Single(create_test_monitor_match(action_name))),
        (action_name.to_string(), ActionPayload::Single(create_test_monitor_match(action_name))),
    ];
    repo.enqueue_outbox_batch(batch).await.unwrap();

    // 2. Fetch pending
    let pending = repo.get_pending_outbox(10).await.unwrap();
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].action_name, action_name);

    // 3. Delete batch
    let ids: Vec<i64> = pending.iter().map(|item| item.id).collect();
    repo.delete_outbox_items_batch(&ids).await.unwrap();

    // 4. Verify empty
    let empty_pending = repo.get_pending_outbox(10).await.unwrap();
    assert!(empty_pending.is_empty());
}

#[tokio::test]
async fn test_outbox_large_batch_chunking() {
    let repo = setup_db().await;
    let action_name = "test_action";
    // Create a large batch of 500 items to test chunking behavior
    let count = 500;
    let mut batch = Vec::with_capacity(count);
    for _ in 0..count {
        batch.push((
            action_name.to_string(),
            ActionPayload::Single(create_test_monitor_match(action_name)),
        ));
    }

    repo.enqueue_outbox_batch(batch).await.expect("Failed to enqueue large batch");

    // Fetch in chunks to verify all were stored
    let mut total_fetched = 0;
    loop {
        let pending = repo.get_pending_outbox(200).await.unwrap();
        if pending.is_empty() {
            break;
        }
        total_fetched += pending.len();
        let fetched_ids: Vec<i64> = pending.iter().map(|item| item.id).collect();
        repo.delete_outbox_items_batch(&fetched_ids).await.unwrap();
    }

    assert_eq!(total_fetched, count);
}
