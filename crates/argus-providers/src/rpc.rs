//! This module provides functionality to create a provider for EVM RPC requests
//! with retry logic and backoff strategies.

use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    sync::Arc,
};

use alloy::{
    primitives::{B256, BloomInput, TxHash},
    providers::{Provider, ProviderBuilder, layers::CallBatchLayer},
    rpc::{
        client::RpcClient,
        types::{Block, Filter, Log, TransactionReceipt},
    },
    transports::{
        http::{Http, reqwest::Url},
        layers::{FallbackLayer, RetryBackoffLayer},
    },
};
use argus_core::{
    config::RpcRetryConfig,
    models::Log as ArgusLog,
    monitor::RegistryProvider,
    providers::traits::{DataSource, DataSourceError},
};
use async_trait::async_trait;
use futures::stream::{self, StreamExt, TryStreamExt};
use tower::ServiceBuilder;

/// A `DataSource` implementation that fetches data from an EVM RPC endpoint.
pub struct EvmRpcSource {
    /// The RPC provider used to fetch block data.
    provider: Arc<dyn Provider + Send + Sync>,

    /// Shared interest registry for bloom-filter pre-screening.
    registry: Arc<dyn RegistryProvider>,
}

impl EvmRpcSource {
    /// Creates a new `EvmRpcSource`.
    #[tracing::instrument(skip(provider, registry), level = "debug")]
    pub fn new(
        provider: Arc<dyn Provider + Send + Sync>,
        registry: Arc<dyn RegistryProvider>,
    ) -> Self {
        Self { provider, registry }
    }
}

#[async_trait]
impl DataSource for EvmRpcSource {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn fetch_block_core_data(
        &self,
        block_number: u64,
    ) -> Result<(Block, Vec<ArgusLog>), DataSourceError> {
        match self.fetch_block_and_logs(block_number).await {
            Ok((block, logs)) => {
                tracing::debug!(block_number, "Successfully fetched core block data.");
                Ok((block, logs.into_iter().map(ArgusLog::from).collect()))
            }
            Err(DataSourceError::BlockNotFound(num)) => {
                tracing::warn!(block_number = num, "Block not found.");
                Err(DataSourceError::BlockNotFound(num))
            }
            Err(e) => {
                tracing::error!(error = %e, block_number, "Failed to fetch core block data.");
                Err(DataSourceError::Provider(Box::new(e)))
            }
        }
    }

    /// Fetches only the transaction receipts for a given list of transaction
    /// hashes.
    ///
    /// Receipt requests are issued concurrently, bounded by `concurrency`,
    /// and leverage the provider's `CallBatchLayer` to automatically batch
    /// the underlying JSON-RPC requests.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn fetch_receipts(
        &self,
        tx_hashes: &[TxHash],
        concurrency: usize,
    ) -> Result<HashMap<TxHash, TransactionReceipt>, DataSourceError> {
        if tx_hashes.is_empty() {
            return Ok(HashMap::new());
        }

        let results: Vec<(TxHash, Option<TransactionReceipt>)> =
            stream::iter(tx_hashes.iter().copied())
                .map(|tx_hash| async move {
                    let receipt = self
                        .provider
                        .get_transaction_receipt(tx_hash)
                        .await
                        .map_err(|e| DataSourceError::Provider(Box::new(e)))?;
                    Ok::<_, DataSourceError>((tx_hash, receipt))
                })
                .buffer_unordered(concurrency)
                .try_collect()
                .await?;

        let receipts = results
            .into_iter()
            .filter_map(|(tx_hash, receipt)| receipt.map(|r| (tx_hash, r)))
            .collect();

        Ok(receipts)
    }

    /// Fetches the current block number from the data source.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_current_block_number(&self) -> Result<u64, DataSourceError> {
        self.provider.get_block_number().await.map_err(|e| DataSourceError::Provider(Box::new(e)))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn fetch_block_only(&self, block_number: u64) -> Result<Block, DataSourceError> {
        self.provider
            .get_block_by_number(block_number.into())
            .full()
            .await
            .map_err(|e| DataSourceError::Provider(Box::new(e)))?
            .ok_or(DataSourceError::BlockNotFound(block_number))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn fetch_logs_for_range(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<ArgusLog>, DataSourceError> {
        let interest_registry = self.registry.interest_registry();

        // No log-aware monitors — skip entirely.
        if interest_registry.log_interests.is_empty()
            && interest_registry.global_event_signatures.is_empty()
        {
            tracing::debug!("No log-aware monitors. Skipping range log fetch.");
            return Ok(Vec::new());
        }

        // Build a topic filter to let the node / eRPC skip irrelevant logs before
        // transfer when possible.
        let topic_filter = Self::build_topic_filter(&interest_registry);
        let logs = self.fetch_logs_for_block_range(from_block, to_block, topic_filter).await?;

        // Filter logs based on the interest registry. This is necessary even when a
        // topic filter was applied, because topic0 filtering can still admit logs
        // from unmonitored addresses and broad-mode interests cannot use it safely.
        Ok(logs
            .into_iter()
            .map(ArgusLog::from)
            .filter(|log| interest_registry.is_log_interesting(log))
            .collect())
    }
}

impl EvmRpcSource {
    /// Builds a topic0 OR-filter from the interest registry, if possible.
    ///
    /// Returns `None` when at least one address-specific monitor is in "broad
    /// mode" (i.e. its `log_interests` entry is `None`), because in that case
    /// we must accept every log from that address regardless of topic and
    /// cannot apply a topic filter safely.
    ///
    /// When a non-empty `Some` is returned the caller should pass the vector
    /// as a `topic0` OR-filter to `eth_getLogs`, which lets the node / eRPC
    /// discard irrelevant events before they reach the client.
    fn build_topic_filter(registry: &argus_core::monitor::InterestRegistry) -> Option<Vec<B256>> {
        // Broad-mode monitors need every log from their address — cannot filter.
        if registry.log_interests.values().any(|v| v.is_none()) {
            return None;
        }

        // Union of all event signatures we care about.
        let mut topics: HashSet<B256> = registry.global_event_signatures.iter().copied().collect();
        for precise_sigs in registry.log_interests.values().filter_map(|v| v.as_ref()) {
            topics.extend(precise_sigs.iter().copied());
        }

        if topics.is_empty() { None } else { Some(topics.into_iter().collect()) }
    }

    /// Fetches logs for a block, optionally restricting to a set of topic0
    /// values (OR semantics).
    async fn fetch_logs_for_block_range(
        &self,
        from: u64,
        to: u64,
        topic_filter: Option<Vec<B256>>,
    ) -> Result<Vec<Log>, DataSourceError> {
        let mut filter = Filter::new().from_block(from).to_block(to);
        if let Some(topics) = topic_filter {
            filter = filter.event_signature(topics);
        }
        self.provider.get_logs(&filter).await.map_err(|e| DataSourceError::Provider(Box::new(e)))
    }

    /// Fetches the core data for a block (the block itself and all its logs).
    ///
    /// This method does NOT fetch transaction receipts, which must be fetched
    /// separately if required.
    #[tracing::instrument(skip(self), level = "debug")]
    pub async fn fetch_block_and_logs(
        &self,
        number: u64,
    ) -> Result<(Block, Vec<Log>), DataSourceError> {
        // Fetch block first
        let block = self
            .provider
            .get_block_by_number(number.into())
            .full()
            .await
            .map_err(|e| DataSourceError::Provider(Box::new(e)))?
            .ok_or(DataSourceError::BlockNotFound(number))?;

        // Check if there is any log interest in this block
        // If not, skip fetching logs to save RPC calls
        let block_bloom = &block.header.logs_bloom;
        let interest_registry = self.registry.interest_registry();

        // Early exit if there are absolutely no log-aware monitors of any kind.
        if interest_registry.log_interests.is_empty()
            && interest_registry.global_event_signatures.is_empty()
        {
            tracing::debug!(block_number = number, "No log-aware monitors. Skipping log fetch.");
            return Ok((block, Vec::new()));
        }

        // Check 1: Do any globally monitored topics appear in the bloom?
        let might_have_global_logs = interest_registry
            .global_event_signatures
            .iter()
            .any(|topic| block_bloom.contains_input(BloomInput::Raw(topic.as_slice())));

        // Check 2: Do any address-specific interests appear in the bloom?
        let might_have_address_logs =
            interest_registry.log_interests.iter().any(|(addr, interest_mode)| {
                // All address-specific checks must first match the address in the bloom.
                if !block_bloom.contains_input(BloomInput::Raw(addr.as_slice())) {
                    return false;
                }

                match interest_mode {
                    // Precise Mode: Address is present, now check if any of its specific topics are
                    // also present.
                    Some(specific_signatures) => specific_signatures
                        .iter()
                        .any(|topic| block_bloom.contains_input(BloomInput::Raw(topic.as_slice()))),
                    // Broad Mode: Address is present, and since we can't be more specific, we must
                    // fetch.
                    None => true,
                }
            });

        let might_contain_relevant_logs = might_have_global_logs || might_have_address_logs;

        // Build a topic0 OR-filter from the interest registry when possible.
        // This lets the node / eRPC discard irrelevant events before transfer.
        let topic_filter = Self::build_topic_filter(&interest_registry);

        // Conditionally call eth_getLogs based on the bloom filter check.
        let logs = if might_contain_relevant_logs {
            // The bloom filter indicates a potential match. We MUST fetch the logs to
            // verify.
            tracing::debug!(block_number = number, "Bloom filter hit. Fetching logs.");
            self.fetch_logs_for_block_range(number, number, topic_filter).await?
        } else {
            // The bloom filter guarantees no relevant logs are in this block.
            // We can safely skip the expensive eth_getLogs call.
            tracing::debug!(block_number = number, "Bloom filter miss. Skipping log fetch.");
            Vec::new()
        };

        Ok((block, logs))
    }
}

/// Custom error type for provider operations.
#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    /// Error when creating the provider.
    #[error("Provider creation failed: {0}")]
    CreationError(String),
}

/// Creates a new provider with the given RPC URLs.
pub fn create_provider(
    urls: Vec<Url>,
    retry_config: RpcRetryConfig,
) -> Result<impl Provider, ProviderError> {
    if urls.is_empty() {
        return Err(ProviderError::CreationError("RPC URL list cannot be empty".into()));
    }

    // Create a FallbackLayer with the provided URLs
    let fallback_layer = FallbackLayer::default().with_active_transport_count(
        NonZeroUsize::new(urls.len()).expect("At least one URL is required"),
    );

    let transports: Vec<_> = urls.into_iter().map(Http::new).collect();

    // Instantiate the RetryBackoffLayer with the configuration
    let retry_layer = RetryBackoffLayer::new(
        retry_config.max_retry,
        retry_config.backoff_ms,
        retry_config.compute_units_per_second,
    )
    .with_avg_unit_cost(retry_config.avg_compute_unit_cost);

    // Apply the layers
    let service =
        ServiceBuilder::new().layer(retry_layer).layer(fallback_layer).service(transports);

    let client = RpcClient::builder().transport(service, false);
    let provider = ProviderBuilder::new().layer(CallBatchLayer::new()).connect_client(client);
    Ok(provider)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        str::FromStr,
        sync::Arc,
    };

    use alloy::{
        primitives::{Address, B256, Bloom, BloomInput, U256, address, b256},
        providers::{Provider, ProviderBuilder},
        rpc::types::{Block, TransactionReceipt},
        transports::{http::reqwest::Url, mock::Asserter},
    };
    use arc_swap::ArcSwap;
    use argus_core::{
        config::RpcRetryConfig,
        monitor::{InterestRegistry, RegistryProvider},
        test_utils::{BlockBuilder, LogBuilder, ReceiptBuilder},
    };

    use super::*;

    // --- Test helpers ---

    fn mock_provider() -> (Arc<dyn Provider + Send + Sync>, Asserter) {
        let asserter = Asserter::new();
        let provider = Arc::new(ProviderBuilder::new().connect_mocked_client(asserter.clone()));
        (provider, asserter)
    }

    fn make_empty_registry() -> Arc<dyn RegistryProvider> {
        Arc::new(ArcSwap::new(Arc::new(InterestRegistry::default())))
    }

    fn make_address_registry(address: Address) -> Arc<dyn RegistryProvider> {
        let mut map = HashMap::new();
        map.insert(address, None);
        Arc::new(ArcSwap::new(Arc::new(InterestRegistry {
            log_interests: Arc::new(map),
            ..Default::default()
        })))
    }

    fn make_global_topic_registry(topic: B256) -> Arc<dyn RegistryProvider> {
        let mut set = HashSet::new();
        set.insert(topic);
        Arc::new(ArcSwap::new(Arc::new(InterestRegistry {
            global_event_signatures: Arc::new(set),
            ..Default::default()
        })))
    }

    // --- Tests ---

    #[tokio::test]
    async fn test_fetch_block_core_data_success() {
        let (provider, asserter) = mock_provider();

        let monitored_address = address!("1111111111111111111111111111111111111111");

        let mut bloom = Bloom::default();
        bloom.accrue(BloomInput::Raw(monitored_address.as_slice()));

        let block = BlockBuilder::new().number(1).bloom(bloom).build();
        let log = LogBuilder::new().block_number(1).address(monitored_address).build();

        asserter.push_success(&block);
        asserter.push_success(&vec![log.clone()]);

        let source = EvmRpcSource::new(provider, make_address_registry(monitored_address));

        let (fetched_block, fetched_logs) = source.fetch_block_core_data(1).await.unwrap();

        assert_eq!(fetched_block, block);
        assert_eq!(fetched_logs, vec![log]);
    }

    #[tokio::test]
    async fn test_fetch_block_core_data_block_not_found() {
        let (provider, asserter) = mock_provider();

        asserter.push_success(&Option::<Block>::None);

        let source = EvmRpcSource::new(provider, make_empty_registry());

        let result = source.fetch_block_core_data(1).await;

        assert!(matches!(result, Err(DataSourceError::BlockNotFound(1))));
    }

    #[tokio::test]
    async fn test_fetch_block_core_data_error_handling() {
        let (provider, asserter) = mock_provider();
        asserter.push_failure_msg("RPC error");

        let source = EvmRpcSource::new(provider, make_empty_registry());

        let result = source.fetch_block_core_data(1).await;

        assert!(matches!(result, Err(DataSourceError::Provider(_))));
    }

    #[tokio::test]
    async fn test_get_current_block_number() {
        let (provider, asserter) = mock_provider();
        asserter.push_success(&U256::from(1));

        let source = EvmRpcSource::new(provider, make_empty_registry());

        let block_number = source.get_current_block_number().await.unwrap();

        assert_eq!(block_number, 1);
    }

    #[tokio::test]
    async fn test_get_current_block_number_error_handling() {
        let (provider, asserter) = mock_provider();
        asserter.push_failure_msg("RPC error");

        let source = EvmRpcSource::new(provider, make_empty_registry());

        let result = source.get_current_block_number().await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_fetch_receipts_error_handling() {
        let (provider, asserter) = mock_provider();
        asserter.push_failure_msg("RPC error");

        let source = EvmRpcSource::new(provider, make_empty_registry());

        let tx_hashes = &[B256::default()];
        let result = source.fetch_receipts(tx_hashes, 4).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_provider_success() {
        let url = Url::from_str("http://localhost:8545").unwrap();
        let retry_config = RpcRetryConfig::default();

        let result = create_provider(vec![url], retry_config);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_provider_error_handling() {
        let retry_config = RpcRetryConfig::default();
        let result = create_provider(vec![], retry_config);
        assert!(matches!(result, Err(ProviderError::CreationError(_))));
    }

    #[tokio::test]
    async fn test_fetch_block_and_logs_bloom_hit_address() {
        let (provider, asserter) = mock_provider();
        let block_number = 123;
        let monitored_address = Address::default();

        let mut bloom = Bloom::default();
        bloom.accrue(BloomInput::Raw(monitored_address.as_slice()));

        let block = BlockBuilder::new().number(block_number).bloom(bloom).build();
        let logs: Vec<Log> = vec![Log::default(), Log::default()];

        asserter.push_success(&block);
        asserter.push_success(&logs);

        let data_source = EvmRpcSource::new(provider, make_address_registry(monitored_address));
        let (fetched_block, fetched_logs) =
            data_source.fetch_block_and_logs(block_number).await.unwrap();

        assert_eq!(fetched_block.header.number, block_number);
        assert_eq!(fetched_logs.len(), 2);
    }

    #[tokio::test]
    async fn test_fetch_block_and_logs_bloom_hit_topic() {
        let (provider, asserter) = mock_provider();
        let block_number = 123;
        let transfer_topic =
            b256!("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");

        let mut bloom = Bloom::default();
        bloom.accrue(BloomInput::Raw(transfer_topic.as_slice()));

        let block = BlockBuilder::new().number(block_number).bloom(bloom).build();
        let log = LogBuilder::new().topics(vec![transfer_topic]).build();

        asserter.push_success(&block);
        asserter.push_success(&vec![log]);

        let data_source = EvmRpcSource::new(provider, make_global_topic_registry(transfer_topic));
        let (fetched_block, fetched_logs) =
            data_source.fetch_block_and_logs(block_number).await.unwrap();

        assert_eq!(fetched_block.header.number, block_number);
        assert_eq!(fetched_logs.len(), 1);
    }

    #[tokio::test]
    async fn test_fetch_block_and_logs_bloom_miss() {
        let (provider, asserter) = mock_provider();
        let block_number = 123;
        let monitored_address = address!("1111111111111111111111111111111111111111");

        // Default bloom is empty — address won't appear in it.
        let block = BlockBuilder::new().number(block_number).build();

        asserter.push_success(&block);

        let data_source = EvmRpcSource::new(provider, make_address_registry(monitored_address));
        let (fetched_block, fetched_logs) =
            data_source.fetch_block_and_logs(block_number).await.unwrap();

        assert_eq!(fetched_block.header.number, block_number);
        assert!(fetched_logs.is_empty());
    }

    #[tokio::test]
    async fn test_fetch_receipts_success() {
        let (provider, asserter) = mock_provider();
        let tx_hash1 = B256::from_slice(&[1; 32]);
        let tx_hash2 = B256::from_slice(&[2; 32]);

        let receipt1 = ReceiptBuilder::new().transaction_hash(tx_hash1).build();
        let receipt2 = ReceiptBuilder::new().transaction_hash(tx_hash2).build();

        asserter.push_success(&receipt1);
        asserter.push_success(&receipt2);

        let data_source = EvmRpcSource::new(provider, make_empty_registry());
        let receipts = data_source.fetch_receipts(&[tx_hash1, tx_hash2], 4).await.unwrap();

        assert_eq!(receipts.len(), 2);
        assert_eq!(receipts.get(&tx_hash1).unwrap().transaction_hash, tx_hash1);
        assert_eq!(receipts.get(&tx_hash2).unwrap().transaction_hash, tx_hash2);
    }

    #[tokio::test]
    async fn test_fetch_receipts_empty() {
        let (provider, _) = mock_provider();
        let data_source = EvmRpcSource::new(provider, make_empty_registry());
        let receipts = data_source.fetch_receipts(&[], 4).await.unwrap();
        assert!(receipts.is_empty());
    }

    #[tokio::test]
    async fn test_get_current_block_number_success() {
        let (provider, asserter) = mock_provider();
        let current_block = 999;

        asserter.push_success(&U256::from(current_block));

        let data_source = EvmRpcSource::new(provider, make_empty_registry());
        let result = data_source.get_current_block_number().await.unwrap();

        assert_eq!(result, current_block);
    }

    #[tokio::test]
    async fn test_fetch_block_not_found() {
        let (provider, asserter) = mock_provider();
        let block_number = 404;

        asserter.push_success(&Option::<Block>::None);

        let data_source = EvmRpcSource::new(provider, make_empty_registry());
        let result = data_source.fetch_block_and_logs(block_number).await;

        assert!(matches!(result, Err(DataSourceError::BlockNotFound(404))));
    }

    #[tokio::test]
    async fn test_fetch_receipts_partial_success() {
        let (provider, asserter) = mock_provider();
        let tx_hash1 = B256::from_slice(&[1; 32]);
        let tx_hash2 = B256::from_slice(&[2; 32]);

        let receipt1 = ReceiptBuilder::new().transaction_hash(tx_hash1).build();

        asserter.push_success(&receipt1);
        asserter.push_success(&Option::<TransactionReceipt>::None);

        let data_source = EvmRpcSource::new(provider, make_empty_registry());
        let receipts = data_source.fetch_receipts(&[tx_hash1, tx_hash2], 4).await.unwrap();

        assert_eq!(receipts.len(), 1);
        assert!(receipts.contains_key(&tx_hash1));
        assert!(!receipts.contains_key(&tx_hash2));
    }

    #[tokio::test]
    async fn test_provider_error_propagation() {
        let (provider, asserter) = mock_provider();

        asserter.push_failure_msg("test provider error");

        let data_source = EvmRpcSource::new(provider, make_empty_registry());
        let result = data_source.get_current_block_number().await;

        assert!(matches!(result, Err(DataSourceError::Provider(_))));
        assert!(result.unwrap_err().to_string().contains("test provider error"));
    }

    #[tokio::test]
    async fn test_fetch_block_and_logs_no_log_interest() {
        let (provider, asserter) = mock_provider();
        let block_number = 123;

        let mut bloom = Bloom::default();
        bloom.accrue(BloomInput::Raw(&[1; 32]));
        let block = BlockBuilder::new().number(block_number).bloom(bloom).build();

        // No log interests in the registry — logs should never be requested.
        asserter.push_success(&block);

        let data_source = EvmRpcSource::new(provider, make_empty_registry());
        let (fetched_block, fetched_logs) =
            data_source.fetch_block_and_logs(block_number).await.unwrap();

        assert_eq!(fetched_block.header.number, block_number);
        assert!(fetched_logs.is_empty());
    }

    #[tokio::test]
    async fn test_fetch_block_and_logs_log_fetch_fails() {
        let (provider, asserter) = mock_provider();
        let block_number = 123;
        let monitored_address = Address::default();

        let mut bloom = Bloom::default();
        bloom.accrue(BloomInput::Raw(monitored_address.as_slice()));
        let block = BlockBuilder::new().number(block_number).bloom(bloom).build();

        asserter.push_success(&block);
        asserter.push_failure_msg("failed to get logs");

        let data_source = EvmRpcSource::new(provider, make_address_registry(monitored_address));
        let result = data_source.fetch_block_and_logs(block_number).await;

        assert!(matches!(result, Err(DataSourceError::Provider(_))));
        assert!(result.unwrap_err().to_string().contains("failed to get logs"));
    }

    #[tokio::test]
    async fn test_fetch_receipts_provider_error() {
        let (provider, asserter) = mock_provider();
        let tx_hash1 = B256::from_slice(&[1; 32]);
        let tx_hash2 = B256::from_slice(&[2; 32]);

        let receipt1 = ReceiptBuilder::new().transaction_hash(tx_hash1).build();

        asserter.push_success(&receipt1);
        asserter.push_failure_msg("receipt unavailable");

        let data_source = EvmRpcSource::new(provider, make_empty_registry());
        let result = data_source.fetch_receipts(&[tx_hash1, tx_hash2], 4).await;

        assert!(matches!(result, Err(DataSourceError::Provider(_))));
        assert!(result.unwrap_err().to_string().contains("receipt unavailable"));
    }

    // ---- fetch_logs_for_range ----

    #[tokio::test]
    async fn test_fetch_logs_for_range_no_log_interest() {
        // No pushes — any RPC call would cause the mock to error.
        let (provider, _asserter) = mock_provider();
        let data_source = EvmRpcSource::new(provider, make_empty_registry());

        let result = data_source.fetch_logs_for_range(100, 200).await.unwrap();

        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_fetch_logs_for_range_with_global_topic() {
        let (provider, asserter) = mock_provider();
        let transfer_topic =
            b256!("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");
        let log = LogBuilder::new().topics(vec![transfer_topic]).build();
        asserter.push_success(&vec![log]);

        let data_source = EvmRpcSource::new(provider, make_global_topic_registry(transfer_topic));

        let logs = data_source.fetch_logs_for_range(100, 200).await.unwrap();

        assert_eq!(logs.len(), 1);
    }

    #[tokio::test]
    async fn test_fetch_logs_for_range_with_address_interest() {
        // Broad-mode address interest: no topic filter applied, all logs returned.
        let (provider, asserter) = mock_provider();
        let monitored_address = address!("1111111111111111111111111111111111111111");
        let logs = vec![
            LogBuilder::new().address(monitored_address).build(),
            LogBuilder::new().address(monitored_address).build(),
        ];
        asserter.push_success(&logs);

        let data_source = EvmRpcSource::new(provider, make_address_registry(monitored_address));

        let result = data_source.fetch_logs_for_range(50, 150).await.unwrap();

        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn test_fetch_logs_for_range_rpc_error() {
        let (provider, asserter) = mock_provider();
        let transfer_topic =
            b256!("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");
        asserter.push_failure_msg("getLogs failed");

        let data_source = EvmRpcSource::new(provider, make_global_topic_registry(transfer_topic));

        let result = data_source.fetch_logs_for_range(100, 200).await;

        assert!(matches!(result, Err(DataSourceError::Provider(_))));
        assert!(result.unwrap_err().to_string().contains("getLogs failed"));
    }

    #[tokio::test]
    async fn test_fetch_logs_for_range_provider_rejection_classifies_as_shrinkable() {
        // Provider rejections must classify as shrinkable query rejections.
        let (provider, asserter) = mock_provider();
        let transfer_topic =
            b256!("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");
        asserter.push_failure_msg("query returned more than 10000 results");

        let data_source = EvmRpcSource::new(provider, make_global_topic_registry(transfer_topic));

        let err = data_source.fetch_logs_for_range(100, 200).await.unwrap_err();

        assert!(err.to_string().contains("query returned more than 10000 results"));
        assert!(crate::block_fetcher::is_query_rejection(&err));
    }
}
