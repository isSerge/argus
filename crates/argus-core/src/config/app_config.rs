use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use url::Url;

use super::{
    BaseHttpClientConfig, HttpRetryConfig, OutboxConfig, RhaiConfig, RpcRetryConfig, ServerConfig,
    deserialize_duration_from_ms, deserialize_duration_from_ms_opt,
    deserialize_duration_from_seconds, deserialize_urls, initial_start_block::InitialStartBlock,
};
use crate::models::NetworkId;

/// Provides the default value for shutdown_timeout_secs.
fn default_shutdown_timeout() -> Duration {
    Duration::from_secs(30)
}

/// Provides the default value for notification_channel_capacity.
fn default_notification_channel_capacity() -> u32 {
    1024
}

/// Provides the default value for aggregation_check_interval_secs.
fn default_aggregation_check_interval_secs() -> Duration {
    Duration::from_secs(5)
}

/// Provides the default value for concurrency.
fn default_concurrency() -> u64 {
    8 // sweet spot according to benchmarks against local RPC cache
}

/// Provides the default maximum number of blocks per `eth_getLogs` RPC call.
///
/// 2 000 is a conservative value that works with most providers (Alchemy,
/// Ankr, Infura, QuickNode, etc.). Raise it if your provider allows larger
/// windows; set it to 0 to disable chunking (single call, as before).
fn default_log_chunk_size() -> u64 {
    2_000
}

/// Application configuration for Argus.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct AppConfig {
    /// Database URL for the SQLite database.
    pub database_url: String,

    /// RPC URLs for the Ethereum node.
    #[serde(deserialize_with = "deserialize_urls")]
    pub rpc_urls: Vec<Url>,

    /// Network ID for the Ethereum network.
    pub network_id: NetworkId,

    /// Path to monitor configuration file.
    #[serde(skip_deserializing)]
    pub monitor_config_path: PathBuf,

    /// Path to action configuration file.
    #[serde(skip_deserializing)]
    pub action_config_path: PathBuf,

    /// Optional retry configuration.
    #[serde(default)]
    pub rpc_retry_config: RpcRetryConfig,

    /// Configuration for HTTP client retry policies.
    #[serde(default)]
    pub http_retry_config: HttpRetryConfig,

    /// Configuration for the base HTTP client.
    #[serde(default)]
    pub http_base_config: BaseHttpClientConfig,

    /// The size of the block chunk to process at once.
    pub block_chunk_size: u64,

    /// Maximum number of blocks covered by a single `eth_getLogs` RPC call.
    ///
    /// When `block_chunk_size` exceeds this value the log-fetch is split into
    /// multiple sub-range requests that are issued in parallel. Set to `0` to
    /// disable chunking and always issue a single call (legacy behaviour).
    ///
    /// Rejected sub-ranges are automatically halved and retried, down to a
    /// single block.
    #[serde(default = "default_log_chunk_size")]
    pub log_chunk_size: u64,

    /// The number of concurrent block fetches.
    #[serde(default = "default_concurrency")]
    pub concurrency: u64,

    /// The interval in milliseconds to poll for new blocks.
    #[serde(
        deserialize_with = "deserialize_duration_from_ms",
        serialize_with = "serialize_duration_to_ms"
    )]
    pub polling_interval_ms: Duration,

    /// Expected block time of the target chain in milliseconds (e.g.
    /// Ethereum 12000, BSC 3000, Polygon 2000, Arbitrum 1000).
    ///
    /// When set, the ingestor's live polling cadence tracks the chain: once
    /// caught up it polls at ~this interval (clamped to [250ms,
    /// `polling_interval_ms`]) instead of `polling_interval_ms`, which then
    /// only serves as the backoff after errors.
    #[serde(
        default,
        deserialize_with = "deserialize_duration_from_ms_opt",
        serialize_with = "serialize_duration_to_ms_opt"
    )]
    pub expected_block_time_ms: Option<Duration>,

    /// Number of confirmation blocks to wait for before processing a block
    /// (reorg-safety depth: `safe head = head - confirmation_blocks`).
    ///
    /// Chain-specific: the same count means very different wall-clock safety
    /// and latency on different chains. See the per-chain recommended-values
    /// table in `docs/src/user_guide/app_yaml.md`.
    pub confirmation_blocks: u64,

    /// The maximum time in seconds to wait for graceful shutdown.
    #[serde(
        deserialize_with = "deserialize_duration_from_seconds",
        serialize_with = "serialize_duration_to_seconds",
        default = "default_shutdown_timeout"
    )]
    pub shutdown_timeout: Duration,

    /// Rhai script execution configuration.
    #[serde(default)]
    pub rhai: RhaiConfig,

    /// The capacity of the channel used for sending notifications.
    #[serde(default = "default_notification_channel_capacity")]
    pub notification_channel_capacity: u32,

    /// Path to ABI configuration directory.
    pub abi_config_path: PathBuf,

    /// The interval in seconds to check for aggregated matches.
    #[serde(
        deserialize_with = "deserialize_duration_from_seconds",
        serialize_with = "serialize_duration_to_seconds",
        default = "default_aggregation_check_interval_secs"
    )]
    pub aggregation_check_interval_secs: Duration,

    /// The block number or tag (e.g., "latest", "-100") to start from on
    /// initial run.
    #[serde(default)]
    pub initial_start_block: InitialStartBlock,

    /// Server configuration
    #[serde(default)]
    pub server: ServerConfig,

    /// Outbox processor configuration
    #[serde(default)]
    pub outbox: OutboxConfig,
}

impl AppConfig {
    /// Creates a new `AppConfig` by reading from the configuration directory.
    pub fn new(config_dir: Option<&str>) -> Result<Self, ConfigError> {
        let config_dir_str = config_dir.unwrap_or("configs");
        let s = Config::builder()
            .add_source(File::with_name(&format!("{}/app.yaml", config_dir_str)))
            .add_source(Environment::with_prefix("ARGUS").separator("__"))
            .build()?;
        let mut config: Self = s.try_deserialize()?;

        let config_path = Path::new(config_dir_str);

        // Join the config paths with the config directory
        // This ensures that the paths are correctly resolved relative to the config
        // directory.
        let monitor_path = config_path.join("monitors.yaml");
        config.monitor_config_path = monitor_path;

        let action_path = config_path.join("actions.yaml");
        config.action_config_path = action_path;

        Ok(config)
    }

    /// Creates a new `AppConfigBuilder` for testing purposes.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn builder() -> AppConfigBuilder {
        AppConfigBuilder::default()
    }
}

/// A builder for creating `AppConfig` instances for testing.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Default)]
pub struct AppConfigBuilder {
    config: AppConfig,
}

#[cfg(any(test, feature = "test-utils"))]
impl AppConfigBuilder {
    pub fn rpc_urls(mut self, rpc_urls: Vec<Url>) -> Self {
        self.config.rpc_urls = rpc_urls;
        self
    }

    pub fn network_id(mut self, network_id: &NetworkId) -> Self {
        self.config.network_id = network_id.clone();
        self
    }

    pub fn monitor_config_path(mut self, path: &str) -> Self {
        self.config.monitor_config_path = path.into();
        self
    }

    pub fn action_config_path(mut self, path: &str) -> Self {
        self.config.action_config_path = path.into();
        self
    }

    pub fn database_url(mut self, url: &str) -> Self {
        self.config.database_url = url.to_string();
        self
    }

    pub fn confirmation_blocks(mut self, blocks: u64) -> Self {
        self.config.confirmation_blocks = blocks;
        self
    }

    pub fn build(self) -> AppConfig {
        self.config
    }

    pub fn abi_config_path(mut self, path: &str) -> Self {
        self.config.abi_config_path = path.into();
        self
    }

    pub fn polling_interval(mut self, interval_ms: u64) -> Self {
        self.config.polling_interval_ms = Duration::from_millis(interval_ms);
        self
    }

    pub fn expected_block_time(mut self, block_time_ms: u64) -> Self {
        self.config.expected_block_time_ms = Some(Duration::from_millis(block_time_ms));
        self
    }

    pub fn block_chunk_size(mut self, block_chunk_size: u64) -> Self {
        self.config.block_chunk_size = block_chunk_size;
        self
    }

    pub fn initial_start_block(mut self, block: InitialStartBlock) -> Self {
        self.config.initial_start_block = block;
        self
    }

    pub fn concurrency(mut self, concurrency: u64) -> Self {
        self.config.concurrency = concurrency;
        self
    }

    pub fn log_chunk_size(mut self, log_chunk_size: u64) -> Self {
        self.config.log_chunk_size = log_chunk_size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_app_config_builder() {
        let rpc_urls = vec![Url::parse("http://localhost:8545").unwrap()];
        let config = AppConfig::builder()
            .rpc_urls(rpc_urls)
            .network_id(&NetworkId::default())
            .monitor_config_path("test_monitor.yaml")
            .action_config_path("test_action.yaml")
            .database_url("sqlite::memory:")
            .confirmation_blocks(12)
            .abi_config_path("abis/")
            .build();

        assert_eq!(config.rpc_urls.len(), 1);
        assert_eq!(config.network_id, NetworkId::default());
        assert_eq!(config.monitor_config_path, PathBuf::from("test_monitor.yaml"));
        assert_eq!(config.action_config_path, PathBuf::from("test_action.yaml"));
        assert_eq!(config.database_url, "sqlite::memory:");
        assert_eq!(config.confirmation_blocks, 12);
    }

    #[test]
    fn test_app_config_from_file() {
        // Create a temporary config file for testing
        let config_content = r#"
        database_url: "sqlite::memory:"
        rpc_urls:
          - "http://localhost:8545"
        network_id: "testnet"
        confirmation_blocks: 12
        block_chunk_size: 0
        polling_interval_ms: 10000
        expected_block_time_ms: 2000
        abi_config_path: abis/
        "#;
        let temp_dir = tempfile::tempdir().unwrap();
        let app_yaml_path = temp_dir.path().join("app.yaml");
        std::fs::write(&app_yaml_path, config_content).unwrap();

        let temp_dir_path = temp_dir.path();
        let config = AppConfig::new(Some(temp_dir_path.to_str().unwrap())).unwrap();
        assert!(!config.rpc_urls.is_empty());
        assert_eq!(config.network_id, NetworkId::default());

        let expected_monitor_path = temp_dir_path.join("monitors.yaml");
        assert_eq!(config.monitor_config_path, expected_monitor_path);

        let expected_action_path = temp_dir_path.join("actions.yaml");
        assert_eq!(config.action_config_path, expected_action_path);

        assert_eq!(config.database_url, "sqlite::memory:");
        assert_eq!(config.confirmation_blocks, 12);
        assert_eq!(config.shutdown_timeout, Duration::from_secs(30));
        assert_eq!(config.notification_channel_capacity, 1024);
        assert_eq!(config.block_chunk_size, 0);
        assert_eq!(config.polling_interval_ms, Duration::from_millis(10000));
        assert_eq!(config.expected_block_time_ms, Some(Duration::from_millis(2000)));
    }

    #[test]
    fn test_app_config_from_file_with_http_base_config() {
        let config_content = r#"
        database_url: "sqlite::memory:"
        rpc_urls:
          - "http://localhost:8545"
        network_id: "testnet"
        confirmation_blocks: 12
        block_chunk_size: 0
        polling_interval_ms: 10000
        abi_config_path: abis/
        http_base_config:
          max_idle_per_host: 50
          idle_timeout: 120
          connect_timeout: 20
        "#;
        let temp_dir = tempfile::tempdir().unwrap();
        let app_yaml_path = temp_dir.path().join("app.yaml");
        std::fs::write(&app_yaml_path, config_content).unwrap();

        let temp_dir_path = temp_dir.path();
        let config = AppConfig::new(Some(temp_dir_path.to_str().unwrap())).unwrap();

        assert_eq!(config.http_base_config.max_idle_per_host, 50);
        assert_eq!(config.http_base_config.idle_timeout, Duration::from_secs(120));
        assert_eq!(config.http_base_config.connect_timeout, Duration::from_secs(20));
    }

    #[test]
    fn test_app_config_from_file_with_env_var_override() {
        let config_content = r#"
        database_url: "sqlite::memory:"
        rpc_urls:
          - "http://localhost:8545"
        network_id: "testnet"
        confirmation_blocks: 12
        block_chunk_size: 0
        polling_interval_ms: 10000
        abi_config_path: abis/
        "#;
        let temp_dir = tempfile::tempdir().unwrap();
        let app_yaml_path = temp_dir.path().join("app.yaml");
        std::fs::write(&app_yaml_path, config_content).unwrap();

        unsafe {
            std::env::set_var("ARGUS__RPC_URLS", "http://override:8545,http://another:8545");
        }

        let temp_dir_path = temp_dir.path();
        let config = AppConfig::new(Some(temp_dir_path.to_str().unwrap())).unwrap();

        assert_eq!(config.rpc_urls.len(), 2);
        assert_eq!(config.rpc_urls[0].to_string(), "http://override:8545/");
        assert_eq!(config.rpc_urls[1].to_string(), "http://another:8545/");

        unsafe {
            std::env::remove_var("ARGUS__RPC_URLS");
        }
    }
}
