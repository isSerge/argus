//! Application context and initialization logic.
//! This module handles loading configuration, setting up the database,
//! initializing the ABI service, and preparing the EVM data provider.
//! The `AppContext` struct encapsulates all these components for use
//! throughout the application.

mod abi_repository;
mod error;

use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use abi_repository::AbiRepository;
use alloy::providers::Provider;
use argus_abi::AbiService;
pub use argus_core::metrics::{AppMetrics, Metrics};
use argus_core::{
    config::{AppConfig, InitialStartBlock},
    models::{action::ActionConfig, monitor::MonitorConfig},
    persistence::traits::{AppRepository, KeyValueStore},
};
use argus_dispatch::{ActionDispatcher, http_client::HttpClientPool, template::TemplateService};
use argus_engine::{alert_manager::AlertManager, filtering::RhaiFilteringEngine};
use argus_monitor::{MonitorManager, MonitorValidator};
use argus_providers::rpc::create_provider;
use argus_rhai::{RhaiCompiler, RhaiScriptValidator};
use argus_store::SqliteStateRepository;
pub use error::{AppContextError, InitializationError};

use crate::loader::load_config;

/// The application context, holding configuration, database repository,
/// ABI service, script compiler, and EVM data provider.
pub struct AppContext<T: AppRepository + KeyValueStore> {
    /// Shared application configuration.
    pub config: AppConfig,

    /// The state repository for database interactions.
    pub repo: Arc<T>,

    /// The ABI service for managing and querying ABIs.
    pub abi_service: Arc<AbiService>,

    /// The Rhai script compiler for compiling and validating scripts.
    pub script_compiler: Arc<RhaiCompiler>,

    /// The EVM data provider for blockchain interactions.
    pub provider: Arc<dyn Provider + Send + Sync>,

    /// Template service for rendering action templates.
    pub template_service: Arc<TemplateService>,

    /// The monitor manager for managing monitor configurations.
    pub monitor_manager: Arc<MonitorManager>,

    /// Shared MonitorValidator for validating monitors across the application.
    pub monitor_validator: Arc<MonitorValidator>,

    /// The filtering engine for evaluating blockchain data against monitors.
    pub filtering_engine: Arc<RhaiFilteringEngine>,

    /// The HTTP client pool for making HTTP requests.
    pub http_client_pool: Arc<HttpClientPool>,

    /// The action dispatcher for executing actions.
    pub action_dispatcher: Arc<ActionDispatcher>,

    /// The alert manager for managing alerts and notifications.
    pub alert_manager: Arc<AlertManager<T>>,
}

/// A builder for the `AppContext`, allowing configuration overrides
/// and step-by-step initialization.
pub struct AppContextBuilder {
    /// Optional configuration directory to load settings from.
    config_dir: Option<String>,

    /// Optional override for the initial start block.
    from_block_override: Option<InitialStartBlock>,

    /// Optional override for the database URL.
    database_url_override: Option<String>,
}

impl AppContextBuilder {
    /// Creates a new `AppContextBuilder` with optional configuration directory
    /// and initial start block override.
    pub fn new(config_dir: Option<String>, from_block_override: Option<InitialStartBlock>) -> Self {
        Self { config_dir, from_block_override, database_url_override: None }
    }

    /// Sets a database URL override.
    pub fn database_url(mut self, url: String) -> Self {
        self.database_url_override = Some(url);
        self
    }

    /// Builds the `AppContext`, performing all initialization steps.
    /// This includes loading configuration, setting up the database,
    /// initializing the ABI service, and preparing the EVM data provider.
    pub async fn build(self) -> Result<AppContext<SqliteStateRepository>, AppContextError> {
        tracing::debug!("Loading application configuration...");
        let mut config = AppConfig::new(self.config_dir.as_deref())?;
        tracing::debug!(database_url = %config.database_url, rpc_urls = ?config.rpc_urls, network_id = %config.network_id, "Configuration loaded.");

        if let Some(override_val) = self.from_block_override {
            tracing::info!(
                from_block = ?override_val,
                "Overriding initial start block with value from --from-block CLI argument."
            );
            config.initial_start_block = override_val;
        }

        if let Some(db_url) = self.database_url_override {
            tracing::info!(
                database_url = %db_url,
                "Overriding database URL."
            );
            config.database_url = db_url;
        }

        tracing::debug!("Initializing state repository...");
        let repo = Arc::new(SqliteStateRepository::new(&config.database_url).await?);
        repo.run_migrations().await?;
        tracing::info!("Database migrations completed.");

        // Sync filesystem ABIs to database before loading monitors
        // This is necessary because monitors may reference ABIs via foreign key
        Self::sync_abis_from_filesystem(repo.as_ref(), &config.abi_config_path).await?;

        tracing::debug!("Initializing ABI repository from database...");
        let abi_repository = Arc::new(AbiRepository::new(repo.clone()).await.map_err(|e| {
            AppContextError::Initialization(InitializationError::AbiLoad(format!(
                "Failed to initialize ABI repository from database: {e}"
            )))
        })?);
        tracing::info!("ABI repository initialized with {} ABIs.", abi_repository.len());

        tracing::debug!("Initializing ABI service");
        let abi_service = Arc::new(AbiService::from_named_abis(abi_repository.all_abis()));

        let template_service = Arc::new(TemplateService::new());

        let script_compiler = Arc::new(RhaiCompiler::new(config.rhai.clone()));

        tracing::debug!(rpc_urls = ?config.rpc_urls, "Initializing EVM data provider...");
        let provider =
            Arc::new(create_provider(config.rpc_urls.clone(), config.rpc_retry_config.clone())?);

        Self::initialize_block_state(&config, repo.as_ref(), provider.as_ref()).await?;
        Self::load_actions_from_file(&config, repo.as_ref()).await?;

        Self::load_monitors_from_file(
            &config,
            repo.as_ref(),
            abi_service.clone(),
            script_compiler.clone(),
            template_service.clone(),
        )
        .await?;
        Self::load_abis_from_monitors(&config, repo.as_ref(), abi_service.clone()).await?;

        // Initialize high-level services
        tracing::debug!(network_id = %config.network_id, "Loading monitors from database for filtering engine...");
        let monitors = repo.get_monitors(&config.network_id).await.map_err(|e| {
            AppContextError::Initialization(InitializationError::MonitorLoad(format!(
                "Failed to fetch monitors from DB: {e}"
            )))
        })?;
        tracing::info!(count = monitors.len(), network_id = %config.network_id, "Loaded monitors from database for filtering engine.");

        let monitor_manager =
            Arc::new(MonitorManager::new(monitors, script_compiler.clone(), abi_service.clone()));

        let filtering_engine = Arc::new(RhaiFilteringEngine::new(
            abi_service.clone(),
            config.rhai.clone(),
            monitor_manager.clone(),
        ));

        let http_client_pool = Arc::new(HttpClientPool::new(config.http_base_config.clone()));

        tracing::debug!(network_id = %config.network_id, "Loading actions from database for action dispatcher...");
        let actions = repo.get_actions(&config.network_id).await.map_err(|e| {
            AppContextError::Initialization(InitializationError::ActionLoad(format!(
                "Failed to fetch actions from DB: {e}"
            )))
        })?;
        tracing::info!(count = actions.len(), network_id = %config.network_id, "Loaded actions from database for action dispatcher.");

        // Keep a copy of the actions Vec for creating the shared MonitorValidator.
        let actions_for_validator: Arc<Vec<ActionConfig>> = Arc::new(actions.clone());

        let actions_map: Arc<std::collections::HashMap<String, ActionConfig>> =
            Arc::new(actions.into_iter().map(|t| (t.name.clone(), t)).collect());

        let action_dispatcher = Arc::new(
            ActionDispatcher::new(actions_map.clone(), http_client_pool.clone()).await.map_err(
                |e| {
                    AppContextError::Initialization(InitializationError::ActionLoad(format!(
                        "Failed to initialize action dispatcher: {e}"
                    )))
                },
            )?,
        );

        let alert_manager = AlertManager::new(repo.clone(), actions_map).await.map_err(|e| {
            AppContextError::Initialization(InitializationError::AlertManagerInitialization(
                format!("Failed to initialize alert manager: {e}"),
            ))
        })?;

        // Create a shared MonitorValidator using the populated services and actions.
        let script_validator = RhaiScriptValidator::new(script_compiler.clone());
        let monitor_validator = Arc::new(MonitorValidator::new(
            script_validator,
            abi_service.clone(),
            template_service.clone(),
            config.network_id.clone(),
            actions_for_validator,
        ));

        Ok(AppContext {
            config,
            repo,
            abi_service,
            script_compiler,
            provider,
            template_service,
            monitor_manager,
            filtering_engine,
            http_client_pool,
            action_dispatcher,
            alert_manager: Arc::new(alert_manager),
            monitor_validator,
        })
    }

    /// Initializes the block state in the database if not already set.
    /// This sets the latest processed block based on the configuration
    /// and the current block number from the provider.
    async fn initialize_block_state(
        config: &AppConfig,
        repo: &dyn AppRepository,
        provider: &dyn Provider,
    ) -> Result<(), InitializationError> {
        let network_id = &config.network_id;

        let latest_block = repo.get_last_processed_block(network_id).await.map_err(|e| {
            InitializationError::BlockStateInitialization(format!(
                "Failed to fetch latest processed block from DB: {e}"
            ))
        })?;

        if latest_block.is_some() {
            tracing::info!(network_id = %network_id, "Latest processed block already set in database. Skipping block state initialization.");
            return Ok(());
        }

        tracing::info!(network_id = %network_id, "No latest processed block found in database. Initializing...");
        let latest_block_number = provider.get_block_number().await.map_err(|e| {
            InitializationError::BlockStateInitialization(format!(
                "Failed to fetch latest block number from provider: {e}"
            ))
        })?;

        let safe_head = latest_block_number.saturating_sub(config.confirmation_blocks);

        let target_block = match config.initial_start_block {
            InitialStartBlock::Latest => latest_block_number,
            InitialStartBlock::Absolute(n) => n,
            InitialStartBlock::Offset(offset) => latest_block_number.saturating_add_signed(offset),
        };

        let final_start_block = std::cmp::min(target_block, safe_head);

        tracing::info!(
            network_id = %network_id,
            latest_block = latest_block_number,
            confirmation_blocks = config.confirmation_blocks,
            safe_head = safe_head,
            initial_start_block = ?config.initial_start_block,
            start_block = final_start_block,
            "Setting latest processed block in database."
        );

        repo.set_last_processed_block(network_id, final_start_block.saturating_sub(1), None)
            .await
            .map_err(|e| {
                InitializationError::BlockStateInitialization(format!(
                    "Failed to set latest processed block in DB: {e}"
                ))
            })?;

        Ok(())
    }

    /// Loads monitors from the configuration file into the database
    /// if no monitors currently exist in the database. Validates each
    /// monitor before storing it.
    async fn load_monitors_from_file(
        config: &AppConfig,
        repo: &dyn AppRepository,
        abi_service: Arc<AbiService>,
        script_compiler: Arc<RhaiCompiler>,
        template_service: Arc<TemplateService>,
    ) -> Result<(), InitializationError> {
        let network_id = &config.network_id;
        let config_path = &config.monitor_config_path;

        tracing::debug!(network_id = %network_id, "Checking for existing monitors in database...");
        let existing_monitors = repo.get_monitors(network_id).await.map_err(|e| {
            InitializationError::MonitorLoad(format!(
                "Failed to fetch existing monitors from DB: {e}"
            ))
        })?;

        if !existing_monitors.is_empty() {
            tracing::info!(
                network_id = %network_id,
                count = existing_monitors.len(),
                "Monitors already exist in the database. Skipping loading from file."
            );
            return Ok(());
        }

        tracing::info!(config_path = %config_path.display(), "No monitors found in database. Loading from configuration file...");
        let monitors = load_config::<MonitorConfig>(PathBuf::from(config_path)).map_err(|e| {
            InitializationError::MonitorLoad(format!("Failed to load monitors from file: {e}"))
        })?;

        let actions = repo.get_actions(network_id).await.map_err(|e| {
            InitializationError::MonitorLoad(format!(
                "Failed to fetch actions for monitor validation: {e}"
            ))
        })?;

        let script_validator = RhaiScriptValidator::new(script_compiler);
        let actions_arc = Arc::new(actions);
        let validator = MonitorValidator::new(
            script_validator,
            abi_service,
            template_service,
            network_id.clone(),
            actions_arc,
        );
        for monitor in &monitors {
            validator.validate(monitor).map_err(|e| {
                InitializationError::MonitorLoad(format!(
                    "Monitor validation failed for '{}': {}",
                    monitor.name, e
                ))
            })?;
        }

        let count = monitors.len();
        tracing::info!(count = count, "Loaded monitors from configuration file.");
        repo.clear_monitors(network_id).await.map_err(|e| {
            InitializationError::MonitorLoad(format!(
                "Failed to clear existing monitors in DB: {e}"
            ))
        })?;
        repo.add_monitors(network_id, monitors).await.map_err(|e| {
            InitializationError::MonitorLoad(format!("Failed to add monitors to DB: {e}"))
        })?;
        tracing::info!(count = count, network_id = %network_id, "Monitors from file stored in database.");
        Ok(())
    }

    /// Loads actions from the configuration file into the database
    /// if no actions currently exist in the database. Validates each
    /// action during loading.
    async fn load_actions_from_file(
        config: &AppConfig,
        repo: &dyn AppRepository,
    ) -> Result<(), InitializationError> {
        let network_id = &config.network_id;
        let config_path = &config.action_config_path;

        tracing::debug!(network_id = %network_id, "Checking for existing actions in database...");
        let existing_actions = repo.get_actions(network_id).await.map_err(|e| {
            InitializationError::ActionLoad(format!(
                "Failed to fetch existing actions from DB: {e}"
            ))
        })?;

        if !existing_actions.is_empty() {
            tracing::info!(
                network_id = %network_id,
                count = existing_actions.len(),
                "actions already exist in the database. Skipping loading from file."
            );
            return Ok(());
        }

        tracing::info!(config_path = %config_path.display(), "No actions found in database. Loading from configuration file...");
        let actions = load_config::<ActionConfig>(PathBuf::from(config_path)).map_err(|e| {
            InitializationError::ActionLoad(format!("Failed to load actions from file: {e}"))
        })?;
        let count = actions.len();
        tracing::info!(count = count, "Loaded actions from configuration file.");
        repo.clear_actions(network_id).await.map_err(|e| {
            InitializationError::ActionLoad(format!("Failed to clear existing actions in DB: {e}"))
        })?;

        for action in actions {
            repo.create_action(network_id, action).await.map_err(|e| {
                InitializationError::ActionLoad(format!("Failed to add action to DB: {e}"))
            })?;
        }

        tracing::info!(count = count, network_id = %network_id, "actions from file stored in database.");
        Ok(())
    }

    /// Loads ABIs referenced by monitors in the database into the ABI service.
    /// This ensures that all ABIs needed for monitoring are available.
    async fn load_abis_from_monitors(
        config: &AppConfig,
        repo: &dyn AppRepository,
        abi_service: Arc<AbiService>,
    ) -> Result<(), InitializationError> {
        let network_id = &config.network_id;
        tracing::debug!(network_id = %network_id, "Loading ABIs from monitors in database...");
        let monitors = repo.get_monitors(network_id).await.map_err(|e| {
            InitializationError::AbiLoad(format!("Failed to fetch monitors for ABI loading: {e}"))
        })?;

        for monitor in &monitors {
            if let (Some(address_str), Some(abi_name)) = (&monitor.address, &monitor.abi_name) {
                if address_str.eq_ignore_ascii_case("all") {
                    abi_service.add_global_abi(abi_name).map_err(|e| {
                        InitializationError::AbiLoad(format!(
                            "Failed to add global ABI '{}' for monitor '{}': {}",
                            abi_name, monitor.name, e
                        ))
                    })?;
                } else {
                    let address = address_str.parse().map_err(|e| {
                        InitializationError::AbiLoad(format!(
                            "Failed to parse address for monitor '{}': {}",
                            monitor.name, e
                        ))
                    })?;

                    abi_service.link_abi(address, abi_name).map_err(|e| {
                        InitializationError::AbiLoad(format!(
                            "Failed to link ABI '{}' for monitor '{}': {}",
                            abi_name, monitor.name, e
                        ))
                    })?;
                }
            }
        }
        tracing::info!(count = monitors.len(), network_id = %network_id, "ABIs loaded for monitors from database.");
        Ok(())
    }

    /// Syncs ABIs from the filesystem to the database.
    /// This function scans a directory for ABI files and adds them to the
    /// database if they don't already exist.
    async fn sync_abis_from_filesystem(
        repo: &dyn AppRepository,
        abi_dir: &Path,
    ) -> Result<(), InitializationError> {
        tracing::debug!("Syncing filesystem ABIs to database...");

        if !abi_dir.exists() {
            tracing::warn!("ABI directory does not exist: {}", abi_dir.display());
            return Ok(());
        }

        let entries = fs::read_dir(abi_dir).map_err(|e| {
            InitializationError::AbiLoad(format!("Failed to scan ABI directory: {e}"))
        })?;

        for entry in entries {
            let path = entry
                .map_err(|e| {
                    InitializationError::AbiLoad(format!("Failed to read ABI directory entry: {e}"))
                })?
                .path();

            if path.is_file() && path.extension().is_some_and(|ext| ext == "json") {
                let abi_name = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(|| path.file_name().unwrap().to_string_lossy().into_owned());

                match repo.get_abi(&abi_name).await {
                    Ok(Some(_)) => {
                        tracing::debug!(abi_name = %abi_name, "ABI already exists in database, skipping");
                    }
                    Ok(None) => {
                        let content = fs::read_to_string(&path).map_err(|e| {
                            InitializationError::AbiLoad(format!(
                                "Failed to read ABI file '{}': {}",
                                path.display(),
                                e
                            ))
                        })?;
                        repo.create_abi(&abi_name, &content).await.map_err(|e| {
                            InitializationError::AbiLoad(format!(
                                "Failed to sync ABI '{}' to database: {}",
                                abi_name, e
                            ))
                        })?;
                        tracing::debug!(abi_name = %abi_name, "Synced ABI to database");
                    }
                    Err(e) => {
                        return Err(InitializationError::AbiLoad(format!(
                            "Failed to check if ABI '{}' exists in database: {}",
                            abi_name, e
                        )));
                    }
                }
            }
        }

        tracing::info!("Filesystem ABIs synced to database successfully.");
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use argus_core::{
        config::{AppConfig, RhaiConfig},
        models::monitor::MonitorStatus,
        test_utils::ActionBuilder,
    };

    use super::*;

    fn create_test_config() -> AppConfig {
        AppConfig::builder()
            .rpc_urls(vec![url::Url::parse("http://localhost:8545").unwrap()])
            .database_url("sqlite::memory:")
            .build()
    }

    async fn create_test_repo() -> Arc<SqliteStateRepository> {
        let repo = SqliteStateRepository::new("sqlite::memory:")
            .await
            .expect("Failed to connect to in-memory db");
        repo.run_migrations().await.expect("Failed to run migrations");
        Arc::new(repo)
    }

    #[test]
    fn test_app_context_builder_new() {
        let builder = AppContextBuilder::new(Some("test_config".to_string()), None);
        assert_eq!(builder.config_dir, Some("test_config".to_string()));
        assert!(builder.from_block_override.is_none());
        assert!(builder.database_url_override.is_none());
    }

    #[test]
    fn test_app_context_builder_with_database_override() {
        let builder =
            AppContextBuilder::new(None, None).database_url("sqlite::memory:".to_string());

        assert_eq!(builder.database_url_override, Some("sqlite::memory:".to_string()));
    }

    #[test]
    fn test_app_context_error_display() {
        let config_error =
            AppContextError::Config(config::ConfigError::Message("test error".to_string()));
        let error_string = format!("{}", config_error);
        assert!(error_string.contains("Config error: test error"));
    }

    #[test]
    fn test_initialization_error_display() {
        let error = InitializationError::MonitorLoad("test error".to_string());
        let error_string = format!("{}", error);
        assert!(error_string.contains("Failed to load monitors from file: test error"));
    }

    #[tokio::test]
    async fn test_load_monitors_from_file_when_monitors_exist() {
        let config = create_test_config();
        let repo = create_test_repo().await;

        // Add existing monitor
        let existing_monitor = MonitorConfig {
            name: "existing".to_string(),
            network: config.network_id.clone(),
            address: None,
            abi_name: None,
            filter_script: "true".to_string(),
            actions: vec![],
            status: MonitorStatus::default(),
        };
        repo.add_monitors(&config.network_id, vec![existing_monitor]).await.unwrap();

        let abi_repository = Arc::new(AbiRepository::new(repo.clone()).await.unwrap());

        // Should skip loading since monitors already exist
        let result = AppContextBuilder::load_monitors_from_file(
            &config,
            repo.as_ref(),
            Arc::new(AbiService::from_named_abis(abi_repository.all_abis())),
            Arc::new(RhaiCompiler::new(RhaiConfig::default())),
            Arc::new(TemplateService::new()),
        )
        .await;

        assert!(result.is_ok());
        let monitors = repo.get_monitors(&config.network_id).await.unwrap();
        assert_eq!(monitors.len(), 1);
        assert_eq!(monitors[0].name, "existing");
    }

    #[tokio::test]
    async fn test_load_actions_from_file_when_actions_exist() {
        let config = create_test_config();
        let repo = create_test_repo().await;

        // Add existing action
        let existing_action = ActionBuilder::new("existing").build();
        repo.create_action(&config.network_id, existing_action).await.unwrap();

        // Should skip loading since actions already exist
        let result = AppContextBuilder::load_actions_from_file(&config, repo.as_ref()).await;

        assert!(result.is_ok());
        let actions = repo.get_actions(&config.network_id).await.unwrap();
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].name, "existing");
    }

    #[tokio::test]
    async fn test_load_actions_from_file_missing_file() {
        let config = create_test_config();
        let repo = create_test_repo().await;

        // Try to load from non-existent file
        let result = AppContextBuilder::load_actions_from_file(&config, repo.as_ref()).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            InitializationError::ActionLoad(msg) => {
                assert!(msg.contains("Failed to load actions from file"));
            }
            _ => panic!("Expected ActionLoad error"),
        }
    }

    #[tokio::test]
    async fn test_load_monitors_from_file_missing_file() {
        let config = create_test_config();
        let repo = create_test_repo().await;
        let abi_repository = Arc::new(AbiRepository::new(repo.clone()).await.unwrap());

        // Try to load from non-existent file
        let result = AppContextBuilder::load_monitors_from_file(
            &config,
            repo.as_ref(),
            Arc::new(AbiService::from_named_abis(abi_repository.all_abis())),
            Arc::new(RhaiCompiler::new(RhaiConfig::default())),
            Arc::new(TemplateService::new()),
        )
        .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            InitializationError::MonitorLoad(msg) => {
                assert!(msg.contains("Failed to load monitors from file"));
            }
            _ => panic!("Expected MonitorLoad error"),
        }
    }

    #[tokio::test]
    async fn test_load_abis_from_monitors_contract_specific() {
        let config = create_test_config();
        let repo = create_test_repo().await;
        repo.create_abi("erc20", "[]").await.unwrap();

        // Add monitor with contract address and ABI (using existing erc20.json)
        let monitor = MonitorConfig {
            name: "test_monitor".to_string(),
            network: config.network_id.clone(),
            address: Some("0x1234567890123456789012345678901234567890".to_string()),
            abi_name: Some("erc20".to_string()),
            filter_script: "true".to_string(),
            actions: vec![],
            status: MonitorStatus::default(),
        };
        repo.add_monitors(&config.network_id, vec![monitor]).await.unwrap();

        // Use the real abis directory
        let abi_repository = Arc::new(AbiRepository::new(repo.clone()).await.unwrap());
        let abi_service = Arc::new(AbiService::from_named_abis(abi_repository.all_abis()));

        let result =
            AppContextBuilder::load_abis_from_monitors(&config, repo.as_ref(), abi_service.clone())
                .await;
        assert!(result.is_ok());

        // Verify ABI was linked
        let address = "0x1234567890123456789012345678901234567890".parse().unwrap();
        let cached_contract = abi_service.get_abi(address);
        assert!(cached_contract.is_some());
    }

    #[tokio::test]
    async fn test_load_abis_from_monitors_global_monitor() {
        let config = create_test_config();
        let repo = create_test_repo().await;
        repo.create_abi("usdc", "[]").await.unwrap();

        // Add global monitor with ABI (using existing usdc.json)
        let monitor = MonitorConfig {
            name: "global_monitor".to_string(),
            network: config.network_id.clone(),
            address: Some("all".to_string()),
            abi_name: Some("usdc".to_string()),
            filter_script: "true".to_string(),
            actions: vec![],
            status: MonitorStatus::default(),
        };
        repo.add_monitors(&config.network_id, vec![monitor]).await.unwrap();

        // Use the real abis directory
        let abi_repository = Arc::new(AbiRepository::new(repo.clone()).await.unwrap());
        let abi_service = Arc::new(AbiService::from_named_abis(abi_repository.all_abis()));

        let result =
            AppContextBuilder::load_abis_from_monitors(&config, repo.as_ref(), abi_service.clone())
                .await;
        assert!(result.is_ok());

        // Verify global ABI was added
        let abi = abi_service.get_abi_by_name("usdc");
        assert!(abi.is_some());
    }

    #[tokio::test]
    async fn test_load_abis_from_monitors_invalid_address() {
        let config = create_test_config();
        let repo = create_test_repo().await;
        repo.create_abi("erc20", "[]").await.unwrap();

        // Add monitor with invalid address
        let monitor = MonitorConfig {
            name: "invalid_monitor".to_string(),
            network: config.network_id.clone(),
            address: Some("invalid_address".to_string()),
            abi_name: Some("erc20".to_string()),
            filter_script: "true".to_string(),
            actions: vec![],
            status: MonitorStatus::default(),
        };
        repo.add_monitors(&config.network_id, vec![monitor]).await.unwrap();

        // Use the real abis directory
        let abi_repository = Arc::new(AbiRepository::new(repo.clone()).await.unwrap());
        let abi_service = Arc::new(AbiService::from_named_abis(abi_repository.all_abis()));

        let result =
            AppContextBuilder::load_abis_from_monitors(&config, repo.as_ref(), abi_service).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            InitializationError::AbiLoad(msg) => {
                assert!(msg.contains("Failed to parse address"));
                assert!(msg.contains("invalid_monitor"));
            }
            _ => panic!("Expected AbiLoad error"),
        }
    }

    #[tokio::test]
    async fn test_load_abis_from_monitors_no_abi_or_address() {
        let config = create_test_config();
        let repo = create_test_repo().await;

        // Add monitor without ABI or address
        let monitor = MonitorConfig {
            name: "minimal_monitor".to_string(),
            network: config.network_id.clone(),
            address: None,
            abi_name: None,
            filter_script: "true".to_string(),
            actions: vec![],
            status: MonitorStatus::default(),
        };
        repo.add_monitors(&config.network_id, vec![monitor]).await.unwrap();

        // Use the real abis directory
        let abi_repository = Arc::new(AbiRepository::new(repo.clone()).await.unwrap());
        let abi_service = Arc::new(AbiService::from_named_abis(abi_repository.all_abis()));

        // Should succeed but do nothing
        let result =
            AppContextBuilder::load_abis_from_monitors(&config, repo.as_ref(), abi_service).await;
        assert!(result.is_ok());
    }
}
