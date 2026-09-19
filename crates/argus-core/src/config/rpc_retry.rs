use serde::Deserialize;

/// Default average cost of a single RPC request in provider compute units.
///
/// Matches alloy's heuristic (`eth_getBlockByNumber` ≈ 16 CU,
/// `eth_getStorageAt` ≈ 17 CU on Alchemy). See the provider-tier table in
/// `docs/src/user_guide/app_yaml.md`.
const DEFAULT_AVG_COMPUTE_UNIT_COST: u64 = 17;

/// Configuration for the RPC retry backoff policy.
#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct RpcRetryConfig {
    /// The maximum number of retries for a request.
    pub max_retry: u32,
    /// The initial backoff delay in milliseconds.
    pub backoff_ms: u64, /* Keep as u64 because alloy::transports::layers::RetryBackoffLayer
                          * expects it. */
    /// Client-side compute-unit budget per second, shared by all RPC URLs.
    ///
    /// Only engages as extra wait time on retries (e.g. after a 429), so it
    /// should reflect what the *slowest* configured provider allows:
    /// roughly `compute_units_per_second / avg_compute_unit_cost` requests per
    /// second. Recommended values per provider tier are documented in
    /// `docs/src/user_guide/app_yaml.md`.
    pub compute_units_per_second: u64,
    /// Estimated average compute-unit cost of a request. Alloy applies one
    /// blended number, not per-method costs (`eth_getBlockByNumber` ≈ 16 CU,
    /// `eth_getLogs` 75+ CU); it is only used to pace retries after
    /// rate-limit errors. Raise it for log/receipt-heavy workloads.
    pub avg_compute_unit_cost: u64,
}

impl Default for RpcRetryConfig {
    fn default() -> Self {
        Self {
            max_retry: 10,
            backoff_ms: 1000,
            compute_units_per_second: 1000,
            avg_compute_unit_cost: DEFAULT_AVG_COMPUTE_UNIT_COST,
        }
    }
}

#[cfg(test)]
mod tests {
    use config::Config;

    use super::*;

    #[test]
    fn test_rpc_retry_config_with_custom_values() {
        let yaml = "
            max_retry: 5
            backoff_ms: 500
            compute_units_per_second: 50
            avg_compute_unit_cost: 34
        ";

        let builder =
            Config::builder().add_source(config::File::from_str(yaml, config::FileFormat::Yaml));
        let config: RpcRetryConfig = builder.build().unwrap().try_deserialize().unwrap();

        assert_eq!(config.max_retry, 5);
        assert_eq!(config.backoff_ms, 500);
        assert_eq!(config.compute_units_per_second, 50);
        assert_eq!(config.avg_compute_unit_cost, 34);
    }

    #[test]
    fn test_rpc_retry_config_without_custom_values_uses_default() {
        let default_config = RpcRetryConfig::default();
        let yaml = ""; // Empty YAML, so defaults should be used

        let builder =
            Config::builder().add_source(config::File::from_str(yaml, config::FileFormat::Yaml));
        let config: RpcRetryConfig = builder.build().unwrap().try_deserialize().unwrap();

        assert_eq!(config.max_retry, default_config.max_retry);
        assert_eq!(config.backoff_ms, default_config.backoff_ms);
        assert_eq!(config.compute_units_per_second, default_config.compute_units_per_second);
        assert_eq!(config.avg_compute_unit_cost, default_config.avg_compute_unit_cost);
    }
}
