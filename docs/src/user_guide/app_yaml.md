# `app.yaml` Configuration

The `app.yaml` file defines the global settings for the service, including how it connects to the blockchain, how it stores data, etc.

## Example `app.yaml`

```yaml
# The connection string for the SQLite database.
database_url: "sqlite:data/monitor.db"

# A list of RPC endpoint URLs. Argus will cycle through these if one fails.
rpc_urls:
  - "https://eth.llamarpc.com"
  - "https://1rpc.io/eth"
  - "https://rpc.mevblocker.io"

# A unique identifier for the network being monitored.
network_id: "ethereum"

# The directory where contract ABI JSON files are located.
abi_config_path: abis/

# Controls where Argus starts on a fresh database.
# Can be a block number (e.g., 18000000), 'latest', or a negative offset (e.g., -100).
initial_start_block: -100

# Performance and reliability settings.
block_chunk_size: 5
polling_interval_ms: 10000
# Optional: expected block time of the target chain (ms). See table below.
# expected_block_time_ms: 2000
# Required: pick a value appropriate for your chain
# (see "Confirmation Depth Per Chain" below).
confirmation_blocks: 12

# API server configuration
server:
  listen_address: "0.0.0.0:8080"
  api_key: "your-secret-api-key-here"
```

## Configuration Parameters

### Core Settings

| Parameter | Description | Default |
| :--- | :--- | :--- |
| `database_url` | The connection string for the SQLite database. **This field is required.** | (none) |
| `rpc_urls` | A list of RPC endpoint URLs for the EVM network. Argus will use them in a fallback sequence if one fails. **At least one URL is required.** | (none) |
| `network_id` | A unique identifier for the network being monitored (e.g., "ethereum", "sepolia"). **This field is required.** | (none) |
| `abi_config_path` | The directory where contract ABI JSON files are located. **This field is required.** | (none) |
| `initial_start_block` | Controls where Argus starts processing blocks on a fresh database. Can be an absolute block number (e.g., `18000000`), a negative offset from the latest block (e.g., `-100`), or the string `'latest'`. | `-100` |

### Performance & Reliability

| Parameter | Description | Default |
| :--- | :--- | :--- |
| `block_chunk_size` | The number of blocks to fetch and process in a single batch. **This field is required.** | (none) |
| `log_chunk_size` | Maximum number of blocks covered by a single `eth_getLogs` RPC call. When `block_chunk_size` exceeds this, the log fetch is split into parallel sub-range requests. Set to `0` to disable chunking. | `2000` |
| `polling_interval_ms` | The interval in milliseconds to poll for new blocks. Also used as the backoff after ingestion errors. **This field is required.** | (none) |
| `expected_block_time_ms` | Optional expected block time of the target chain in milliseconds (e.g. Ethereum `12000`, BSC `3000`, Polygon/Base `2000`, Arbitrum `1000`). When set, live polling tracks the chain: once caught up, Argus polls at ~this interval (clamped to [250ms, `polling_interval_ms`]) so alert latency stays ~one block on fast chains. | unset |
| `confirmation_blocks` | Number of blocks to wait before processing a block, to protect against reorgs. Higher is safer but adds latency. **This field is required** — pick a value appropriate for your chain (see [table below](#confirmation-depth-per-chain)). | (none) |
| `notification_channel_capacity` | The capacity of the internal channel for sending notifications. | `1024` |
| `shutdown_timeout` | The maximum time in seconds to wait for a graceful shutdown. | `30` |
| `aggregation_check_interval_secs` | The interval in seconds to check for aggregated matches for action with policies. | `5` |

### Confirmation Depth Per Chain

`confirmation_blocks` is a **reorg-safety** knob, not a finality guarantee: Argus only processes
blocks at or below `head - confirmation_blocks`. Because block times and finality rules differ
per chain, the same number means very different things in practice — 12 blocks is ~2.4 min on
Ethereum, ~36 s on BSC, ~24 s on Polygon (where true finality only arrives with the L1
checkpoint, anyway). The single knob deliberately conflates *reorg-safety* with *finality*, so
Argus requires you to set it explicitly — the table below gives recommended starting points;
the final depth is yours to choose.

| `network_id` aliases | Recommended | ≈ Wall-clock | Rationale |
| :--- | :--- | :--- | :--- |
| `ethereum`, `mainnet`, `eth` | `12` | ~2.4 min | Comfortably inside Ethereum's ~12.8 min finality window; covers typical reorgs. |
| `sepolia`, `holesky`, `hoodi` | `12` | ~2.4 min | Same consensus rules as mainnet. |
| `bsc`, `bnb`, `bnb-smart-chain` | `15` | ~45 s | Covers the fast-finality window and BSC's historical multi-block reorgs. |
| `polygon`, `matic`, `bor` | `128` | ~4.3 min | Deep reorgs (~100 blocks) have occurred on Bor; true finality is only the L1 checkpoint. If you need checkpoint-grade safety, wait for checkpoints instead of a bigger number. |
| `arbitrum`, `arbitrum-one` | `20` | ~5 s | Reorgs essentially don't happen; this only pads against sequencer-feed quirks. |
| `base`, `optimism`, `op-mainnet` | `15` | ~30 s | OP-stack soft confirmations are sequencer-ordered and effectively stable. |
| `gnosis`, `xdai` | `12` | ~1 min | PoS with rare, shallow reorgs. |
| `avalanche`, `avax`, `c-chain` | `5` | ~5–10 s | Near-instant Snowman finality. |
| *(anything else)* | `12` | — | Ethereum-tuned fallback; pick a value appropriate for your chain. |

On rollups (Arbitrum/OP/Base), real *finality* is inherited from L1 and takes minutes
regardless of the depth you choose — the small values above accept sequencer-confirmed ordering
as "safe", which is the usual trade-off for alerting. For high-value security monitoring,
consider raising the depth for your chain explicitly.

---

### Nested Configuration Sections

The following configurations are nested under their respective top-level keys in `app.yaml`.

### Server Settings (`server`)

These settings control the built-in REST API server. The API server provides read-only introspection endpoints and secured write endpoints for dynamic configuration.

**Default Configuration:**
```yaml
server:
  listen_address: "0.0.0.0:8080"
  api_key: null  # Can be set via ARGUS_API_KEY environment variable
```

| Parameter | Description |
| :--- | :--- |
| `listen_address` | The address and port for the HTTP server to listen on. |
| `api_key` | Optional API key for securing write endpoints. If not set in config, falls back to `ARGUS_API_KEY` environment variable. **Required** for write operations like `POST /monitors`. |

**Security Note:** All write endpoints (like `POST /monitors`) require bearer token authentication. The API key must be included in the `Authorization` header as `Bearer <your-api-key>`. Read-only endpoints like `GET /monitors` and `GET /health` do not require authentication.

### RPC Client Settings (`rpc_retry_config`)

These settings control the behavior of the client used to communicate with the EVM RPC endpoints.

**Default Configuration:**
```yaml
rpc_retry_config:
  max_retry: 10
  backoff_ms: 1000
  compute_units_per_second: 1000
  avg_compute_unit_cost: 17
```

| Parameter | Description |
| :--- | :--- |
| `max_retry` | The maximum number of retries for a failing RPC request. |
| `backoff_ms` | The initial backoff delay in milliseconds for RPC retries. |
| `compute_units_per_second` | Client-side compute-unit (CU) budget per second, shared by all `rpc_urls`. It only affects retry pacing after errors (e.g. 429s), roughly allowing `compute_units_per_second / avg_compute_unit_cost` requests per second. |
| `avg_compute_unit_cost` | Average CU cost of a single request (default `17` ≈ `eth_getBlockByNumber` on Alchemy). Raise it for heavy methods so the pacing errs on the safe side. |

#### Tuning `compute_units_per_second` per provider tier

The budget is shared across **all** configured `rpc_urls`, so match it to the *slowest* provider in the list. Recommended starting points:

| Provider tier | `compute_units_per_second` |
| :--- | :--- |
| Public / community endpoints (1rpc.io, llamarpc, mevblocker, …) | 100–250 |
| Infura Free (~200 CU/s) | 200 |
| Alchemy Free (330 CU/s) | 330 |
| Alchemy Growth (1,320 CU/s) | 1320 |
| Paid / dedicated nodes | 1000+ (per your contract) |

Notes:

- Costs are method-weighted: `eth_getBlockByNumber` ≈ 16 CU, but wide `eth_getLogs` ranges and receipt fetches can cost 50–75+ CU. For log-heavy monitoring, either raise `avg_compute_unit_cost` or lower the budget accordingly.
- **Fast chains** (BSC ~3s, Polygon/Base/OP ~2s, Arbitrum ≤1s) issue more `eth_getBlockByNumber`/`eth_getLogs`/`eth_getTransactionReceipt` calls per real-time second than Ethereum. If you see self-throttling in logs (`backing off due to rate limit`) while your provider reports low CU utilization, raise the budget; if you see 429s from the provider, lower it (a client-side budget cannot protect per-host limits when several endpoints share the list).

### HTTP Client Settings (`http_retry_config`)

These settings control the retry behavior of the internal HTTP client, which is used for sending webhook notifications.

**Default Configuration:**
```yaml
http_retry_config:
  max_retries: 3
  initial_backoff_ms: 250
  max_backoff_secs: 10
  base_for_backoff: 2
  jitter: full
```

| Parameter | Description |
| :--- | :--- |
| `max_retries` | The maximum number of retries for a failing HTTP request. |
| `initial_backoff_ms` | The initial backoff delay in milliseconds for HTTP retries. |
| `max_backoff_secs` | The maximum backoff delay in seconds for HTTP retries. |
| `base_for_backoff` | The base for the exponential backoff calculation. |
| `jitter` | The jitter to apply to the backoff (`none` or `full`). |

### Rhai Script Engine Settings (`rhai`)

These settings provide guardrails for the Rhai scripts to prevent long-running or resource-intensive scripts from impacting the application's performance.

**Default Configuration:**
```yaml
rhai:
  max_operations: 100000
  max_call_levels: 10
  max_string_size: 8192
  max_array_size: 1000
  execution_timeout: 5000
```

| Parameter | Description |
| :--- | :--- |
| `max_operations` | Maximum number of operations a script can perform. |
| `max_call_levels` | Maximum function call nesting depth in a script. |
| `max_string_size` | Maximum size of strings in characters. |
| `max_array_size` | Maximum number of array elements. |
| `execution_timeout` | Maximum execution time per script in milliseconds. |
