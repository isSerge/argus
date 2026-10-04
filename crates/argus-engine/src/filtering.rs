//! This module defines the [`FilteringEngine`] trait and its Rhai-based
//! implementation, which is responsible for evaluating incoming blockchain data
//! (transactions and logs) against a set of user-defined Rhai scripts.
//!
//! # Evaluation Flow
//!
//! The filtering engine uses a two-pass evaluation strategy per block item:
//!
//! 1. **Log-Aware Pass**: Evaluates monitors that reference log data in their
//!    scripts.
//!    - Uses O(1) lexical peeking to quickly skip logs that don't match named
//!      events.
//!    - Decodes and caches logs lazily only when they pass lexical filters,
//!      ensuring each log is decoded at most once.
//!    - Each monitor is evaluated against surviving logs.
//!    - A global monitor (no address filter) can match multiple logs, creating
//!      a separate set of action matches for each.
//!    - Matched monitors are recorded to prevent re-evaluation in pass 2.
//!
//! 2. **Transaction-Only Pass**: Evaluates remaining monitors without log
//!    context.
//!    - Only runs on monitors that did not match in pass 1.
//!    - Handles pure transaction monitors (e.g., `tx.value > 100`).
//!    - Handles hybrid monitors that did not match any logs.
//!
//! # Duplicate Match Prevention
//!
//! The engine prevents duplicate matches for hybrid monitors (monitors with
//! scripts like `tx.value > 100 || log.name == "Transfer"`):
//!
//! - If the monitor matches a log → creates `LogMatch`, skips tx-only pass.
//! - If no logs match but the tx condition is true → creates
//!   `TransactionMatch`.
//! - If both conditions are true → creates **only** `LogMatch` (more specific).
//!
//! This ensures each monitor produces at most one type of match per
//! transaction, while still allowing a global monitor to match multiple logs.
//!
//! # Performance Notes
//!
//! Several optimizations are applied to reduce redundant work:
//!
//! - **Pre-compiled ASTs**: Each [`ClassifiedMonitor`] stores its script as a
//!   pre-compiled [`rhai::AST`] built at monitor-load time, eliminating
//!   per-evaluation compilation.
//! - **Lazy log caching**: Logs are decoded only when needed (after passing
//!   lexical event-name filters) and cached for reuse across monitors,
//!   eliminating redundant decoding while avoiding upfront work for skipped
//!   logs.
//! - **Lazy tx/call serialization**: Transaction detail payloads and decoded
//!   call JSON are serialized at most once per item via [`EvaluationContext`]
//!   caches.
//! - **Event-name pre-filter**: Monitors that statically name specific events
//!   skip Rhai evaluation entirely when a log's event name is not in the set.
//! - **Rayon parallelism**: In the live [`BlockProcessor`] path (`run`),
//!   per-item evaluation is parallelised across Rayon's thread pool inside
//!   `tokio::task::spawn_blocking`.
//!
//! # Example Scenarios
//!
//! ## Global ERC20 monitor
//! ```ignore
//! script: "log.name == 'Transfer'"
//! transaction: contains 3 Transfer logs from different contracts
//! result: 3 LogMatches × number of actions
//! ```
//!
//! ## Hybrid monitor — log wins
//! ```ignore
//! script: "tx.value > ether(1) || log.name == 'Transfer'"
//! transaction: value = 0.5 ETH, 1 Transfer log
//! result: 1 LogMatch
//! ```
//!
//! ## Hybrid monitor — tx fallback
//! ```ignore
//! script: "tx.value > ether(1) || log.name == 'Transfer'"
//! transaction: value = 2 ETH, no logs
//! result: 1 TransactionMatch
//! ```

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use alloy::primitives::B256;
use argus_abi::{AbiService, DecodedCall, DecodedLog};
use argus_core::{
    config::RhaiConfig,
    models::{
        correlated_data::CorrelatedBlockItem,
        decoded_block::CorrelatedBlockData,
        monitor::Monitor,
        monitor_match::{LogDetails, MonitorMatch},
    },
};
use argus_monitor::{ClassifiedMonitor, MonitorCapabilities, MonitorManager};
use argus_rhai::{
    RhaiCompilerError,
    conversions::{
        build_log_params_payload, build_transaction_details_payload, build_transaction_map,
    },
    create_engine, is_execution_timeout_error,
    proxies::{CallProxy, LogProxy},
    start_timeout,
};
use async_trait::async_trait;
#[cfg(test)]
use mockall::automock;
use rayon::prelude::*;
use rhai::{AST, Engine, EvalAltResult, Map, Scope};
use thiserror::Error;
use tokio::sync::mpsc;

/// Errors that can occur during Rhai script compilation or runtime execution.
#[derive(Debug, Error)]
pub enum RhaiError {
    /// The script could not be compiled (syntax error, unknown identifier,
    /// etc.).
    #[error("Script compilation failed: {0}")]
    CompilationError(#[from] RhaiCompilerError),

    /// The script produced a runtime error during evaluation.
    #[error("Script runtime error: {0}")]
    RuntimeError(Box<EvalAltResult>),

    /// Script execution exceeded the configured wall-clock time limit
    /// (`RhaiConfig::execution_timeout`).
    #[error("Script execution timeout after {timeout:?}")]
    ExecutionTimeout { timeout: Duration },
}

/// Trait for an engine that applies monitor filtering logic to decoded block
/// data.
///
/// The two core operations are:
/// - [`evaluate_item`](FilteringEngine::evaluate_item) — synchronous, used by
///   the dry-run command which parallelises evaluation externally (rayon).
/// - [`run`](FilteringEngine::run) — async streaming loop used by the live
///   block ingestor.
#[cfg_attr(test, automock)]
#[async_trait]
pub trait FilteringEngine: Send + Sync {
    /// Evaluates a single correlated block item against all configured
    /// monitors.
    ///
    /// Returns a `Vec` of all [`MonitorMatch`] results (one entry per
    /// matching monitor–action pair). Returns an empty `Vec` when nothing
    /// matches; only returns `Err` for hard script errors.
    fn evaluate_item(&self, item: &CorrelatedBlockItem) -> Result<Vec<MonitorMatch>, RhaiError>;

    /// Returns `true` if at least one active monitor references transaction
    /// receipt fields (e.g. `tx.status`, `tx.gas_used`) in its filter script,
    /// meaning receipts must be fetched for every transaction.
    fn requires_receipt_data(&self) -> bool;

    /// Runs the filtering engine as a long-lived async service.
    ///
    /// Receives [`CorrelatedBlockData`] items from `receiver`, evaluates each
    /// block's items in parallel across Rayon's thread pool (via
    /// `tokio::task::spawn_blocking`), and forwards every [`MonitorMatch`] to
    /// `notifications_tx`.
    async fn run(
        &self,
        mut receiver: mpsc::Receiver<CorrelatedBlockData>,
        notifications_tx: mpsc::Sender<Vec<MonitorMatch>>,
    );
}

/// A [`FilteringEngine`] implementation backed by the [Rhai](https://rhai.rs)
/// scripting engine.
///
/// Instances are cheaply cloneable (`Arc`-wrapped internals) so they can be
/// moved into `spawn_blocking` closures without lifetime issues.
#[derive(Debug, Clone)]
pub struct RhaiFilteringEngine {
    abi_service: Arc<AbiService>,
    engine: Arc<Engine>,
    execution_timeout: Duration,
    monitor_manager: Arc<MonitorManager>,
}

/// A unique key for caching decoded logs based on transaction hash and log
/// index.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
struct LogCacheKey {
    tx_hash: B256,
    log_index: u64,
}

/// Transient state accumulated while evaluating a single
/// [`CorrelatedBlockItem`].
///
/// Centralising evaluation state here allows the various helper methods to
/// share caches without passing many arguments or performing redundant work.
///
/// # Duplicate Match Prevention
///
/// `matched_monitor_ids` records every monitor that produced at least one match
/// during the **log-aware pass**.  The **transaction-only pass** skips any
/// monitor already in this set, preventing hybrid monitors (e.g.
/// `tx.value > 100 || log.name == "Transfer"`) from producing both a
/// `LogMatch` and a `TransactionMatch` for the same transaction.
///
/// Note: this does **not** prevent a global log-aware monitor from matching
/// multiple logs — each matching log correctly generates its own `LogMatch`.
struct EvaluationContext<'a> {
    /// The block item being evaluated.
    item: &'a CorrelatedBlockItem,
    /// Lazily populated full transaction-details JSON payload (includes receipt
    /// fields when available). Computed at most once per item.
    tx_details_cache: Option<serde_json::Value>,
    /// Accumulated matches produced so far for this item.
    matches: Vec<MonitorMatch>,
    /// IDs of monitors that already produced a match in the log-aware pass.
    matched_monitor_ids: HashSet<i64>,
    /// Lazily populated decoded calldata result. `None` means not yet
    /// attempted; `Some(None)` means decoding was attempted but failed
    /// (e.g. unknown selector).
    decoded_call_cache: Option<Option<Arc<DecodedCall>>>,
    /// Cached JSON serialization of the decoded call — computed at most once
    /// per transaction regardless of how many monitors match or how many
    /// actions they have.
    decoded_call_json_cache: Option<Option<serde_json::Value>>,
    /// Cache for decoded logs, keyed by (transaction hash, log index).
    ///
    /// Stores `Some(arc)` for a successful decode and `None` for a known
    /// failure (unknown ABI / bad signature). The presence of a key —
    /// regardless of its value — means the decode was already attempted, so
    /// failures are not retried for every monitor that encounters the same
    /// log.
    decoded_logs_cache: HashMap<LogCacheKey, Option<Arc<DecodedLog>>>,
    /// Cache for the transaction map (Rhai `Map` of transaction fields) to
    /// avoid redundant construction when multiple logs match and the map is
    /// needed for each.
    tx_map_cache: Option<Map>,
}

impl<'a> EvaluationContext<'a> {
    /// Creates a fresh evaluation context for the given block item.
    fn new(item: &'a CorrelatedBlockItem) -> Self {
        Self {
            item,
            tx_details_cache: None,
            matches: Vec::new(),
            matched_monitor_ids: HashSet::new(),
            decoded_call_cache: None,
            decoded_call_json_cache: None,
            decoded_logs_cache: HashMap::new(),
            tx_map_cache: None,
        }
    }

    /// Lazily decodes a log and caches the result — including failures — for
    /// subsequent reuse within this transaction's evaluation.
    ///
    /// On the first call for a given (tx_hash, log_index) pair the decode is
    /// attempted and the outcome (`Some` or `None`) is written into
    /// `decoded_logs_cache`. All later calls for the same key return the cached
    /// outcome immediately, so each log is decoded at most once per
    /// transaction regardless of how many monitors inspect it.
    fn get_or_decode_log(
        &mut self,
        abi_service: &AbiService,
        raw_log: &argus_core::models::Log,
    ) -> Option<Arc<DecodedLog>> {
        let cache_key =
            if let (Some(tx), Some(idx)) = (raw_log.transaction_hash(), raw_log.log_index()) {
                Some(LogCacheKey { tx_hash: tx, log_index: idx })
            } else {
                None
            };

        // If the key is present the decode was already attempted; return the
        // cached outcome (which may be `None` for a known failure).
        if let Some(key) = cache_key
            && let Some(cached) = self.decoded_logs_cache.get(&key)
        {
            return cached.clone();
        }

        let result = abi_service.decode_log(&raw_log.0).ok().map(Arc::new);

        if let Some(key) = cache_key {
            self.decoded_logs_cache.insert(key, result.clone());
        }

        result
    }

    /// Lazily builds the Rhai transaction map and caches it for reuse across
    /// multiple log matches within the same transaction.
    fn get_tx_map(&mut self) -> Map {
        if let Some(map) = &self.tx_map_cache {
            return map.clone();
        }
        let map = build_transaction_map(&self.item.transaction.0, self.item.receipt.as_ref());
        self.tx_map_cache = Some(map.clone());
        map
    }

    /// Lazily serializes and caches the transaction payload
    fn get_tx_details(&mut self) -> serde_json::Value {
        if let Some(ref details) = self.tx_details_cache {
            return details.clone();
        }
        let details =
            build_transaction_details_payload(&self.item.transaction.0, self.item.receipt.as_ref());
        self.tx_details_cache = Some(details.clone());
        details
    }

    /// Lazily serializes the decoded call to JSON, caching the result so that
    /// `serde_json::to_value` is called at most once per transaction even when
    /// multiple monitors match or a monitor has multiple actions.
    fn get_decoded_call_json(&mut self) -> Option<serde_json::Value> {
        if self.decoded_call_json_cache.is_none()
            && let Some(ref opt_call) = self.decoded_call_cache
        {
            self.decoded_call_json_cache = Some(opt_call.as_ref().map(|call| {
                serde_json::to_value(call.as_ref()).unwrap_or(serde_json::Value::Null)
            }));
        }
        self.decoded_call_json_cache.as_ref().and_then(|v| v.clone())
    }

    /// Returns `true` if this monitor has already produced a match during the
    /// log-aware pass and should be skipped in the transaction-only pass.
    fn has_matched(&self, monitor_id: i64) -> bool {
        self.matched_monitor_ids.contains(&monitor_id)
    }

    /// Records that the monitor has produced at least one match. Subsequent
    /// calls for the same `monitor_id` are idempotent (HashSet insert).
    fn mark_as_matched(&mut self, monitor_id: i64) {
        self.matched_monitor_ids.insert(monitor_id);
    }
}

impl RhaiFilteringEngine {
    /// Creates a new [`RhaiFilteringEngine`].
    ///
    /// Builds the shared Rhai [`Engine`] once at construction time so that all
    /// evaluation calls reuse the same configured instance.
    pub fn new(
        abi_service: Arc<AbiService>,
        config: RhaiConfig,
        monitor_manager: Arc<MonitorManager>,
    ) -> Self {
        let execution_timeout = config.execution_timeout;
        let engine = Arc::new(create_engine(config));
        Self { abi_service, engine, execution_timeout, monitor_manager }
    }

    /// First evaluation pass: checks log-aware monitors against each log.
    ///
    /// # Behavior
    ///
    /// 1. For each monitor, an **event-name pre-filter** is applied: if the
    ///    monitor's script statically names specific events (e.g. `log.name ==
    ///    "Transfer"`), [`AbiService::peek_event_name`] is used to skip logs
    ///    that don't match without full decoding.
    /// 2. Surviving logs are decoded lazily and cached in the
    ///    [`EvaluationContext`], ensuring each log is decoded at most once per
    ///    transaction while avoiding upfront cost for filtered logs.
    /// 3. For each surviving (monitor, log) pair, the monitor's pre-compiled
    ///    [`AST`] is evaluated. A match creates action entries via
    ///    [`create_log_matches`](Self::create_log_matches) and marks the
    ///    monitor to prevent re-evaluation in the tx-only pass.
    ///
    /// A monitor can match **multiple logs** in one transaction (e.g. a global
    /// ERC20 monitor matching all Transfer events). Each matching log produces
    /// its own set of action matches.
    fn evaluate_log_aware_monitors(
        &self,
        context: &mut EvaluationContext<'_>,
        monitors: &[&ClassifiedMonitor],
    ) -> Result<(), RhaiError> {
        if monitors.is_empty() {
            return Ok(());
        }

        for cm in monitors {
            let ast = &cm.analysis.ast;
            let filter_names = &cm.analysis.accessed_log_event_names;

            // Set up the scope variables outside the log loop.
            let mut scope = Scope::new();
            let mut scope_initialized = false;
            let mut base_len = 0;

            for raw_log in &context.item.logs {
                // 1. O(1) Pre-Filter
                if !filter_names.is_empty() {
                    match self.abi_service.peek_event_name(&raw_log.0) {
                        Some(event) if !filter_names.contains(&event.name) => continue,
                        None => continue,
                        _ => {} // Matches, proceed
                    }
                }

                // 2. Lazy Decode Log
                let Some(decoded_log) = context.get_or_decode_log(&self.abi_service, raw_log)
                else {
                    continue;
                };

                // 3. Only clone the tx_map once we are certain
                // there is at least one matching log in this transaction.
                if !scope_initialized {
                    scope.push_constant("tx", context.get_tx_map());

                    let mut decoded_call_result = None;
                    if cm.caps.contains(MonitorCapabilities::CALL) {
                        if context.decoded_call_cache.is_none() {
                            context.decoded_call_cache = Some(
                                self.abi_service
                                    .decode_function_input(&context.item.transaction.0)
                                    .ok()
                                    .map(Arc::new),
                            );
                        }
                        decoded_call_result = context.decoded_call_cache.as_ref().unwrap().clone();
                    }

                    scope.push_constant("decoded_call", CallProxy(decoded_call_result));
                    base_len = scope.len();
                    scope_initialized = true;
                }

                // 4. Push and Rewind
                scope.push("log", LogProxy(Some(decoded_log.clone())));

                let is_match = self.eval_ast_bool_secure(ast, &mut scope)?;

                // Rewind instantly removes `log` from the scope without dropping the `tx` map
                scope.rewind(base_len);

                if is_match {
                    self.create_log_matches(context, &cm.monitor, &decoded_log);
                }
            }
        }
        Ok(())
    }

    /// Second evaluation pass: checks remaining monitors in a transaction-only
    /// context (no log variable in scope).
    ///
    /// # Behavior
    ///
    /// Runs **after**
    /// [`evaluate_log_aware_monitors`](Self::evaluate_log_aware_monitors)
    /// and only evaluates monitors that have not already matched during the log
    /// pass. This prevents duplicate matches for hybrid monitors (scripts like
    /// `tx.value > 100 || log.name == "Transfer"`):
    ///
    /// - If the monitor matched a log → skip (already recorded in pass 1).
    /// - If no logs matched but the tx condition is true → create
    ///   `TransactionMatch`.
    /// - If both conditions were true → only the `LogMatch` from pass 1 is
    ///   kept.
    fn evaluate_tx_aware_monitors(
        &self,
        context: &mut EvaluationContext<'_>,
        monitors: &[&ClassifiedMonitor],
    ) -> Result<(), RhaiError> {
        if monitors.is_empty() {
            return Ok(());
        }

        let mut scope = Scope::new();
        scope.push_constant("tx", context.get_tx_map());
        let base_len = scope.len();

        for cm in monitors {
            if context.has_matched(cm.monitor.id) {
                continue;
            }

            // Use the pre-compiled AST stored on ClassifiedMonitor.
            let ast = &cm.analysis.ast;

            let mut decoded_call_result = None;
            if cm.caps.contains(MonitorCapabilities::CALL) {
                if context.decoded_call_cache.is_none() {
                    context.decoded_call_cache = Some(
                        self.abi_service
                            .decode_function_input(&context.item.transaction.0)
                            .ok()
                            .map(Arc::new),
                    );
                }
                decoded_call_result = context.decoded_call_cache.as_ref().unwrap().clone();
            }

            scope.push("log", LogProxy(None));
            scope.push("decoded_call", CallProxy(decoded_call_result));

            let is_match = self.eval_ast_bool_secure(ast, &mut scope)?;
            scope.rewind(base_len);

            if is_match {
                self.create_tx_matches(context, &cm.monitor);
            }
        }
        Ok(())
    }

    /// Creates and stores [`MonitorMatch`] entries for a log-based match.
    ///
    /// Produces one [`MonitorMatch`] per action configured on the monitor.
    /// Each match carries:
    /// - `LogDetails`: name, parameters, address, and log index.
    /// - Transaction details payload (lazily serialized, cached on `context`).
    /// - Decoded call JSON (lazily serialized, cached on `context`).
    ///
    /// Marks the monitor as having matched so the tx-only pass skips it.
    /// This is an idempotent operation (HashSet), so calling this function
    /// multiple times for the same monitor (one per matching log) is safe and
    /// correct — each call appends a distinct `LogMatch`.
    fn create_log_matches(
        &self,
        context: &mut EvaluationContext<'_>,
        monitor: &Monitor,
        decoded_log: &DecodedLog,
    ) {
        let log_match_payload = build_log_params_payload(&decoded_log.params);
        let tx_details = context.get_tx_details();
        let decoded_call_json = context.get_decoded_call_json();
        for action in &monitor.actions {
            let log_details = LogDetails {
                log_index: decoded_log.log.log_index.unwrap_or_default(),
                address: decoded_log.log.address(),
                name: decoded_log.name.clone(),
                params: log_match_payload.clone(),
            };
            context.matches.push(
                MonitorMatch::builder(
                    monitor.id,
                    monitor.name.clone(),
                    action.clone(),
                    context.item.transaction.block_number().unwrap_or_default(),
                    context.item.transaction.hash(),
                )
                .log_match(log_details, tx_details.clone())
                .decoded_call(decoded_call_json.clone())
                .build(),
            );
        }
        context.mark_as_matched(monitor.id);
    }

    /// Creates and stores [`MonitorMatch`] entries for a transaction-level
    /// match.
    ///
    /// Produces one [`MonitorMatch`] per action configured on the monitor.
    /// Each match carries the transaction details payload (lazily serialized,
    /// cached on `context`) and decoded call JSON if available.
    fn create_tx_matches(&self, context: &mut EvaluationContext<'_>, monitor: &Monitor) {
        let tx_match_payload = context.get_tx_details();
        let decoded_call_json = context.get_decoded_call_json();

        for action in &monitor.actions {
            context.matches.push(
                MonitorMatch::builder(
                    monitor.id,
                    monitor.name.clone(),
                    action.clone(),
                    context.item.transaction.block_number().unwrap_or_default(),
                    context.item.transaction.hash(),
                )
                .transaction_match(tx_match_payload.clone())
                .decoded_call(decoded_call_json.clone())
                .build(),
            );
        }
    }

    /// Evaluates a pre-compiled [`AST`] and expects a boolean result.
    ///
    /// Runs synchronously on the calling thread under the configured
    /// `execution_timeout` wall-clock deadline; a script that exceeds it is
    /// aborted with [`RhaiError::ExecutionTimeout`]. Operation-count and
    /// memory limits set at construction time (via [`create_engine`]) still
    /// apply.
    fn eval_ast_bool_secure(&self, ast: &AST, scope: &mut Scope<'_>) -> Result<bool, RhaiError> {
        let _guard = start_timeout(self.execution_timeout);
        self.engine.eval_ast_with_scope::<bool>(scope, ast).map_err(|e| {
            if is_execution_timeout_error(&e) {
                RhaiError::ExecutionTimeout { timeout: self.execution_timeout }
            } else {
                RhaiError::RuntimeError(e)
            }
        })
    }
}

#[async_trait]
impl FilteringEngine for RhaiFilteringEngine {
    /// Streaming evaluation loop for the live block ingestor.
    ///
    /// For each [`CorrelatedBlockData`] received:
    /// 1. Clones `self` (cheap — all fields are `Arc`-wrapped) and moves it
    ///    into `tokio::task::spawn_blocking` so that CPU-bound Rhai evaluation
    ///    runs on Rayon's dedicated thread pool without blocking async I/O
    ///    tasks.
    /// 2. Inside the blocking closure, `rayon::par_iter` evaluates all items in
    ///    the block concurrently across available CPU cores.
    /// 3. All resulting [`MonitorMatch`] values are forwarded to
    ///    `notifications_tx`.
    /// 4. Yields back to the Tokio executor once per block to keep other tasks
    ///    responsive.
    async fn run(
        &self,
        mut receiver: mpsc::Receiver<CorrelatedBlockData>,
        notifications_tx: mpsc::Sender<Vec<MonitorMatch>>,
    ) {
        while let Some(correlated_block) = receiver.recv().await {
            let engine = self.clone();

            // Evaluate all items in the block in parallel across Rayon's thread pool.
            // spawn_blocking keeps the tokio runtime responsive while the CPU-bound
            // work runs on a dedicated blocking thread.
            let all_matches = match tokio::task::spawn_blocking(move || {
                correlated_block
                    .items
                    .par_iter()
                    .flat_map(|item| match engine.evaluate_item(item) {
                        Ok(matches) => matches,
                        Err(e) => {
                            tracing::error!("Error evaluating item: {}", e);
                            vec![]
                        }
                    })
                    .collect::<Vec<MonitorMatch>>()
            })
            .await
            {
                Ok(matches) => matches,
                Err(e) => {
                    tracing::error!(
                        "Filtering task panicked or was cancelled; block's matches are lost: {e}"
                    );
                    continue;
                }
            };

            if !all_matches.is_empty()
                && let Err(e) = notifications_tx.send(all_matches).await
            {
                tracing::error!("Failed to send notification matches batch: {}", e);
            }

            // Yield once per block to keep the executor unblocked.
            tokio::task::yield_now().await;
        }
    }

    fn evaluate_item(&self, item: &CorrelatedBlockItem) -> Result<Vec<MonitorMatch>, RhaiError> {
        let assets = self.monitor_manager.load();
        let mut context = EvaluationContext::new(item);

        // Load the pre-categorised monitor lists produced by static analysis of
        // each monitor's filter script at load time:
        //   log_aware  — scripts that reference `log` / `log.params`
        //   tx_aware   — scripts that do not reference log data at all
        let log_aware_monitors: Vec<_> = assets
            .log_aware_monitors
            .iter()
            .filter_map(|id| assets.monitors_by_id.get(id))
            .collect();

        let tx_aware_monitors: Vec<_> = assets
            .tx_aware_monitors
            .iter()
            .filter_map(|id| assets.monitors_by_id.get(id))
            .collect();

        // --- Pass 1: evaluate log-aware monitors. ---
        // Skipped entirely when the item carries no logs, avoiding all
        // lexical peeking and Rhai calls for that pass.
        if !item.logs.is_empty() {
            self.evaluate_log_aware_monitors(&mut context, &log_aware_monitors)?;
        }

        // --- Pass 2: evaluate tx-aware (and unmatched hybrid) monitors. ---
        // Monitors that already produced a LogMatch in pass 1 are skipped.
        self.evaluate_tx_aware_monitors(&mut context, &tx_aware_monitors)?;

        Ok(context.matches)
    }

    fn requires_receipt_data(&self) -> bool {
        self.monitor_manager.load().requires_receipts
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use alloy::{
        primitives::{Address, B256, Bytes, U256, address, b256},
        sol_types::SolValue,
    };
    use argus_abi::{
        AbiService,
        test_utils::{create_test_abi_service, erc20_abi_json},
    };
    use argus_core::{
        config::RhaiConfig,
        models::{
            Log,
            monitor_match::{LogDetails, MatchData},
            transaction::Transaction,
        },
        test_utils::{LogBuilder, MonitorBuilder, TransactionBuilder},
    };
    use argus_rhai::RhaiCompiler;

    use super::*;

    const TRANSFER_EVENT_TOPIC: B256 =
        b256!("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");
    const FROM_TOPIC: B256 =
        b256!("000000000000000000000000a0b86a33e6ba3e10e4b86c8c5a3c6b2e6a2e8f1e");
    const TO_TOPIC: B256 =
        b256!("000000000000000000000000b1c97a44f7ca4e21f5b97d8d6a4d7c3f7b3f9e2f");
    const CONTRACT_ADDRESS: Address = address!("0000000000000000000000000000000000000001");
    const TX_INPUT_DATA: &str = "0xa9059cbb00000000000000000000000011223344556677889900aabbccddeeff1122334400000000000000000000000000000000000000000000000000000000000005dc";

    fn create_test_log_and_tx_with_topics(
        log_address: Address,
        topics: Vec<B256>,
        data: Bytes,
    ) -> (Transaction, Log) {
        let tx = TransactionBuilder::new().build();
        let log = LogBuilder::new().address(log_address).topics(topics).data(data).build();
        (tx, log)
    }

    fn setup_engine_with_monitors(
        monitors: Vec<Monitor>,
        abi_service: Arc<AbiService>,
    ) -> RhaiFilteringEngine {
        let config = RhaiConfig::default();
        let compiler = Arc::new(RhaiCompiler::new(config.clone()));
        let monitor_manager =
            Arc::new(MonitorManager::new(monitors, Arc::clone(&compiler), abi_service.clone()));
        RhaiFilteringEngine::new(abi_service, config, monitor_manager)
    }

    #[test]
    fn test_evaluate_item_log_based_match() {
        let abi_service = create_test_abi_service(&[("erc20", erc20_abi_json())]);

        abi_service.link_abi(CONTRACT_ADDRESS, "erc20").unwrap();

        let monitor = MonitorBuilder::new()
            .id(1)
            .address(&CONTRACT_ADDRESS.to_checksum(None))
            .abi_name("erc20")
            .filter_script("log.name == \"Transfer\" ")
            .actions(vec!["action1".to_string(), "action2".to_string()])
            .build();
        let monitors = vec![monitor];
        let engine = setup_engine_with_monitors(monitors, abi_service.clone());

        let amount_data = U256::from(1000).abi_encode().into();
        let (tx, log) = create_test_log_and_tx_with_topics(
            CONTRACT_ADDRESS,
            vec![TRANSFER_EVENT_TOPIC, FROM_TOPIC, TO_TOPIC],
            amount_data,
        );
        let item = CorrelatedBlockItem::new(tx, vec![log], None);

        let matches = engine.evaluate_item(&item).unwrap();
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].monitor_id, 1);
        assert_eq!(matches[0].action_name, "action1");
        assert_eq!(matches[1].monitor_id, 1);
        assert_eq!(matches[1].action_name, "action2");
    }

    #[test]
    fn test_evaluate_item_transaction_based_match() {
        let abi_service = create_test_abi_service(&[("erc20", erc20_abi_json())]);
        let monitor = MonitorBuilder::new()
            .id(1)
            .filter_script("tx.value > parse_bigint(\"100\")")
            .actions(vec!["action1".to_string()])
            .build();
        let monitors = vec![monitor];
        let engine = setup_engine_with_monitors(monitors, abi_service);

        let tx = TransactionBuilder::new().value(U256::from(150)).build();
        // This item has no logs, but should still be evaluated by the tx monitor
        let item = CorrelatedBlockItem::new(tx, vec![], None);

        let matches = engine.evaluate_item(&item).unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].monitor_id, 1);
        assert_eq!(matches[0].action_name, "action1");
    }

    #[test]
    fn test_evaluate_item_script_exceeding_execution_timeout() {
        let abi_service = create_test_abi_service(&[("erc20", erc20_abi_json())]);

        let monitor = MonitorBuilder::new()
            .id(1)
            .filter_script("let x = 0; while x >= 0 { x += 1; } x < 0")
            .actions(vec!["action1".to_string()])
            .build();

        // Unlimited operations: the wall-clock deadline is the only bound.
        let config = RhaiConfig {
            max_operations: 0,
            execution_timeout: Duration::from_millis(100),
            ..RhaiConfig::default()
        };
        let compiler = Arc::new(RhaiCompiler::new(config.clone()));
        let monitor_manager = Arc::new(MonitorManager::new(
            vec![monitor],
            compiler,
            abi_service.clone(),
        ));
        let engine = RhaiFilteringEngine::new(abi_service, config, monitor_manager);

        let tx = TransactionBuilder::new().build();
        let item = CorrelatedBlockItem::new(tx, vec![], None);

        let start = Instant::now();
        let result = engine.evaluate_item(&item);

        assert!(matches!(
            result,
            Err(RhaiError::ExecutionTimeout { timeout }) if timeout == Duration::from_millis(100)
        ));
        assert!(start.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn test_evaluate_item_no_match_for_tx_monitor() {
        let abi_service = create_test_abi_service(&[("erc20", erc20_abi_json())]);

        let monitor = MonitorBuilder::new()
            .id(1)
            .filter_script("tx.value > parse_bigint(\"200\")")
            .actions(vec!["action1".to_string()])
            .build();
        let monitors = vec![monitor];
        let engine = setup_engine_with_monitors(monitors, abi_service);

        let tx = TransactionBuilder::new().value(U256::from(150)).build();
        let item = CorrelatedBlockItem::new(tx, vec![], None);

        let matches = engine.evaluate_item(&item).unwrap();
        assert!(matches.is_empty());
    }

    #[test]
    fn test_evaluate_item_mixed_monitors_both_match() {
        let abi_service = create_test_abi_service(&[("erc20", erc20_abi_json())]);
        abi_service.link_abi(CONTRACT_ADDRESS, "erc20").unwrap();

        let log_monitor = MonitorBuilder::new()
            .id(1)
            .address(&CONTRACT_ADDRESS.to_checksum(None))
            .abi_name("erc20")
            .filter_script("log.name == \"Transfer\" ")
            .actions(vec!["log_action".to_string()])
            .build();
        let tx_monitor = MonitorBuilder::new()
            .id(2)
            .filter_script("tx.value > parse_bigint(\"100\")")
            .actions(vec!["tx_action".to_string()])
            .build();
        let monitors = vec![log_monitor.clone(), tx_monitor.clone()];
        let engine = setup_engine_with_monitors(monitors, abi_service);

        // Create a transaction that will match the transaction-level monitor
        let tx = TransactionBuilder::new().value(U256::from(120)).build();

        // Create a log that will match the log-based monitor
        let amount_data = U256::from(1000).abi_encode().into();
        let log = LogBuilder::new()
            .address(CONTRACT_ADDRESS)
            .topics(vec![TRANSFER_EVENT_TOPIC, FROM_TOPIC, TO_TOPIC])
            .data(amount_data)
            .build();

        // The item contains both the transaction and the log
        let item = CorrelatedBlockItem::new(tx, vec![log], None);

        let matches = engine.evaluate_item(&item).unwrap();
        assert_eq!(matches.len(), 2);
        let mut ids: Vec<i64> = matches.iter().map(|m| m.monitor_id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2]);
    }

    #[test]
    fn test_evaluate_item_filter_by_log_param() {
        let abi_service = create_test_abi_service(&[("erc20", erc20_abi_json())]);
        abi_service.link_abi(CONTRACT_ADDRESS, "erc20").unwrap();

        let monitor = MonitorBuilder::new()
            .id(1)
            .address(&CONTRACT_ADDRESS.to_checksum(None))
            .abi_name("erc20")
            .filter_script("log.name == \"Transfer\" && log.params.value > parse_bigint(\"100\")")
            .actions(vec!["action1".to_string()])
            .build();
        let monitors = vec![monitor];
        let engine = setup_engine_with_monitors(monitors, abi_service);

        // keccak256("Transfer(address,address,uint256)")
        let amount_data = U256::from(150).abi_encode().into();
        let (tx, log) = create_test_log_and_tx_with_topics(
            CONTRACT_ADDRESS,
            vec![TRANSFER_EVENT_TOPIC, FROM_TOPIC, TO_TOPIC],
            amount_data,
        );

        let item = CorrelatedBlockItem::new(tx, vec![log], None);

        let matches = engine.evaluate_item(&item).unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].monitor_id, 1);
        assert_eq!(matches[0].action_name, "action1");
        assert!(matches!(
            &matches[0].match_data,
            MatchData::Log { log_details, .. } if log_details.name == "Transfer"
        ));
    }

    #[test]
    fn test_evaluate_item_no_decoded_logs_still_triggers_tx_monitor() {
        let abi_service = create_test_abi_service(&[("erc20", erc20_abi_json())]);
        let monitor = MonitorBuilder::new().id(1).actions(vec!["action1".to_string()]).build();
        let monitors = vec![monitor];
        let engine = setup_engine_with_monitors(monitors, abi_service);
        let tx = TransactionBuilder::new().build();
        let item = CorrelatedBlockItem::new(tx, vec![], None);

        let matches = engine.evaluate_item(&item).unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].monitor_id, 1);
    }

    #[test]
    fn test_evaluate_item_tx_only_monitor_with_decoded_call_match() {
        let abi_service = create_test_abi_service(&[("erc20", erc20_abi_json())]);
        abi_service.link_abi(CONTRACT_ADDRESS, "erc20").unwrap();

        // This monitor is specific to an address and cares about decoded calldata, but
        // not logs.
        let monitor = MonitorBuilder::new()
            .id(1)
            .address(&CONTRACT_ADDRESS.to_checksum(None))
            .abi_name("erc20")
            .filter_script(
                r#"decoded_call.name == "transfer" && decoded_call.params._value > parse_bigint(1000)"#,
            )
            .actions(vec!["test-action".to_string()])
            .build();
        let engine = setup_engine_with_monitors(vec![monitor], abi_service.clone());

        // This transaction matches the monitor's script.
        let tx = TransactionBuilder::new()
            .to(Some(CONTRACT_ADDRESS))
            .input(TX_INPUT_DATA.parse().unwrap())
            .build();
        let item = CorrelatedBlockItem::new(tx, vec![], None);

        let matches = engine.evaluate_item(&item).unwrap();
        assert_eq!(matches.len(), 1, "Should find one match for high-value transfer");
        assert_eq!(matches[0].monitor_id, 1);
    }

    #[test]
    fn test_evaluate_item_log_aware_monitor_with_decoded_call_match() {
        let abi_service = create_test_abi_service(&[("erc20", erc20_abi_json())]);
        abi_service.link_abi(CONTRACT_ADDRESS, "erc20").unwrap();

        // This monitor cares about both logs and decoded calldata.
        let monitor = MonitorBuilder::new()
            .id(1)
            .address(&CONTRACT_ADDRESS.to_checksum(None))
            .abi_name("erc20")
            .filter_script(
                r#"log.name == "Transfer" && decoded_call.name == "transfer" && decoded_call.params._value > parse_bigint(1000)"#,
            )
            .actions(vec!["test-action".to_string()])
            .build();
        let engine = setup_engine_with_monitors(vec![monitor], abi_service.clone());

        // This transaction matches the monitor's script.
        let tx = TransactionBuilder::new()
            .to(Some(CONTRACT_ADDRESS))
            .input(TX_INPUT_DATA.parse().unwrap())
            .build();
        let amount_data = U256::from(1500).abi_encode().into();
        let (_, log) = create_test_log_and_tx_with_topics(
            CONTRACT_ADDRESS,
            vec![TRANSFER_EVENT_TOPIC, FROM_TOPIC, TO_TOPIC],
            amount_data,
        );
        let item = CorrelatedBlockItem::new(tx, vec![log], None);

        let matches = engine.evaluate_item(&item).unwrap();
        assert_eq!(matches.len(), 1, "Should find one match for high-value transfer with log");
        assert_eq!(matches[0].monitor_id, 1);
    }

    #[test]
    fn test_decoded_call_is_null_for_non_matching_selector() {
        let abi_service = create_test_abi_service(&[("erc20", erc20_abi_json())]);
        abi_service.link_abi(CONTRACT_ADDRESS, "erc20").unwrap();

        let monitor = MonitorBuilder::new()
            .id(1)
            .filter_script("decoded_call.name == \"\"") // Check for empty decoded_call name
            .actions(vec!["action1".to_string()])
            .build();
        let monitors = vec![monitor];
        let engine = setup_engine_with_monitors(monitors, abi_service);

        // This transaction has a `to` address, so decoding will be attempted, but
        // the selector is invalid, so it will fail.
        let tx = TransactionBuilder::new()
            .to(Some(CONTRACT_ADDRESS))
            .input(b256!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").into())
            .build();
        let item = CorrelatedBlockItem::new(tx, vec![], None);

        let matches = engine.evaluate_item(&item).unwrap();
        assert_eq!(matches.len(), 1, "Should match when decoded_call is null");
    }

    #[test]
    fn test_requires_receipt_data_flag_set_correctly() {
        let abi_service = create_test_abi_service(&[("erc20", erc20_abi_json())]);
        // --- Scenario 1: A monitor explicitly uses a receipt field ---
        let monitor_no_receipt = MonitorBuilder::new()
            .id(1)
            .filter_script("tx.value > parse_bigint(\"100\")") // No receipt needed
            .build();
        let monitor_requires_receipt = MonitorBuilder::new()
            .id(2)
            .filter_script("tx.status == 1") // This requires receipt data
            .build();
        let monitors_with_receipt_field = vec![monitor_no_receipt, monitor_requires_receipt];
        let engine = setup_engine_with_monitors(monitors_with_receipt_field, abi_service.clone());
        assert!(engine.requires_receipt_data(), "Should require receipts when 'tx.status' is used");

        // --- Scenario 2: No monitors use any receipt fields ---
        let monitor_no_receipt = MonitorBuilder::new()
            .id(1)
            .filter_script("tx.value > parse_bigint(\"100\")") // No receipt needed
            .build();
        let monitor_no_receipt_too = MonitorBuilder::new()
            .id(2)
            .filter_script("log.name == \"Transfer\" ") // No receipt needed
            .build();
        let monitors_without_receipt_field = vec![monitor_no_receipt, monitor_no_receipt_too];
        let engine_no_receipts =
            setup_engine_with_monitors(monitors_without_receipt_field, abi_service.clone());
        assert!(
            !engine_no_receipts.requires_receipt_data(),
            "Should not require receipts when no receipt fields are used"
        );

        // --- Scenario 3: A receipt field appears in a comment or string (proves AST
        // analysis works) ---
        let monitor_commented_field =
            MonitorBuilder::new().id(1).filter_script("// This script checks tx.status").build();
        let monitor = MonitorBuilder::new()
            .id(2)
            .filter_script("tx.value > parse_bigint(\"100\") && log.name == \"tx.gas_used\"")
            .build();
        let monitors_with_receipt_field_in_comment = vec![monitor_commented_field, monitor];
        let engine_ast_check =
            setup_engine_with_monitors(monitors_with_receipt_field_in_comment, abi_service.clone());
        assert!(
            !engine_ast_check.requires_receipt_data(),
            "Should not require receipts when fields are only in comments or strings"
        );

        // --- Scenario 4: A mix of valid and invalid scripts ---
        let monitor_valid_no_receipt = MonitorBuilder::new()
            .id(1)
            .filter_script("tx.value > parse_bigint(\"100\")") // Valid, no receipt
            .build();
        let monitor_valid_requires_receipt = MonitorBuilder::new()
            .id(2)
            .filter_script("tx.gas_used > parse_bigint(\"50000\")") // Valid, needs receipt
            .build();
        let monitor_invalid = MonitorBuilder::new()
            .id(3)
            .filter_script("tx.value >") // Invalid syntax
            .build();
        let monitors_mixed_validity =
            vec![monitor_valid_no_receipt, monitor_valid_requires_receipt, monitor_invalid];
        let engine_mixed = setup_engine_with_monitors(monitors_mixed_validity, abi_service);
        assert!(
            engine_mixed.requires_receipt_data(),
            "Should require receipts even if other scripts are invalid"
        );
    }

    #[test]
    fn test_evaluate_item_with_evm_wrappers() {
        let abi_service = create_test_abi_service(&[("erc20", erc20_abi_json())]);
        let monitor = MonitorBuilder::new()
            .id(1)
            .filter_script("tx.value > ether(1.5)")
            .actions(vec!["action1".to_string()])
            .build();
        let monitors = vec![monitor];
        let engine = setup_engine_with_monitors(monitors, abi_service);

        // This transaction's value is 2 ETH, which should trigger the monitor
        let tx_match = TransactionBuilder::new()
            .value(U256::from(2) * U256::from(10).pow(U256::from(18)))
            .build();
        let item_match = CorrelatedBlockItem::new(tx_match.clone(), vec![], None);

        // This transaction's value is 1 ETH, which should NOT trigger the monitor
        let tx_no_match = TransactionBuilder::new()
            .value(U256::from(1) * U256::from(10).pow(U256::from(18)))
            .build();
        let item_no_match = CorrelatedBlockItem::new(tx_no_match.clone(), vec![], None);

        // Test matching case
        let matches = engine.evaluate_item(&item_match).unwrap();
        assert_eq!(matches.len(), 1, "Should find one match for value > 1.5 ether");
        assert_eq!(matches[0].monitor_id, 1);
        assert_eq!(matches[0].transaction_hash, tx_match.hash());
        assert!(matches!(matches[0].match_data, MatchData::Transaction { .. }));

        // Test non-matching case
        let no_matches = engine.evaluate_item(&item_no_match).unwrap();
        assert!(no_matches.is_empty(), "Should find no matches for value <= 1.5 ether");
    }

    #[test]
    fn test_evaluate_item_global_log_monitor_match() {
        let abi_service = create_test_abi_service(&[("erc20", erc20_abi_json())]);

        let addr1 = address!("1111111111111111111111111111111111111111");
        let addr2 = address!("2222222222222222222222222222222222222222");

        // Link ABIs to the addresses so logs can be decoded
        abi_service.link_abi(addr1, "erc20").unwrap();
        abi_service.link_abi(addr2, "erc20").unwrap();

        // This monitor has no address, so it should run on logs from ANY address.
        let global_monitor = MonitorBuilder::new()
            .id(100)
            .abi_name("erc20")
            .filter_script("log.name == \"Transfer\" ")
            .actions(vec!["global_action".to_string()])
            .build();

        let monitors = vec![global_monitor];
        let engine = setup_engine_with_monitors(monitors, abi_service);

        let amount_data: Bytes = U256::from(1000).abi_encode().into();
        let (tx, log1) = create_test_log_and_tx_with_topics(
            addr1,
            vec![TRANSFER_EVENT_TOPIC, FROM_TOPIC, TO_TOPIC],
            amount_data.clone(),
        );
        let (_, log2) = create_test_log_and_tx_with_topics(
            addr2,
            vec![TRANSFER_EVENT_TOPIC, FROM_TOPIC, TO_TOPIC],
            amount_data,
        );
        // This log should be ignored by the monitor
        let value_transfered_topic =
            b256!("1dd763d000642c1a04c2286c7b36731314905d0623c408543a35b0a50344c66a");
        let (_, log3) = create_test_log_and_tx_with_topics(
            addr1,
            vec![value_transfered_topic],
            Bytes::default(),
        );

        let item = CorrelatedBlockItem::new(tx, vec![log1.clone(), log2.clone(), log3], None);

        let matches = engine.evaluate_item(&item).unwrap();

        // We expect two matches, one for each "Transfer" log.
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].monitor_id, 100);
        assert_eq!(matches[1].monitor_id, 100);
        assert_eq!(matches[0].block_number, item.transaction.block_number().unwrap_or_default());
        assert!(matches!(
            matches[0].match_data,
            MatchData::Log { log_details: LogDetails { address, .. }, .. } if address == addr1
        ));
        assert_eq!(matches[1].block_number, item.transaction.block_number().unwrap_or_default());
        assert!(matches!(
            matches[1].match_data,
            MatchData::Log { log_details: LogDetails { address, .. }, .. } if address == addr2
        ));
    }

    #[test]
    fn test_evaluate_item_hybrid_monitor_tx_match_no_logs() {
        let abi_service = create_test_abi_service(&[]);

        // This monitor should match on high-value transactions OR on "Transfer" logs.
        let monitor = MonitorBuilder::new()
            .id(1)
            .abi_name("erc20")
            .filter_script(
                r#" 
            tx.value > parse_bigint(100) || log.name == "Transfer"
        "#,
            )
            .actions(vec!["test-action".to_string()])
            .build();
        let engine = setup_engine_with_monitors(vec![monitor], abi_service.clone());

        // This transaction matches the `tx.value` part of the script and has NO logs.
        let tx = TransactionBuilder::new().value(U256::from(150)).build();
        let item = CorrelatedBlockItem::new(tx, vec![], None);

        let matches = engine.evaluate_item(&item).unwrap();
        assert_eq!(matches.len(), 1, "Should match on tx.value even with no logs");
        assert_eq!(matches[0].monitor_id, 1);
        assert!(matches!(matches[0].match_data, MatchData::Transaction { .. }));
    }

    #[test]
    fn test_evaluate_item_hybrid_monitor_log_match_only() {
        let abi_service = create_test_abi_service(&[("erc20", erc20_abi_json())]);
        abi_service.link_abi(CONTRACT_ADDRESS, "erc20").unwrap();

        let monitor = MonitorBuilder::new()
            .id(1)
            .abi_name("erc20")
            .filter_script(
                r#" 
            tx.value > parse_bigint(100) || log.name == "Transfer"
        "#,
            )
            .actions(vec!["test-action".to_string()])
            .build();
        let engine = setup_engine_with_monitors(vec![monitor], abi_service.clone());

        // This transaction does NOT match the tx.value, but its log does.
        let tx = TransactionBuilder::new().value(U256::from(50)).build();
        let amount_data = U256::from(1000).abi_encode().into();
        let (_, log) = create_test_log_and_tx_with_topics(
            CONTRACT_ADDRESS,
            vec![TRANSFER_EVENT_TOPIC, FROM_TOPIC, TO_TOPIC],
            amount_data,
        );
        let item = CorrelatedBlockItem::new(tx, vec![log], None);

        let matches = engine.evaluate_item(&item).unwrap();
        assert_eq!(matches.len(), 1, "Should match on log.name");
        assert_eq!(matches[0].monitor_id, 1);
        assert!(matches!(matches[0].match_data, MatchData::Log { .. }));
    }

    #[test]
    fn test_evaluate_item_hybrid_monitor_prefers_log_match() {
        let abi_service = create_test_abi_service(&[("erc20", erc20_abi_json())]);
        abi_service.link_abi(CONTRACT_ADDRESS, "erc20").unwrap();

        let monitor = MonitorBuilder::new()
            .id(1)
            .abi_name("erc20")
            .filter_script(
                r#" 
            tx.value > parse_bigint(100) || log.name == "Transfer"
        "#,
            )
            .actions(vec!["test-action".to_string()])
            .build();
        let engine = setup_engine_with_monitors(vec![monitor], abi_service.clone());

        // This transaction matches BOTH the tx.value and the log.name.
        let tx = TransactionBuilder::new().value(U256::from(150)).build();
        let amount_data = U256::from(1000).abi_encode().into();
        let (_, log) = create_test_log_and_tx_with_topics(
            CONTRACT_ADDRESS,
            vec![TRANSFER_EVENT_TOPIC, FROM_TOPIC, TO_TOPIC],
            amount_data,
        );
        let item = CorrelatedBlockItem::new(tx, vec![log], None);

        let matches = engine.evaluate_item(&item).unwrap();
        // It should only produce ONE match, and it should be the more specific
        // LogMatch.
        assert_eq!(matches.len(), 1, "Should only produce one match");
        assert_eq!(matches[0].monitor_id, 1);
        assert!(matches!(matches[0].match_data, MatchData::Log { .. }), "Should prefer LogMatch");
    }

    #[test]
    fn test_safe_null_access_on_decoded_call() {
        let abi_service = create_test_abi_service(&[]);

        // This script would fail at runtime if the dot operator on a null
        // `decoded_call` was not handled safely.
        let monitor = MonitorBuilder::new()
            .id(1)
            .filter_script(r#"decoded_call.name == "nonexistent""#)
            .actions(vec!["test-action".to_string()])
            .build();
        let engine = setup_engine_with_monitors(vec![monitor], abi_service.clone());

        // This item has no decoded_call, so the variable will be `()`.
        let tx = TransactionBuilder::new().build();
        let item = CorrelatedBlockItem::new(tx, vec![], None);

        // The script should evaluate to `false` and not error.
        let matches = engine.evaluate_item(&item).unwrap();
        assert!(matches.is_empty(), "Should not match and should not error");
    }

    #[test]
    fn test_safe_null_access_on_log() {
        let abi_service = create_test_abi_service(&[]);

        // This script would fail if `log.name` access on a null `log` errored.
        // This is a transaction-only evaluation context.
        let monitor = MonitorBuilder::new()
            .id(1)
            .filter_script(r#"log.name == "nonexistent""#)
            .actions(vec!["test-action".to_string()])
            .build();
        let engine = setup_engine_with_monitors(vec![monitor], abi_service.clone());

        // This item has no logs, so `log` will be `()` during the tx-only pass.
        let tx = TransactionBuilder::new().build();
        let item = CorrelatedBlockItem::new(tx, vec![], None);

        // The script should evaluate to `false` and not error.
        let matches = engine.evaluate_item(&item).unwrap();
        assert!(matches.is_empty(), "Should not match and should not error");
    }

    #[test]
    fn test_safe_null_access_on_decoded_call_with_valid_call() {
        let abi_service = create_test_abi_service(&[("erc20", erc20_abi_json())]);
        abi_service.link_abi(CONTRACT_ADDRESS, "erc20").unwrap();

        let monitor = MonitorBuilder::new()
            .id(1)
            .address(&CONTRACT_ADDRESS.to_checksum(None))
            .abi_name("erc20")
            .filter_script(r#"decoded_call.name == "transfer""#)
            .actions(vec!["test-action".to_string()])
            .build();
        let engine = setup_engine_with_monitors(vec![monitor], abi_service.clone());

        // This transaction matches the monitor's script.
        let tx = TransactionBuilder::new()
            .to(Some(CONTRACT_ADDRESS))
            .input(TX_INPUT_DATA.parse().unwrap())
            .build();
        let item = CorrelatedBlockItem::new(tx, vec![], None);

        let matches = engine.evaluate_item(&item).unwrap();
        assert_eq!(matches.len(), 1, "Should find one match for the transfer call");
        assert_eq!(matches[0].monitor_id, 1);
    }

    #[test]
    fn test_create_log_matches_payload() {
        let abi_service = create_test_abi_service(&[]);
        let engine = setup_engine_with_monitors(vec![], abi_service);

        let tx = TransactionBuilder::new().value(U256::from(123)).build();
        let log = LogBuilder::new().log_index(42).build();
        let item = CorrelatedBlockItem::new(tx.clone(), vec![log.clone()], None);
        let mut context = EvaluationContext::new(&item);

        let monitor = MonitorBuilder::new().actions(vec!["n1".to_string()]).build();
        let decoded_log =
            DecodedLog { log: log.into(), name: "TestEvent".to_string(), params: vec![] };

        engine.create_log_matches(&mut context, &monitor, &decoded_log);

        assert_eq!(context.matches.len(), 1);
        let monitor_match = &context.matches[0];

        match &monitor_match.match_data {
            MatchData::Log { log_details, tx_details } => {
                // Verify log details
                assert_eq!(log_details.name, "TestEvent");
                assert_eq!(log_details.log_index, 42);

                // Verify transaction details
                let tx_details_map = tx_details.as_object().unwrap();
                assert_eq!(tx_details_map.get("value").unwrap().as_str().unwrap(), "123");
                assert_eq!(
                    tx_details_map.get("hash").unwrap().as_str().unwrap(),
                    tx.hash().to_string()
                );
            }
            _ => panic!("Expected a log match"),
        }
    }

    #[test]
    fn test_create_tx_matches_payload() {
        let abi_service = create_test_abi_service(&[]);
        let engine = setup_engine_with_monitors(vec![], abi_service);

        let tx = TransactionBuilder::new().value(U256::from(456)).build();
        let item = CorrelatedBlockItem::new(tx.clone(), vec![], None);
        let mut context = EvaluationContext::new(&item);

        let monitor = MonitorBuilder::new().actions(vec!["n1".to_string()]).build();

        engine.create_tx_matches(&mut context, &monitor);

        assert_eq!(context.matches.len(), 1);
        let monitor_match = &context.matches[0];

        match &monitor_match.match_data {
            MatchData::Transaction { details } => {
                let tx_details_map = details.as_object().unwrap();
                assert_eq!(tx_details_map.get("value").unwrap().as_str().unwrap(), "456");
                assert_eq!(
                    tx_details_map.get("hash").unwrap().as_str().unwrap(),
                    tx.hash().to_string()
                );
            }
            _ => panic!("Expected a transaction match"),
        }
    }
}
