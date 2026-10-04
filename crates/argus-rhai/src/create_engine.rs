use std::{
    cell::Cell,
    time::{Duration, Instant},
};

use argus_core::config::RhaiConfig;
use rhai::{Engine, EvalAltResult, packages::Package};
use rhai_bigint::BigIntPackage;
use rhai_evm::EvmPackage;

use super::proxies::register_proxies;

/// Token carried by `ErrorTerminated` aborts triggered by the execution
/// timeout guard, so they can be distinguished from other errors.
const EXECUTION_TIMEOUT_TOKEN: &str = "__argus_execution_timeout__";

thread_local! {
    // Deadline for the script evaluating on this thread; per-thread because
    // the shared Engine evaluates concurrently on many threads.
    static EVAL_DEADLINE: Cell<Option<Instant>> = const { Cell::new(None) };
}

/// RAII guard clearing the thread-local evaluation deadline on drop.
#[must_use = "the deadline is cleared when the guard is dropped"]
pub struct TimeoutGuard;

impl Drop for TimeoutGuard {
    fn drop(&mut self) {
        EVAL_DEADLINE.with(|d| d.set(None));
    }
}

/// Installs a wall-clock deadline for script evaluation on the current
/// thread, enforced by engines built with [`create_engine`].
pub fn start_timeout(timeout: Duration) -> TimeoutGuard {
    EVAL_DEADLINE.with(|d| d.set(Some(Instant::now() + timeout)));
    TimeoutGuard
}

/// True when the installed deadline (if any) has passed.
fn deadline_exceeded() -> bool {
    EVAL_DEADLINE.with(|d| d.get().is_some_and(|deadline| deadline <= Instant::now()))
}

/// True if the error is a script execution aborted by the timeout guard.
pub fn is_execution_timeout_error(err: &EvalAltResult) -> bool {
    matches!(err, EvalAltResult::ErrorTerminated(token, _)
        if token.clone().into_string().is_ok_and(|s| s == EXECUTION_TIMEOUT_TOKEN))
}

/// Creates a Rhai engine with security features and custom configurations.
/// Used for both RhaiCompiler (AST compilation) and RhaiFilteringEngine (AST
/// evaluation).
pub fn create_engine(rhai_config: RhaiConfig) -> Engine {
    let mut engine = Engine::new();

    // Apply security limits
    engine.set_max_operations(rhai_config.max_operations);
    engine.set_max_call_levels(rhai_config.max_call_levels);
    engine.set_max_string_size(rhai_config.max_string_size);
    engine.set_max_array_size(rhai_config.max_array_size);

    // Abort evaluation with a sentinel token once the per-script wall-clock
    // deadline (see `start_timeout`) has passed.
    engine.on_progress(|_| deadline_exceeded().then(|| EXECUTION_TIMEOUT_TOKEN.into()));

    // Disable dangerous language features
    const DANGEROUS_SYMBOLS: &[&str] = &[
        "eval", "import", "export", "print", "debug", "File", "file", "http", "net", "system",
        "process", "thread", "spawn",
    ];
    for &symbol in DANGEROUS_SYMBOLS {
        engine.disable_symbol(symbol);
    }

    // Register BigInt package for handling large integers in token values
    BigIntPackage::new().register_into_engine(&mut engine);
    // Register EVM wrappers for handling decoded logs and calls
    EvmPackage::new().register_into_engine(&mut engine);

    // Register custom proxies for accessing decoded logs and calls
    register_proxies(&mut engine);

    engine
}
