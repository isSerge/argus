//! This module provides the Rhai engine for scripting and filtering in Argus.

pub mod compiler;
pub mod conversions;
pub mod create_engine;
pub mod proxies;
pub mod validator;

pub use compiler::{RhaiCompiler, RhaiCompilerError, ScriptAnalysis};
pub use conversions::{
    get_valid_log_rhai_paths, get_valid_receipt_rhai_paths, get_valid_tx_rhai_paths,
};
pub use create_engine::{TimeoutGuard, create_engine, is_execution_timeout_error, start_timeout};
pub use validator::{RhaiScriptValidationError, RhaiScriptValidationResult, RhaiScriptValidator};
