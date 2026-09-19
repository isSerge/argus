//! Argus engine crate.
//!
//! Provides the ABI decoding, monitoring, and block processing engines.

pub mod alert_manager;
pub mod block_ingestor;
pub mod block_processor;
pub mod filtering;
pub mod outbox_processor;
mod polling;
mod reorg;
