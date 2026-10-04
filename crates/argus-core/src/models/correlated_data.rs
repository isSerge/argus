//! This module defines data structures for correlated blockchain data.

use alloy::primitives::TxHash;

use crate::models::{Log, TransactionReceipt, transaction::Transaction};

/// Represents a correlated set of data for a single transaction within a block.
/// This is the unit of data that the `FilteringEngine` will evaluate.
#[derive(Debug, Clone)]
pub struct CorrelatedBlockItem {
    /// The transaction itself.
    pub transaction: Transaction,

    /// All logs associated with this transaction.
    pub logs: Vec<Log>,

    /// The transaction receipt, if available and needed.
    pub receipt: Option<TransactionReceipt>,
    // Potentially other correlated data points like internal calls, state diffs, etc.
}

impl CorrelatedBlockItem {
    /// Creates a new `CorrelatedBlockItem`.
    pub fn new(
        transaction: Transaction,
        logs: Vec<Log>,
        receipt: Option<TransactionReceipt>,
    ) -> Self {
        Self { transaction, logs, receipt }
    }

    /// Returns the hash of the transaction.
    pub fn tx_hash(&self) -> TxHash {
        self.transaction.hash()
    }
}
