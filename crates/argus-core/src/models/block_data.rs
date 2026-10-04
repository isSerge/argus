//! This module defines the `BlockData` structure, which encapsulates all
//! relevant information for a single blockchain block.

use std::collections::HashMap;

use alloy::{network::AnyRpcBlock, primitives::TxHash, rpc::types::Block};

use crate::models::{Log, TransactionReceipt};

/// A comprehensive data structure holding all relevant information for a single
/// block.
#[derive(Debug, Clone)]
pub struct BlockData {
    /// The full block object, including headers and transaction details.
    pub block: AnyRpcBlock,
    /// A map of transaction hashes to their corresponding receipts.
    /// Each transaction hash is associated with a single receipt.
    pub receipts: HashMap<TxHash, TransactionReceipt>,
    /// A map of logs, grouped by their transaction hash.
    pub logs: HashMap<TxHash, Vec<Log>>,
}

impl Default for BlockData {
    fn default() -> Self {
        Self::new(Block::default().into(), HashMap::new(), HashMap::new())
    }
}

impl BlockData {
    /// Creates a new `BlockData` instance.
    pub fn new(
        block: AnyRpcBlock,
        receipts: HashMap<TxHash, TransactionReceipt>,
        logs: HashMap<TxHash, Vec<Log>>,
    ) -> Self {
        Self { block, receipts, logs }
    }

    /// Creates a new `BlockData` instance from raw, ungrouped logs.
    ///
    /// Arguments:
    /// - `block`: The full block object.
    /// - `receipts`: A map of transaction receipts keyed by their transaction
    ///   hash.
    /// - `raw_logs`: A vector of logs that may not be grouped by transaction
    ///   hash.
    ///
    /// Returns:
    /// - A `BlockData` instance with logs grouped by their transaction hash
    pub fn from_raw_data(
        block: AnyRpcBlock,
        receipts: HashMap<TxHash, TransactionReceipt>,
        raw_logs: Vec<Log>,
    ) -> Self {
        let mut logs: HashMap<TxHash, Vec<Log>> = HashMap::new();
        for log in raw_logs {
            if let Some(tx_hash) = log.transaction_hash() {
                logs.entry(tx_hash).or_default().push(log);
            }
        }

        Self { block, receipts, logs }
    }
}

#[cfg(test)]
mod tests {
    use alloy::{primitives::B256, rpc::types::Log as AlloyLog};

    use super::*;

    #[test]
    fn test_new_constructor() {
        let block: AnyRpcBlock = Block::default().into();
        let receipts = HashMap::new();
        let mut logs = HashMap::new();
        let tx_hash = B256::from_slice(&[1; 32]);
        logs.insert(tx_hash, vec![Log::default()]);

        let block_data = BlockData::new(block.clone(), receipts.clone(), logs.clone());

        assert_eq!(block_data.block.header.hash, block.header.hash);
        assert_eq!(block_data.receipts.len(), 0);
        assert_eq!(block_data.logs.len(), 1);
        assert_eq!(block_data.logs.get(&tx_hash).unwrap().len(), 1);
    }

    #[test]
    fn test_default_trait() {
        let block_data = BlockData::default();
        assert!(block_data.receipts.is_empty());
        assert!(block_data.logs.is_empty());
    }

    #[test]
    fn test_from_raw_data_ignores_logs_without_tx_hash() {
        let tx_hash = B256::from_slice(&[1; 32]);
        let log_with_hash = Log(AlloyLog { transaction_hash: Some(tx_hash), ..Default::default() });
        let log_without_hash = Log(AlloyLog { transaction_hash: None, ..Default::default() });

        let raw_logs = vec![log_with_hash, log_without_hash];
        let receipts = HashMap::new();

        let block_data = BlockData::from_raw_data(Block::default().into(), receipts, raw_logs);

        assert_eq!(block_data.logs.len(), 1);
        assert!(block_data.logs.contains_key(&tx_hash));
    }

    #[test]
    fn test_block_data_groups_logs_correctly() {
        let tx_hash1 = B256::from_slice(&[1; 32]);
        let tx_hash2 = B256::from_slice(&[2; 32]);

        let log1 = Log(AlloyLog { transaction_hash: Some(tx_hash1), ..Default::default() });
        let log2 = Log(AlloyLog { transaction_hash: Some(tx_hash2), ..Default::default() });
        let log3 = Log(AlloyLog { transaction_hash: Some(tx_hash1), ..Default::default() });

        let raw_logs = vec![log1, log2, log3];
        let receipts = HashMap::new();

        let block_data = BlockData::from_raw_data(Block::default().into(), receipts, raw_logs);

        assert_eq!(block_data.logs.len(), 2);
        assert_eq!(block_data.logs.get(&tx_hash1).unwrap().len(), 2);
        assert_eq!(block_data.logs.get(&tx_hash2).unwrap().len(), 1);
    }

    #[test]
    fn test_block_data_with_no_logs() {
        let raw_logs = vec![];
        let receipts = HashMap::new();

        let block_data = BlockData::from_raw_data(Block::default().into(), receipts, raw_logs);

        assert!(block_data.logs.is_empty());
    }
}
