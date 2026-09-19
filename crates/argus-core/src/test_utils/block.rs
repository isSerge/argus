use alloy::{
    primitives::{B256, Bloom},
    rpc::types::{Block, BlockTransactions, Header},
};

use crate::models::transaction::Transaction;

/// A builder for creating `Block` instances for testing.
#[derive(Debug, Clone, Default)]
pub struct BlockBuilder {
    header: Header,
    transactions: Vec<Transaction>,
}

impl BlockBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn number(mut self, number: u64) -> Self {
        self.header.number = number;
        self
    }

    pub fn hash(mut self, hash: B256) -> Self {
        self.header.hash = hash;
        self
    }

    pub fn parent_hash(mut self, parent_hash: B256) -> Self {
        self.header.parent_hash = parent_hash;
        self
    }

    pub fn transaction(mut self, tx: Transaction) -> Self {
        self.transactions.push(tx);
        self
    }

    pub fn bloom(mut self, bloom: Bloom) -> Self {
        self.header.logs_bloom = bloom;
        self
    }

    pub fn build(self) -> Block {
        let txs = self.transactions.into_iter().map(|tx| tx.0).collect();
        Block {
            header: self.header,
            transactions: BlockTransactions::Full(txs),
            uncles: Default::default(),
            withdrawals: Default::default(),
        }
    }
}
