//! EVM transaction data structures.

use alloy::{
    consensus::{Transaction as ConsensusTransaction, Typed2718 as _},
    network::AnyRpcTransaction,
    primitives::{Address, B256, Bytes, U256},
};
use serde::{Deserialize, Serialize};

/// A newtype wrapper around `alloy::network::AnyRpcTransaction` to create a
/// stable API boundary for the rest of the application. The `Any` variant
/// tolerates chain-specific transaction types (e.g. OP-stack deposits,
/// Orbit system transactions).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Transaction(pub AnyRpcTransaction);

impl Transaction {
    /// Returns the transaction hash.
    pub fn hash(&self) -> B256 {
        alloy::network::TransactionResponse::tx_hash(&self.0)
    }

    /// Returns the recipient address, or `None` if it is a contract creation.
    pub fn to(&self) -> Option<Address> {
        self.0.inner.to()
    }

    /// Returns the sender address.
    pub fn from(&self) -> Address {
        alloy::network::TransactionResponse::from(&self.0)
    }

    /// Returns the transaction input data.
    pub fn input(&self) -> &Bytes {
        self.0.inner.input()
    }

    /// Returns the value transferred in the transaction.
    pub fn value(&self) -> U256 {
        self.0.inner.value()
    }

    /// Returns the gas limit for the transaction.
    pub fn gas(&self) -> u64 {
        self.0.inner.gas_limit()
    }

    /// Returns the gas price for the transaction.
    pub fn gas_price(&self) -> Option<u128> {
        self.0.inner.gas_price()
    }

    /// Returns the transaction nonce.
    pub fn nonce(&self) -> u64 {
        self.0.inner.nonce()
    }

    /// Returns the hash of the block containing the transaction, or `None` if
    /// it's pending.
    pub fn block_hash(&self) -> Option<B256> {
        self.0.block_hash
    }

    /// Returns the number of the block containing the transaction, or `None` if
    /// it's pending.
    pub fn block_number(&self) -> Option<u64> {
        self.0.block_number
    }

    /// Returns the transaction's index position in the block, or `None` if it's
    /// pending.
    pub fn transaction_index(&self) -> Option<u64> {
        self.0.transaction_index
    }

    /// Returns the EIP-1559 max fee per gas, or `None` if it's a legacy
    /// transaction.
    pub fn max_fee_per_gas(&self) -> u128 {
        self.0.inner.max_fee_per_gas()
    }

    /// Returns the EIP-1559 max priority fee per gas, or `None` if it's a
    /// legacy transaction.
    pub fn max_priority_fee_per_gas(&self) -> Option<u128> {
        self.0.inner.max_priority_fee_per_gas()
    }

    /// Returns `true` if the transaction is a contract creation.
    pub fn is_contract_creation(&self) -> bool {
        self.0.inner.to().is_none()
    }

    /// Returns the chain ID for the transaction.
    pub fn chain_id(&self) -> Option<u64> {
        self.0.inner.chain_id()
    }

    /// Returns the transaction type byte (e.g. 0x00 legacy, 0x02 EIP-1559,
    /// 0x7e OP-stack deposit, 0x6a Orbit system transaction).
    pub fn transaction_type(&self) -> u8 {
        self.0.ty()
    }
}

/// The conversion from the alloy type to our custom type is a zero-cost move.
impl From<AnyRpcTransaction> for Transaction {
    fn from(tx: AnyRpcTransaction) -> Self {
        Self(tx)
    }
}
