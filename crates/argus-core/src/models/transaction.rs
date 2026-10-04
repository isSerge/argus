//! EVM transaction data structures.

use alloy::{
    consensus::{Transaction as ConsensusTransaction, TxType, Typed2718 as _},
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

    /// Returns the fee fields exposed by this transaction. Legacy transactions
    /// carry a single `gas_price`; EIP-1559 transactions carry max fees.
    /// Chain-specific types (OP-stack deposits, Orbit system transactions)
    /// carry no fee fields.
    pub fn fee_fields(&self) -> FeeFields {
        match self.transaction_type() {
            TX_TYPE_LEGACY => FeeFields::Legacy { gas_price: self.gas_price().map(U256::from) },
            TX_TYPE_EIP1559 => FeeFields::Eip1559 {
                max_fee_per_gas: U256::from(self.max_fee_per_gas()),
                max_priority_fee_per_gas: self.max_priority_fee_per_gas().map(U256::from),
            },
            _ => FeeFields::None,
        }
    }
}

const TX_TYPE_LEGACY: u8 = TxType::Legacy as u8;
const TX_TYPE_EIP1559: u8 = TxType::Eip1559 as u8;

/// Fee fields exposed by a transaction, determined by its type byte.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeeFields {
    /// A legacy (type 0x00) transaction with a single gas price.
    Legacy { gas_price: Option<U256> },
    /// An EIP-1559 (type 0x02) transaction with max fees.
    Eip1559 { max_fee_per_gas: U256, max_priority_fee_per_gas: Option<U256> },
    /// A chain-specific type (deposit, system transaction) with no fee fields.
    None,
}

/// The conversion from the alloy type to our custom type is a zero-cost move.
impl From<AnyRpcTransaction> for Transaction {
    fn from(tx: AnyRpcTransaction) -> Self {
        Self(tx)
    }
}

#[cfg(test)]
mod tests {
    use alloy::{consensus::TxType, primitives::U256};

    use super::*;
    use crate::models::transaction_builder::TransactionBuilder;

    #[test]
    fn fee_fields_by_transaction_type() {
        let legacy = TransactionBuilder::new()
            .gas_price(U256::from(150))
            .tx_type(TxType::Legacy)
            .build();
        assert_eq!(
            legacy.fee_fields(),
            FeeFields::Legacy { gas_price: Some(U256::from(150)) }
        );

        let eip1559 = TransactionBuilder::new().tx_type(TxType::Eip1559).build();
        assert!(matches!(
            eip1559.fee_fields(),
            FeeFields::Eip1559 { max_fee_per_gas: _, max_priority_fee_per_gas: Some(_) }
        ));
    }
}
