//! A builder for creating `Transaction` instances.

use alloy::{
    consensus::TxType,
    network::AnyRpcTransaction,
    primitives::{Address, B256, Bytes, U256},
};

use super::transaction::Transaction;

const STANDARD_GAS_LIMIT: u64 = 21_000;

/// A builder for creating `Transaction` instances for testing.
#[derive(Debug, Clone, Default)]
pub struct TransactionBuilder {
    to: Option<Address>,
    input: Bytes,
    from: Option<Address>,
    value: Option<U256>,
    nonce: Option<u64>,
    gas_limit: Option<u64>,
    hash: Option<B256>,
    block_hash: Option<B256>,
    block_number: Option<u64>,
    transaction_index: Option<u64>,
    max_fee_per_gas: Option<U256>,
    max_priority_fee_per_gas: Option<U256>,
    gas_price: Option<U256>, // For legacy transactions
    chain_id: Option<u64>,
    tx_type: Option<TxType>,
}

impl TransactionBuilder {
    /// Creates a new `TransactionBuilder`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the `to` address for the transaction.
    pub fn to(mut self, to: Option<Address>) -> Self {
        self.to = to;
        self
    }

    /// Sets the `input` data for the transaction.
    pub fn input(mut self, input: Bytes) -> Self {
        self.input = input;
        self
    }

    /// Sets the `from` address for the transaction.
    pub fn from(mut self, from: Address) -> Self {
        self.from = Some(from);
        self
    }

    /// Sets the `value` for the transaction.
    pub fn value(mut self, value: U256) -> Self {
        self.value = Some(value);
        self
    }

    /// Sets the `nonce` for the transaction.
    pub fn nonce(mut self, nonce: u64) -> Self {
        self.nonce = Some(nonce);
        self
    }

    /// Sets the `gas_limit` for the transaction.
    pub fn gas_limit(mut self, gas_limit: u64) -> Self {
        self.gas_limit = Some(gas_limit);
        self
    }

    /// Sets the transaction `hash`.
    pub fn hash(mut self, hash: B256) -> Self {
        self.hash = Some(hash);
        self
    }

    /// Sets the `block_hash` where this transaction is included.
    pub fn block_hash(mut self, block_hash: B256) -> Self {
        self.block_hash = Some(block_hash);
        self
    }

    /// Sets the `block_number` where this transaction is included.
    pub fn block_number(mut self, block_number: u64) -> Self {
        self.block_number = Some(block_number);
        self
    }

    /// Sets the `transaction_index` within the block.
    pub fn transaction_index(mut self, transaction_index: u64) -> Self {
        self.transaction_index = Some(transaction_index);
        self
    }

    /// Sets the `max_fee_per_gas` for EIP-1559 transactions.
    pub fn max_fee_per_gas(mut self, max_fee_per_gas: U256) -> Self {
        self.max_fee_per_gas = Some(max_fee_per_gas);
        self
    }

    /// Sets the `max_priority_fee_per_gas` for EIP-1559 transactions.
    pub fn max_priority_fee_per_gas(mut self, max_priority_fee_per_gas: U256) -> Self {
        self.max_priority_fee_per_gas = Some(max_priority_fee_per_gas);
        self
    }

    /// Sets the `chain_id` for the transaction.
    pub fn chain_id(mut self, chain_id: u64) -> Self {
        self.chain_id = Some(chain_id);
        self
    }

    /// Sets the transaction `type` (0 = Legacy, 1 = EIP-2930, 2 = EIP-1559).
    pub fn tx_type(mut self, tx_type: TxType) -> Self {
        self.tx_type = Some(tx_type);
        self
    }

    /// Sets the `gas_price` for legacy transactions.
    pub fn gas_price(mut self, gas_price: U256) -> Self {
        self.gas_price = Some(gas_price);
        self
    }

    /// Builds the `Transaction` with default values.
    pub fn build(self) -> Transaction {
        let from = self.from.unwrap_or_default();
        let value = self.value.unwrap_or(U256::ZERO);
        self.build_from_parts(from, value)
    }

    /// Builds the `Transaction` with specified from address and value.
    pub fn build_from_parts(self, from: Address, value: U256) -> Transaction {
        // Use builder values or fallback to provided/default values
        let actual_from = self.from.unwrap_or(from);
        let actual_value = self.value.unwrap_or(value);
        let nonce = self.nonce.unwrap_or(0);
        let gas_limit = self.gas_limit.unwrap_or(STANDARD_GAS_LIMIT);
        let hash = self.hash.unwrap_or_else(|| B256::from([0x42; 32]));
        let block_hash = self.block_hash;
        let block_number = self.block_number;
        let transaction_index = self.transaction_index;
        let max_fee_per_gas = self.max_fee_per_gas.unwrap_or(U256::from(2_000_000_000u64));
        let max_priority_fee_per_gas =
            self.max_priority_fee_per_gas.unwrap_or(U256::from(1_000_000_000u64));
        let chain_id = self.chain_id.unwrap_or(1);
        let tx_type = self.tx_type.unwrap_or(TxType::Eip1559);

        let mut tx_json = serde_json::json!({
            "hash": hash,
            "nonce": nonce,
            "blockHash": block_hash,
            "blockNumber": block_number,
            "transactionIndex": transaction_index,
            "from": actual_from,
            "to": self.to,
            "value": actual_value,
            "gas": gas_limit,
            "input": self.input,
            "chainId": chain_id,
            "type": tx_type,
            "r": "0x1b41f7bcd8c7c8d35d9f4d3a1f9c8e7b6a5d9c8e7f1a2b3c4d5e6f7a8b9c0d1",
            "s": "0x2c52f8cdd9d8d46e8a0e5d4b2f0d9f8c7b6e0d9f8a2b3d4e5f6a8b9c0d1f2a3",
            "v": "0x1"
        });

        // Build different JSON based on transaction type
        if tx_type == TxType::Legacy {
            let gas_price = self.gas_price.unwrap_or(U256::from(1_000_000_000u64));
            tx_json["gasPrice"] = serde_json::json!(gas_price);
            tx_json["v"] = serde_json::json!("0x25");
        } else {
            tx_json["maxFeePerGas"] = serde_json::json!(max_fee_per_gas);
            tx_json["maxPriorityFeePerGas"] = serde_json::json!(max_priority_fee_per_gas);
            tx_json["accessList"] = serde_json::json!([]);
        }

        let alloy_tx: AnyRpcTransaction =
            serde_json::from_value(tx_json).expect("Failed to create transaction from JSON");

        Transaction(alloy_tx)
    }
}

#[cfg(test)]
mod tests {
    use alloy::{
        consensus::TxType,
        primitives::{address, b256},
    };

    use super::*;

    #[test]
    fn test_transaction_builder() {
        let contract_addr = address!("1234567890123456789012345678901234567890");
        let from_addr = address!("abcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let input_data = Bytes::from(vec![0xa9, 0x05, 0x9c, 0xbb]); // transfer function selector

        let tx = TransactionBuilder::new()
            .to(Some(contract_addr))
            .from(from_addr)
            .input(input_data.clone())
            .value(U256::from(1000))
            .nonce(42)
            .gas_limit(50000)
            .build();

        // Test that the transaction was built correctly
        assert_eq!(tx.to(), Some(contract_addr));
        assert_eq!(tx.from(), from_addr);
        assert_eq!(tx.input(), &input_data);
        assert_eq!(tx.value(), U256::from(1000));
        assert_eq!(tx.nonce(), 42);
        assert_eq!(tx.gas(), 50000);
    }

    #[test]
    fn test_transaction_builder_minimal() {
        let tx = TransactionBuilder::new().build();

        // Test defaults
        assert_eq!(tx.to(), None); // Contract creation
        assert_eq!(tx.from(), Address::default());
        assert_eq!(tx.value(), U256::ZERO);
        assert_eq!(tx.nonce(), 0);
        assert_eq!(tx.gas(), STANDARD_GAS_LIMIT);
        assert!(tx.input().is_empty());
    }

    #[test]
    fn test_transaction_builder_with_all_fields() {
        let contract_addr = address!("1234567890123456789012345678901234567890");
        let from_addr = address!("abcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let input_data = Bytes::from(vec![0xa9, 0x05, 0x9c, 0xbb]);
        let tx_hash = b256!("1111111111111111111111111111111111111111111111111111111111111111");
        let block_hash = b256!("2222222222222222222222222222222222222222222222222222222222222222");

        let tx = TransactionBuilder::new()
            .to(Some(contract_addr))
            .from(from_addr)
            .input(input_data.clone())
            .value(U256::from(5000))
            .nonce(123)
            .gas_limit(100000)
            .hash(tx_hash)
            .block_hash(block_hash)
            .block_number(500)
            .transaction_index(5)
            .max_fee_per_gas(U256::from(2_000_000_000u64))
            .max_priority_fee_per_gas(U256::from(1_500_000_000u64))
            .chain_id(137) // Polygon
            .tx_type(TxType::Eip1559)
            .build();

        // Test all the custom fields
        assert_eq!(tx.to(), Some(contract_addr));
        assert_eq!(tx.from(), from_addr);
        assert_eq!(tx.input(), &input_data);
        assert_eq!(tx.value(), U256::from(5000));
        assert_eq!(tx.nonce(), 123);
        assert_eq!(tx.gas(), 100000);
        assert_eq!(tx.hash(), tx_hash);
        assert_eq!(tx.block_hash(), Some(block_hash));
        assert_eq!(tx.block_number(), Some(500));
        assert_eq!(tx.transaction_index(), Some(5));
        assert_eq!(tx.max_fee_per_gas(), 2_000_000_000);
        assert_eq!(tx.max_priority_fee_per_gas(), Some(1_500_000_000));
        assert_eq!(tx.chain_id(), Some(137));
        assert_eq!(tx.transaction_type(), TxType::Eip1559 as u8);
    }

    #[test]
    fn test_transaction_builder_legacy_transaction() {
        let contract_addr = address!("1234567890123456789012345678901234567890");
        let from_addr = address!("abcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let input_data = Bytes::from(vec![0xa9, 0x05, 0x9c, 0xbb]);

        let tx = TransactionBuilder::new()
            .to(Some(contract_addr))
            .from(from_addr)
            .input(input_data.clone())
            .value(U256::from(1000))
            .nonce(10)
            .gas_limit(30000)
            .gas_price(U256::from(20_000_000_000u64)) // 20 Gwei
            .chain_id(1) // Mainnet
            .tx_type(TxType::Legacy)
            .build();

        // Test legacy transaction fields
        assert_eq!(tx.to(), Some(contract_addr));
        assert_eq!(tx.from(), from_addr);
        assert_eq!(tx.input(), &input_data);
        assert_eq!(tx.value(), U256::from(1000));
        assert_eq!(tx.nonce(), 10);
        assert_eq!(tx.gas(), 30000);
        assert_eq!(tx.gas_price(), Some(20_000_000_000));
        assert_eq!(tx.chain_id(), Some(1));
        assert_eq!(tx.transaction_type(), TxType::Legacy as u8);

        // Legacy transactions should not have EIP-1559 fields
        // For legacy transactions, max_fee_per_gas returns the gas_price
        assert_eq!(tx.max_fee_per_gas(), 20_000_000_000);
        assert_eq!(tx.max_priority_fee_per_gas(), None);
    }

    #[test]
    fn test_transaction_builder_contract_creation() {
        let from_addr = address!("abcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let bytecode = Bytes::from(vec![0x60, 0x80, 0x60, 0x40, 0x52]); // Contract creation bytecode

        let tx = TransactionBuilder::new()
            .from(from_addr)
            .input(bytecode.clone())
            .value(U256::ZERO)
            .nonce(0)
            .gas_limit(500000)
            .max_fee_per_gas(U256::from(3_000_000_000u64))
            .max_priority_fee_per_gas(U256::from(2_000_000_000u64))
            .chain_id(1)
            .tx_type(TxType::Eip1559)
            .build();

        // Test contract creation (to field should be None)
        assert_eq!(tx.to(), None);
        assert_eq!(tx.from(), from_addr);
        assert_eq!(tx.input(), &bytecode);
        assert_eq!(tx.value(), U256::ZERO);
        assert_eq!(tx.nonce(), 0);
        assert_eq!(tx.gas(), 500000);
    }
}
