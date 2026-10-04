//! ABI test helpers.

use std::sync::Arc;

use alloy::{
    consensus::TxType,
    json_abi::JsonAbi,
    network::AnyRpcTransaction as AlloyTransaction,
    primitives::{Address, B256, Bytes, LogData, U256},
    rpc::types::Log as AlloyLog,
};

use crate::AbiService;

const STANDARD_GAS_LIMIT: u64 = 21_000;

/// A simple ABI JSON for testing purposes.
pub fn erc20_abi_json() -> &'static str {
    include_str!("../../../../abis/erc20.json")
}

/// Creates a test `AbiService` with the given ABIs registered in memory.
pub fn create_test_abi_service(abis: &[(&str, &str)]) -> Arc<AbiService> {
    let abi_service = Arc::new(AbiService::new());

    for (name, content) in abis {
        let abi: JsonAbi = serde_json::from_str(content).unwrap();
        abi_service.register_abi((*name).to_string(), Arc::new(abi));
    }

    abi_service
}

/// A builder for creating Alloy `Log` instances for testing.
#[derive(Debug, Clone, Default)]
pub struct LogBuilder {
    address: Address,
    topics: Vec<B256>,
    data: Bytes,
    transaction_hash: Option<B256>,
    transaction_index: Option<u64>,
    block_hash: Option<B256>,
    block_number: Option<u64>,
    log_index: Option<u64>,
    removed: bool,
}

impl LogBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn address(mut self, address: Address) -> Self {
        self.address = address;
        self
    }

    pub fn topic(mut self, topic: B256) -> Self {
        self.topics.push(topic);
        self
    }

    pub fn topics(mut self, topics: Vec<B256>) -> Self {
        self.topics = topics;
        self
    }

    pub fn data(mut self, data: Bytes) -> Self {
        self.data = data;
        self
    }

    pub fn transaction_hash(mut self, hash: B256) -> Self {
        self.transaction_hash = Some(hash);
        self
    }

    pub fn block_number(mut self, number: u64) -> Self {
        self.block_number = Some(number);
        self
    }

    pub fn log_index(mut self, index: u64) -> Self {
        self.log_index = Some(index);
        self
    }

    pub fn transaction_index(mut self, index: u64) -> Self {
        self.transaction_index = Some(index);
        self
    }

    pub fn build(self) -> AlloyLog {
        AlloyLog {
            inner: alloy::primitives::Log {
                address: self.address,
                data: LogData::new_unchecked(self.topics, self.data),
            },
            transaction_hash: self.transaction_hash,
            transaction_index: self.transaction_index,
            block_hash: self.block_hash,
            block_number: self.block_number,
            log_index: self.log_index,
            removed: self.removed,
            block_timestamp: None,
        }
    }
}

/// A builder for creating Alloy `Transaction` instances for testing.
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
    gas_price: Option<U256>,
    chain_id: Option<u64>,
    tx_type: Option<TxType>,
}

impl TransactionBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn to(mut self, to: Option<Address>) -> Self {
        self.to = to;
        self
    }

    pub fn input(mut self, input: Bytes) -> Self {
        self.input = input;
        self
    }

    pub fn from(mut self, from: Address) -> Self {
        self.from = Some(from);
        self
    }

    pub fn value(mut self, value: U256) -> Self {
        self.value = Some(value);
        self
    }

    pub fn nonce(mut self, nonce: u64) -> Self {
        self.nonce = Some(nonce);
        self
    }

    pub fn gas_limit(mut self, gas_limit: u64) -> Self {
        self.gas_limit = Some(gas_limit);
        self
    }

    pub fn hash(mut self, hash: B256) -> Self {
        self.hash = Some(hash);
        self
    }

    pub fn block_hash(mut self, block_hash: B256) -> Self {
        self.block_hash = Some(block_hash);
        self
    }

    pub fn block_number(mut self, block_number: u64) -> Self {
        self.block_number = Some(block_number);
        self
    }

    pub fn transaction_index(mut self, transaction_index: u64) -> Self {
        self.transaction_index = Some(transaction_index);
        self
    }

    pub fn max_fee_per_gas(mut self, max_fee_per_gas: U256) -> Self {
        self.max_fee_per_gas = Some(max_fee_per_gas);
        self
    }

    pub fn max_priority_fee_per_gas(mut self, max_priority_fee_per_gas: U256) -> Self {
        self.max_priority_fee_per_gas = Some(max_priority_fee_per_gas);
        self
    }

    pub fn gas_price(mut self, gas_price: U256) -> Self {
        self.gas_price = Some(gas_price);
        self
    }

    pub fn chain_id(mut self, chain_id: u64) -> Self {
        self.chain_id = Some(chain_id);
        self
    }

    pub fn tx_type(mut self, tx_type: TxType) -> Self {
        self.tx_type = Some(tx_type);
        self
    }

    pub fn build(self) -> AlloyTransaction {
        let from = self.from.unwrap_or_default();
        let value = self.value.unwrap_or(U256::ZERO);
        self.build_from_parts(from, value)
    }

    pub fn build_from_parts(self, from: Address, value: U256) -> AlloyTransaction {
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

        if tx_type == TxType::Legacy {
            let gas_price = self.gas_price.unwrap_or(U256::from(1_000_000_000u64));
            tx_json["gasPrice"] = serde_json::json!(gas_price);
            tx_json["v"] = serde_json::json!("0x25");
        } else {
            tx_json["maxFeePerGas"] = serde_json::json!(max_fee_per_gas);
            tx_json["maxPriorityFeePerGas"] = serde_json::json!(max_priority_fee_per_gas);
            tx_json["accessList"] = serde_json::json!([]);
        }

        serde_json::from_value(tx_json).expect("Failed to create transaction from JSON")
    }
}
