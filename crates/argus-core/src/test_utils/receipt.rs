use alloy::{
    consensus::{Eip658Value, Receipt, ReceiptWithBloom},
    network::{AnyReceiptEnvelope, AnyTransactionReceipt},
    primitives::{Address, B256, Bloom},
    rpc::types::{TransactionReceipt, WithOtherFields},
};

use crate::models::TransactionReceipt as ArgusReceipt;

/// A builder for creating `TransactionReceipt` instances for testing.
#[derive(Debug, Default, Clone)]
pub struct ReceiptBuilder {
    transaction_hash: Option<B256>,
    block_number: Option<u64>,
    gas_used: Option<u64>,
    effective_gas_price: Option<u128>,
    status: Option<bool>,
}

impl ReceiptBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn transaction_hash(mut self, hash: B256) -> Self {
        self.transaction_hash = Some(hash);
        self
    }

    pub fn block_number(mut self, number: u64) -> Self {
        self.block_number = Some(number);
        self
    }

    pub fn gas_used(mut self, gas: u64) -> Self {
        self.gas_used = Some(gas);
        self
    }

    pub fn effective_gas_price(mut self, price: u128) -> Self {
        self.effective_gas_price = Some(price);
        self
    }

    pub fn status(mut self, success: bool) -> Self {
        self.status = Some(success);
        self
    }

    pub fn build(self) -> ArgusReceipt {
        let status = if self.status.unwrap_or(true) {
            Eip658Value::Eip658(true)
        } else {
            Eip658Value::Eip658(false)
        };

        let inner_receipt =
            Receipt { status, cumulative_gas_used: self.gas_used.unwrap_or(21_000), logs: vec![] };

        let receipt_with_bloom =
            ReceiptWithBloom { receipt: inner_receipt, logs_bloom: Bloom::default() };

        let receipt = TransactionReceipt {
            transaction_hash: self.transaction_hash.unwrap_or_default(),
            block_number: self.block_number,
            transaction_index: Some(1),
            block_hash: Some(B256::default()),
            from: Address::default(),
            to: Some(Address::default()),
            gas_used: self.gas_used.unwrap_or(21_000),
            contract_address: None,
            effective_gas_price: self.effective_gas_price.unwrap_or(1_000_000_000),
            blob_gas_used: None,
            blob_gas_price: None,
            inner: AnyReceiptEnvelope { inner: receipt_with_bloom, r#type: 0x02 },
        };
        ArgusReceipt(WithOtherFields::new(receipt))
    }
}
