//! Data conversion utilities for Rhai scripting engine.
//!
//! This module handles conversion between blockchain data types and
//! Rhai-compatible types

use std::collections::HashSet;

use alloy::{dyn_abi::DynSolValue, primitives::U256};
use argus_abi::{DecodedCall, DecodedLog};
use argus_core::models::{FeeFields, Transaction, TransactionReceipt};
use rhai::{Dynamic, Map};
use rhai_evm::{i256_to_bigint_dynamic, u256_to_bigint_dynamic};
use serde_json::{Value, json};

/// Converts a `DynSolValue` directly to a Rhai `Dynamic` value.
///
/// `uint256` and `int256` types are always converted to `BigInt` to ensure
/// type consistency within Rhai scripts, preventing comparison errors.
pub fn dyn_sol_value_to_rhai(value: &DynSolValue) -> Dynamic {
    match value {
        DynSolValue::Address(a) => a.to_checksum(None).into(),
        DynSolValue::Bool(b) => (*b).into(),
        DynSolValue::Bytes(b) => format!("0x{}", hex::encode(b)).into(),
        DynSolValue::FixedBytes(fb, _) => format!("0x{}", hex::encode(fb)).into(),

        // Always convert large integer types from ABI to BigInt for consistency.
        DynSolValue::Int(i, _) => i256_to_bigint_dynamic(*i),
        DynSolValue::Uint(u, _) => u256_to_bigint_dynamic(*u),

        DynSolValue::String(s) => s.clone().into(),

        // Handle arrays and tuples
        DynSolValue::Array(arr) | DynSolValue::FixedArray(arr) | DynSolValue::Tuple(arr) => {
            let rhai_array: Vec<Dynamic> = arr.iter().map(dyn_sol_value_to_rhai).collect();
            rhai_array.into()
        }

        _ => Dynamic::UNIT,
    }
}

/// Converts a `DynSolValue` directly to a `serde_json::Value`.
///
/// Large numbers (>i64/u64 range) are converted to strings to preserve
/// precision.
pub fn dyn_sol_value_to_json(value: &DynSolValue) -> Value {
    match value {
        DynSolValue::Address(a) => Value::String(a.to_checksum(None)),
        DynSolValue::Bool(b) => Value::Bool(*b),
        DynSolValue::Bytes(b) => Value::String(format!("0x{}", hex::encode(b))),
        DynSolValue::FixedBytes(fb, _) => Value::String(format!("0x{}", hex::encode(fb))),

        // Handle large signed integers
        DynSolValue::Int(i, _) => {
            // Try to fit in i64, otherwise use string
            match i.to_string().parse::<i64>() {
                Ok(val) => Value::Number(serde_json::Number::from(val)),
                Err(_) => Value::String(i.to_string()),
            }
        }

        DynSolValue::String(s) => Value::String(s.clone()),

        // Handle large unsigned integers
        DynSolValue::Uint(u, _) => {
            // Try to fit in i64, otherwise use string
            if *u <= U256::from(i64::MAX as u64) {
                Value::Number(serde_json::Number::from(u.to::<u64>() as i64))
            } else {
                Value::String(u.to_string())
            }
        }

        // Handle arrays and tuples
        DynSolValue::Array(arr) | DynSolValue::FixedArray(arr) | DynSolValue::Tuple(arr) => {
            let json_array: Vec<Value> = arr.iter().map(dyn_sol_value_to_json).collect();
            Value::Array(json_array)
        }

        _ => Value::Null,
    }
}

/// Builds a Rhai `Map` from log and call parameters using direct conversion.
pub fn build_params_map(params: &[(String, DynSolValue)]) -> Map {
    let mut map = Map::new();
    for (name, value) in params {
        map.insert(name.clone().into(), dyn_sol_value_to_rhai(value));
    }
    map
}

/// --- Transaction Fields ---
const KEY_TX_TO: &str = "to";
const KEY_TX_FROM: &str = "from";
const KEY_TX_HASH: &str = "hash";
const KEY_TX_VALUE: &str = "value";
const KEY_TX_GAS_LIMIT: &str = "gas_limit";
const KEY_TX_NONCE: &str = "nonce";
const KEY_TX_INPUT: &str = "input";
const KEY_TX_BLOCK_NUMBER: &str = "block_number";
const KEY_TX_TRANSACTION_INDEX: &str = "transaction_index";
const KEY_TX_GAS_PRICE: &str = "gas_price";
const KEY_TX_MAX_FEE_PER_GAS: &str = "max_fee_per_gas";
const KEY_TX_MAX_PRIORITY_FEE_PER_GAS: &str = "max_priority_fee_per_gas";
/// --- Receipt Fields ---
const KEY_TX_GAS_USED: &str = "gas_used";
const KEY_TX_STATUS: &str = "status";
const KEY_TX_EFFECTIVE_GAS_PRICE: &str = "effective_gas_price";

/// Returns a set of valid Rhai paths for transaction fields.
/// Used for validating Rhai scripts.
/// This includes all standard transaction fields, prefixed with "tx."
/// Does not include receipt fields (see separate method below)
pub fn get_valid_tx_rhai_paths() -> HashSet<String> {
    [
        KEY_TX_FROM,
        KEY_TX_TO,
        KEY_TX_HASH,
        KEY_TX_VALUE,
        KEY_TX_GAS_LIMIT,
        KEY_TX_NONCE,
        KEY_TX_INPUT,
        KEY_TX_BLOCK_NUMBER,
        KEY_TX_TRANSACTION_INDEX,
        // Legacy fields
        KEY_TX_GAS_PRICE,
        // EIP-1559 fields
        KEY_TX_MAX_FEE_PER_GAS,
        KEY_TX_MAX_PRIORITY_FEE_PER_GAS,
    ]
    .iter()
    .map(|s| format!("tx.{}", s))
    .collect()
}

/// Returns a set of valid Rhai paths for receipt fields.
pub fn get_valid_receipt_rhai_paths() -> HashSet<String> {
    [KEY_TX_GAS_USED, KEY_TX_STATUS, KEY_TX_EFFECTIVE_GAS_PRICE]
        .iter()
        .map(|s| format!("tx.{}", s))
        .collect()
}

/// Builds a Rhai `Map` from a transaction and optional receipt.
pub fn build_transaction_map(
    transaction: &Transaction,
    receipt: Option<&TransactionReceipt>,
) -> Map {
    let mut map = Map::new();

    if let Some(to) = transaction.to() {
        map.insert(KEY_TX_TO.into(), to.to_checksum(None).into());
    }

    map.insert(KEY_TX_FROM.into(), transaction.from().to_checksum(None).into());
    map.insert(KEY_TX_HASH.into(), transaction.hash().to_string().into());

    // --- Selective Conversion ---
    // Values that can exceed i64::MAX are always converted to BigInt.
    map.insert(KEY_TX_VALUE.into(), u256_to_bigint_dynamic(transaction.value()));

    // Values that are bounded (typically u64) are converted to i64.
    map.insert(KEY_TX_GAS_LIMIT.into(), (transaction.gas() as i64).into());
    map.insert(KEY_TX_NONCE.into(), (transaction.nonce() as i64).into());
    map.insert(KEY_TX_INPUT.into(), format!("0x{}", hex::encode(transaction.input())).into());

    if let Some(block_number) = transaction.block_number() {
        map.insert(KEY_TX_BLOCK_NUMBER.into(), (block_number as i64).into());
    } else {
        map.insert(KEY_TX_BLOCK_NUMBER.into(), Dynamic::UNIT);
    }

    if let Some(transaction_index) = transaction.transaction_index() {
        map.insert(KEY_TX_TRANSACTION_INDEX.into(), (transaction_index as i64).into());
    } else {
        map.insert(KEY_TX_TRANSACTION_INDEX.into(), Dynamic::UNIT);
    }

    match transaction.fee_fields() {
        FeeFields::Legacy { gas_price } => {
            let value = gas_price.map(u256_to_bigint_dynamic).unwrap_or(Dynamic::UNIT);
            map.insert(KEY_TX_GAS_PRICE.into(), value);
        }
        FeeFields::Eip1559 { max_fee_per_gas, max_priority_fee_per_gas } => {
            map.insert(KEY_TX_MAX_FEE_PER_GAS.into(), u256_to_bigint_dynamic(max_fee_per_gas));
            let value = max_priority_fee_per_gas
                .map(u256_to_bigint_dynamic)
                .unwrap_or(Dynamic::UNIT);
            map.insert(KEY_TX_MAX_PRIORITY_FEE_PER_GAS.into(), value);
        }
        FeeFields::None => {}
    }

    // Add receipt fields if available
    if let Some(receipt) = receipt {
        map.insert(KEY_TX_GAS_USED.into(), (receipt.gas_used as i64).into());
        // status is 0 or 1, fits in i64.
        map.insert(KEY_TX_STATUS.into(), (receipt.inner.inner.is_success() as i64).into());
        // effective_gas_price can be large.
        map.insert(
            KEY_TX_EFFECTIVE_GAS_PRICE.into(),
            u256_to_bigint_dynamic(U256::from(receipt.effective_gas_price)),
        );
    } else {
        // When no receipt is available, set receipt fields to UNIT (null)
        map.insert(KEY_TX_GAS_USED.into(), Dynamic::UNIT);
        map.insert(KEY_TX_STATUS.into(), Dynamic::UNIT);
        map.insert(KEY_TX_EFFECTIVE_GAS_PRICE.into(), Dynamic::UNIT);
    }

    map
}

/// --- Log Fields (static) ---
const KEY_LOG_NAME: &str = "name";
const KEY_LOG_PARAMS: &str = "params";
const KEY_LOG_ADDRESS: &str = "address";
const KEY_LOG_INDEX: &str = "log_index";
const KEY_LOG_BLOCK_NUMBER: &str = "block_number";
const KEY_LOG_TRANSACTION_HASH: &str = "transaction_hash";
const KEY_LOG_TRANSACTION_INDEX: &str = "transaction_index";

/// Returns a set of valid Rhai paths for log fields.
/// Used for validating Rhai scripts.
/// This includes all standard log fields, prefixed with "log.", does not
/// include any dynamic fields.
pub fn get_valid_log_rhai_paths() -> HashSet<String> {
    [
        KEY_LOG_NAME,
        KEY_LOG_ADDRESS,
        KEY_LOG_INDEX,
        KEY_LOG_BLOCK_NUMBER,
        KEY_LOG_TRANSACTION_HASH,
        KEY_LOG_TRANSACTION_INDEX,
    ]
    .iter()
    .map(|s| format!("log.{}", s))
    .collect()
}

/// Builds a Rhai `Map` from a decoded log.
pub fn build_log_map(log: &DecodedLog, params_map: Map) -> Map {
    let mut log_map = Map::new();
    log_map.insert(KEY_LOG_NAME.into(), log.name.clone().into());
    log_map.insert(KEY_LOG_PARAMS.into(), params_map.into());
    log_map.insert(KEY_LOG_ADDRESS.into(), log.log.address().to_checksum(None).into());

    // Bounded u64 values become i64
    if let Some(block_number) = log.log.block_number {
        log_map.insert(KEY_LOG_BLOCK_NUMBER.into(), (block_number as i64).into());
    } else {
        log_map.insert(KEY_LOG_BLOCK_NUMBER.into(), Dynamic::UNIT);
    }
    log_map.insert(
        KEY_LOG_TRANSACTION_HASH.into(),
        log.log.transaction_hash.unwrap_or_default().to_string().into(),
    );
    if let Some(log_index) = log.log.log_index {
        log_map.insert(KEY_LOG_INDEX.into(), (log_index as i64).into());
    } else {
        log_map.insert(KEY_LOG_INDEX.into(), Dynamic::UNIT);
    }
    if let Some(transaction_index) = log.log.transaction_index {
        log_map.insert(KEY_LOG_TRANSACTION_INDEX.into(), (transaction_index as i64).into());
    } else {
        log_map.insert(KEY_LOG_TRANSACTION_INDEX.into(), Dynamic::UNIT);
    }

    log_map
}

// --- Decoded Call Static Fields ---
const KEY_CALL_NAME: &str = "name";
const KEY_CALL_PARAMS: &str = "params";

/// Returns a set of valid Rhai paths for decoded call fields.
/// Used for validating Rhai scripts.
/// This includes all standard decoded call fields, prefixed with
/// "decoded_call.", does not include any dynamic fields.
pub fn get_valid_decoded_call_rhai_paths() -> HashSet<String> {
    let mut set: HashSet<String> =
        [KEY_CALL_NAME, KEY_CALL_PARAMS].iter().map(|s| format!("decoded_call.{}", s)).collect();

    set.insert("decoded_call".to_string());

    set
}

/// Builds payload data JSON from log parameters using the same conversion logic
/// as Rhai.
pub fn build_log_params_payload(params: &[(String, DynSolValue)]) -> Value {
    let mut json_map = serde_json::Map::new();
    for (name, value) in params {
        json_map.insert(name.clone(), dyn_sol_value_to_json(value));
    }
    Value::Object(json_map)
}

/// Builds payload data JSON from a transaction, ensuring consistency with Rhai
/// script data.
pub fn build_transaction_details_payload(
    transaction: &Transaction,
    receipt: Option<&TransactionReceipt>,
) -> Value {
    let mut map = serde_json::Map::new();

    if let Some(to) = transaction.to() {
        map.insert(KEY_TX_TO.to_string(), json!(to.to_checksum(None)));
    }
    map.insert(KEY_TX_FROM.to_string(), json!(transaction.from().to_checksum(None)));
    map.insert(KEY_TX_HASH.to_string(), json!(transaction.hash().to_string()));

    // Potentially large values are stringified to prevent overflow
    map.insert(KEY_TX_VALUE.to_string(), json!(transaction.value().to_string()));

    // Bounded values are kept as JSON numbers
    map.insert(KEY_TX_GAS_LIMIT.to_string(), json!(transaction.gas()));
    map.insert(KEY_TX_NONCE.to_string(), json!(transaction.nonce()));
    map.insert(KEY_TX_INPUT.to_string(), json!(format!("0x{}", hex::encode(transaction.input()))));

    if let Some(transaction_index) = transaction.transaction_index() {
        map.insert(KEY_TX_TRANSACTION_INDEX.to_string(), json!(transaction_index));
    }

    match transaction.fee_fields() {
        FeeFields::Legacy { gas_price } => {
            if let Some(gas_price) = gas_price {
                map.insert(KEY_TX_GAS_PRICE.to_string(), json!(gas_price.to_string()));
            }
        }
        FeeFields::Eip1559 { max_fee_per_gas, max_priority_fee_per_gas } => {
            map.insert(KEY_TX_MAX_FEE_PER_GAS.to_string(), json!(max_fee_per_gas.to_string()));
            if let Some(max_priority_fee_per_gas) = max_priority_fee_per_gas {
                map.insert(
                    KEY_TX_MAX_PRIORITY_FEE_PER_GAS.to_string(),
                    json!(max_priority_fee_per_gas.to_string()),
                );
            }
        }
        FeeFields::None => {}
    }

    if let Some(receipt) = receipt {
        // gas_used and effective_gas_price are u128 and must be stringified
        map.insert(KEY_TX_GAS_USED.to_string(), json!(receipt.gas_used.to_string()));
        map.insert(KEY_TX_STATUS.to_string(), json!(receipt.inner.inner.is_success() as u64));
        map.insert(
            KEY_TX_EFFECTIVE_GAS_PRICE.to_string(),
            json!(receipt.effective_gas_price.to_string()),
        );
    }

    Value::Object(map)
}

/// Builds a Rhai `Map` for a decoded call with name and parameters.
pub fn build_decoded_call_map(call: &DecodedCall) -> Map {
    let mut call_map = Map::new();
    call_map.insert(KEY_CALL_NAME.into(), call.name.clone().into());
    let params_map = build_params_map(&call.params);
    call_map.insert(KEY_CALL_PARAMS.into(), params_map.into());
    call_map
}

#[cfg(test)]
mod tests {
    use alloy::{
        consensus::TxType,
        dyn_abi::Word,
        primitives::{Address, Function, I256, U256, address, b256},
    };
    use argus_abi::test_utils::LogBuilder;
    use argus_core::test_utils::{ReceiptBuilder, TransactionBuilder};
    use num_bigint::BigInt;
    use serde_json::json;

    use super::*;

    // --- Tests for dyn_sol_value_to_rhai ---

    #[test]
    fn test_dyn_sol_value_to_rhai_primitives() {
        // Address
        let addr = Address::repeat_byte(0x11);
        let result = dyn_sol_value_to_rhai(&DynSolValue::Address(addr));
        assert_eq!(result.cast::<String>(), "0x1111111111111111111111111111111111111111");

        // Bool
        let result = dyn_sol_value_to_rhai(&DynSolValue::Bool(true));
        assert!(result.is::<bool>());
        assert!(result.cast::<bool>());

        // String
        let result = dyn_sol_value_to_rhai(&DynSolValue::String("hello".to_string()));
        assert_eq!(result.cast::<String>(), "hello");
    }

    #[test]
    fn test_dyn_sol_value_to_rhai_numeric_types() {
        // Small Uint becomes BigInt
        let result = dyn_sol_value_to_rhai(&DynSolValue::Uint(U256::from(123), 256));
        assert!(result.is::<BigInt>());
        assert_eq!(result.cast::<BigInt>(), BigInt::from(123));

        // Large Uint becomes BigInt
        let large_uint = U256::MAX;
        let result = dyn_sol_value_to_rhai(&DynSolValue::Uint(large_uint, 256));
        assert!(result.is::<BigInt>());
        assert_eq!(result.cast::<BigInt>().to_string(), large_uint.to_string());

        // Negative Int becomes BigInt
        let result = dyn_sol_value_to_rhai(&DynSolValue::Int(I256::try_from(-123).unwrap(), 256));
        assert!(result.is::<BigInt>());
        assert_eq!(result.cast::<BigInt>(), BigInt::from(-123));
    }

    #[test]
    fn test_dyn_sol_value_to_rhai_structured_types() {
        // Bytes
        let bytes = [0x01; 24];
        let result = dyn_sol_value_to_rhai(&DynSolValue::Bytes(bytes.into()));
        assert_eq!(result.cast::<String>(), "0x010101010101010101010101010101010101010101010101");

        // FixedBytes
        let mut fixed_bytes_array = [0u8; 32];
        fixed_bytes_array[0..3].copy_from_slice(&[0x04, 0x05, 0x06]);
        let fixed_bytes = Word::new(fixed_bytes_array);
        let result = dyn_sol_value_to_rhai(&DynSolValue::FixedBytes(fixed_bytes, 3));
        assert_eq!(
            result.cast::<String>(),
            "0x0405060000000000000000000000000000000000000000000000000000000000"
        );

        // Tuple (becomes an Array in Rhai)
        let tuple_values = vec![DynSolValue::Uint(U256::from(42), 256), DynSolValue::Bool(true)];
        let result = dyn_sol_value_to_rhai(&DynSolValue::Tuple(tuple_values));
        let rhai_array = result.cast::<rhai::Array>();
        assert_eq!(rhai_array.len(), 2);
        assert_eq!(rhai_array[0].clone().cast::<BigInt>(), BigInt::from(42));
        assert!(rhai_array[1].clone().cast::<bool>());
    }

    #[test]
    fn test_dyn_sol_value_to_rhai_unhandled_variant() {
        // Function is not handled, should result in UNIT
        let bytes = [0x01; 24];
        let function = Function::new(bytes);
        let result = dyn_sol_value_to_rhai(&DynSolValue::Function(function));
        assert!(result.is_unit());
    }

    // --- Tests for dyn_sol_value_to_json ---

    #[test]
    fn test_dyn_sol_value_to_json_numeric_types() {
        // Small Uint becomes JSON Number
        let result = dyn_sol_value_to_json(&DynSolValue::Uint(U256::from(123), 256));
        assert_eq!(result, json!(123));

        // Large Uint becomes JSON String
        let large_uint = U256::from(i64::MAX as u64) + U256::from(1);
        let result = dyn_sol_value_to_json(&DynSolValue::Uint(large_uint, 256));
        assert_eq!(result, json!(large_uint.to_string()));

        // Negative Int becomes JSON Number
        let result = dyn_sol_value_to_json(&DynSolValue::Int(I256::try_from(-123).unwrap(), 256));
        assert_eq!(result, json!(-123));

        // Large Int becomes JSON String
        let large_int = I256::try_from(i64::MIN as i128 - 1).unwrap();
        let result = dyn_sol_value_to_json(&DynSolValue::Int(large_int, 256));
        assert_eq!(result, json!(large_int.to_string()));
    }

    #[test]
    fn test_dyn_sol_value_to_json_unhandled_variant() {
        let bytes = [0x01; 24];
        let function = Function::new(bytes);
        let result = dyn_sol_value_to_json(&DynSolValue::Function(function));
        assert_eq!(result, json!(null));
    }

    #[test]
    fn test_build_params_map() {
        let params = vec![
            ("value".to_string(), DynSolValue::Uint(U256::from(150), 256)),
            (
                "sender".to_string(),
                DynSolValue::Address(address!("1111111111111111111111111111111111111111")),
            ),
        ];

        let result = build_params_map(&params);

        assert_eq!(result.len(), 2);
        assert_eq!(result.get("value").unwrap().clone().cast::<BigInt>(), BigInt::from(150));
        assert_eq!(
            result.get("sender").unwrap().clone().cast::<String>(),
            "0x1111111111111111111111111111111111111111"
        );
    }

    #[test]
    fn test_build_decoded_call_map() {
        let to_address = address!("2222222222222222222222222222222222222222");
        let call = DecodedCall {
            name: "transfer".to_string(),
            params: vec![
                ("to".to_string(), DynSolValue::Address(to_address)),
                ("amount".to_string(), DynSolValue::Uint(U256::from(500), 256)),
            ],
        };
        let result = build_decoded_call_map(&call);

        assert_eq!(result.len(), 2, "Expected 2 fields in decoded call map");
        assert_eq!(result.get("name").unwrap().clone().cast::<String>(), "transfer");
        let params_map = result.get("params").unwrap().clone().cast::<Map>();
        assert_eq!(params_map.len(), 2, "Expected 2 parameters in params map: to and amount");
        assert_eq!(
            params_map.get("to").unwrap().clone().cast::<String>(),
            "0x2222222222222222222222222222222222222222"
        );
        assert_eq!(params_map.get("amount").unwrap().clone().cast::<BigInt>(), BigInt::from(500));
    }

    #[test]
    fn test_u256_to_bigint_dynamic() {
        // Small value
        assert_eq!(u256_to_bigint_dynamic(U256::from(123)).cast::<BigInt>(), BigInt::from(123));

        // At boundary
        assert_eq!(
            u256_to_bigint_dynamic(U256::from(i64::MAX as u64)).cast::<BigInt>(),
            BigInt::from(i64::MAX)
        );

        // Beyond boundary
        let large = U256::from(i64::MAX as u64) + U256::from(1);
        let bigint_val = u256_to_bigint_dynamic(large).cast::<BigInt>();
        assert_eq!(bigint_val.to_string(), large.to_string());
    }

    #[test]
    fn test_dyn_sol_value_to_rhai_signed_integers() {
        // Small Int (should become BigInt)
        let result = dyn_sol_value_to_rhai(&DynSolValue::Int(I256::try_from(123).unwrap(), 256));
        assert_eq!(result.cast::<BigInt>(), BigInt::from(123));

        let result = dyn_sol_value_to_rhai(&DynSolValue::Int(I256::try_from(-123).unwrap(), 256));
        assert_eq!(result.cast::<BigInt>(), BigInt::from(-123));

        // Int at i64::MAX boundary (should become BigInt)
        let result =
            dyn_sol_value_to_rhai(&DynSolValue::Int(I256::try_from(i64::MAX).unwrap(), 256));
        assert_eq!(result.cast::<BigInt>(), BigInt::from(i64::MAX));

        // Int at i64::MIN boundary (should become BigInt)
        let result =
            dyn_sol_value_to_rhai(&DynSolValue::Int(I256::try_from(i64::MIN).unwrap(), 256));
        assert_eq!(result.cast::<BigInt>(), BigInt::from(i64::MIN));

        // Int just beyond i64::MAX (should become BigInt)
        let beyond_i64_max = I256::try_from(i64::MAX as i128 + 1).unwrap();
        let result = dyn_sol_value_to_rhai(&DynSolValue::Int(beyond_i64_max, 256));
        let bigint_val = result.cast::<BigInt>();
        assert_eq!(bigint_val.to_string(), beyond_i64_max.to_string());
    }

    // --- Tests for build_transaction_map ---

    /// Confirms that fields that can be large are always BigInt, and bounded
    /// fields are i64.
    #[test]
    fn test_build_transaction_map_hybrid_types_comprehensive() {
        let tx = TransactionBuilder::new()
            .value(U256::from(1000)) // Small value that could have been i64
            .gas_limit(21000)
            .nonce(5)
            .block_number(12345)
            .max_fee_per_gas(U256::from(200)) // Small value
            .tx_type(TxType::Eip1559)
            .build();

        let receipt = ReceiptBuilder::new()
            .gas_used(18500)
            .status(true)
            .effective_gas_price(150) // Small value
            .build();

        let map = build_transaction_map(&tx, Some(&receipt));

        // --- Assert BigInt types for potentially large values ---
        let value = map.get("value").unwrap().clone();
        assert_eq!(value.type_name(), "num_bigint::bigint::BigInt");
        assert_eq!(value.cast::<BigInt>(), BigInt::from(1000u64));

        let max_fee = map.get("max_fee_per_gas").unwrap().clone();
        assert_eq!(max_fee.type_name(), "num_bigint::bigint::BigInt");
        assert_eq!(max_fee.cast::<BigInt>(), BigInt::from(200u64));

        let effective_price = map.get("effective_gas_price").unwrap().clone();
        assert_eq!(effective_price.type_name(), "num_bigint::bigint::BigInt");
        assert_eq!(effective_price.cast::<BigInt>(), BigInt::from(150u64));

        // --- Assert i64 types for bounded values ---
        let gas_limit = map.get("gas_limit").unwrap().clone();
        assert_eq!(gas_limit.type_name(), "i64");
        assert_eq!(gas_limit.cast::<i64>(), 21000);

        let nonce = map.get("nonce").unwrap().clone();
        assert_eq!(nonce.type_name(), "i64");
        assert_eq!(nonce.cast::<i64>(), 5);

        let block_num = map.get("block_number").unwrap().clone();
        assert_eq!(block_num.type_name(), "i64");
        assert_eq!(block_num.cast::<i64>(), 12345);

        let gas_used = map.get("gas_used").unwrap().clone();
        assert_eq!(gas_used.type_name(), "i64");
        assert_eq!(gas_used.cast::<i64>(), 18500);

        let status = map.get("status").unwrap().clone();
        assert_eq!(status.type_name(), "i64");
        assert_eq!(status.cast::<i64>(), 1);
    }

    #[test]
    fn test_build_transaction_map_legacy_tx() {
        let tx =
            TransactionBuilder::new().gas_price(U256::from(150)).tx_type(TxType::Legacy).build();
        let map = build_transaction_map(&tx, None);

        assert!(!map.contains_key("max_fee_per_gas"));
        assert!(map.contains_key("gas_price"));
        let gas_price = map.get("gas_price").unwrap().clone();
        assert!(gas_price.is::<BigInt>());
        assert_eq!(gas_price.cast::<BigInt>(), BigInt::from(150u64));
    }

    #[test]
    fn test_build_transaction_map_no_receipt_and_creation() {
        let tx = TransactionBuilder::new().to(None).build(); // Contract creation
        let map = build_transaction_map(&tx, None); // No receipt

        assert!(!map.contains_key("to"));
        assert!(map.get("gas_used").unwrap().is_unit());
        assert!(map.get("status").unwrap().is_unit());
        assert!(map.get("effective_gas_price").unwrap().is_unit());
    }

    #[test]
    fn test_build_log_map_all_fields() {
        let log_address = address!("0000000000000000000000000000000000000001");
        let tx_hash = b256!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let log_raw = LogBuilder::new()
            .address(log_address)
            .block_number(100)
            .transaction_hash(tx_hash)
            .log_index(5)
            .transaction_index(2)
            .build();

        let decoded_log = DecodedLog { name: "Transfer".to_string(), params: vec![], log: log_raw };

        let params_map = Map::new(); // Empty for this test
        let map = build_log_map(&decoded_log, params_map);

        assert_eq!(map.get("name").unwrap().clone().cast::<String>(), "Transfer");
        assert_eq!(
            map.get("address").unwrap().clone().cast::<String>(),
            log_address.to_checksum(None)
        );
        assert_eq!(map.get("block_number").unwrap().clone().cast::<i64>(), 100);
        assert_eq!(
            map.get("transaction_hash").unwrap().clone().cast::<String>(),
            tx_hash.to_string()
        );
        assert_eq!(map.get("log_index").unwrap().clone().cast::<i64>(), 5);
        assert_eq!(map.get("transaction_index").unwrap().clone().cast::<i64>(), 2);
    }

    #[test]
    fn test_build_transaction_map_with_receipt() {
        let tx_hash = b256!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let tx = TransactionBuilder::new().hash(tx_hash).build();

        let receipt = ReceiptBuilder::new()
            .transaction_hash(tx_hash)
            .gas_used(18500)
            .status(true)
            .effective_gas_price(145)
            .build();

        let map = build_transaction_map(&tx, Some(&receipt));

        // Verify receipt fields are included with correct types
        assert_eq!(map.get("gas_used").unwrap().clone().cast::<i64>(), 18500);
        assert_eq!(map.get("status").unwrap().clone().cast::<i64>(), 1);
        assert_eq!(
            map.get("effective_gas_price").unwrap().clone().cast::<BigInt>(),
            BigInt::from(145u64)
        );
    }

    // --- Tests for build_transaction_details_payload ---

    #[test]
    fn test_build_transaction_details_payload_comprehensive() {
        let tx = TransactionBuilder::new()
            .value(U256::MAX) // Large value
            .gas_limit(21000)
            .tx_type(TxType::Eip1559)
            .max_fee_per_gas(U256::from(u128::MAX)) // Large value
            .build();
        let receipt = ReceiptBuilder::new()
            .gas_used(18500)
            .status(false)
            .effective_gas_price(u128::MAX) // Large value
            .build();
        let data = build_transaction_details_payload(&tx, Some(&receipt));

        // Assert strings for large number types
        assert_eq!(data["value"], json!(U256::MAX.to_string()));
        assert_eq!(data["max_fee_per_gas"], json!(u128::MAX.to_string()));
        assert_eq!(data["gas_used"], json!("18500")); // Now a string
        assert_eq!(data["effective_gas_price"], json!(u128::MAX.to_string()));

        // Assert numbers for bounded types
        assert_eq!(data["gas_limit"], json!(21000));
        assert_eq!(data["status"], json!(0));
    }

    #[test]
    fn test_build_transaction_details_payload_legacy_and_creation() {
        // Use a large value that is valid for gas_price (fits in u128)
        let large_gas_price = U256::from(u128::MAX);

        let tx = TransactionBuilder::new()
            .to(None) // Contract creation
            .gas_price(large_gas_price)
            .tx_type(TxType::Legacy)
            .build();

        let data = build_transaction_details_payload(&tx, None);

        assert!(data.get("to").is_none());
        assert!(data.get("max_fee_per_gas").is_none());
        assert!(data.get("gas_used").is_none());
        // The final JSON should now correctly be a string.
        assert_eq!(data["gas_price"], json!(large_gas_price.to_string()));
    }

    #[test]
    fn test_build_log_params_payload() {
        let address = address!("0x1234567890abcdef1234567890abcdef12345678");
        let params = vec![
            ("amount".to_string(), DynSolValue::Uint(U256::from(5000), 256)),
            ("recipient".to_string(), DynSolValue::Address(address)),
        ];

        let data = build_log_params_payload(&params);

        assert_eq!(data["amount"], 5000);
        assert_eq!(data["recipient"], json!(address.to_checksum(None)));
    }

    #[test]
    fn test_get_valid_tx_rhai_paths() {
        let valid_paths = get_valid_tx_rhai_paths();
        let expected_paths: HashSet<String> = [
            "tx.to",
            "tx.from",
            "tx.hash",
            "tx.value",
            "tx.gas_limit",
            "tx.nonce",
            "tx.input",
            "tx.block_number",
            "tx.transaction_index",
            "tx.gas_price",
            "tx.max_fee_per_gas",
            "tx.max_priority_fee_per_gas",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();

        assert_eq!(valid_paths, expected_paths);
    }

    #[test]
    fn test_get_valid_receipt_rhai_paths() {
        let valid_paths = get_valid_receipt_rhai_paths();
        let expected_paths: HashSet<String> =
            ["tx.gas_used", "tx.status", "tx.effective_gas_price"]
                .iter()
                .map(|s| s.to_string())
                .collect();

        assert_eq!(valid_paths, expected_paths);
    }

    #[test]
    fn test_get_valid_log_rhai_paths() {
        let valid_paths = get_valid_log_rhai_paths();
        let expected_paths: HashSet<String> = [
            "log.address",
            "log.block_number",
            "log.transaction_hash",
            "log.transaction_index",
            "log.name",
            "log.log_index",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();

        assert_eq!(valid_paths, expected_paths);
    }

    #[test]
    fn test_get_valid_decoded_call_rhai_paths() {
        let valid_paths = get_valid_decoded_call_rhai_paths();
        let expected_paths: HashSet<String> =
            ["decoded_call.name", "decoded_call.params", "decoded_call"]
                .iter()
                .map(|s| s.to_string())
                .collect();

        assert_eq!(valid_paths, expected_paths);
    }
}
