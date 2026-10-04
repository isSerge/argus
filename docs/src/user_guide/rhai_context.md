# Rhai Data Context (`tx` & `log`)

When your `filter_script` is executed, it has access to a set of variables that contain the context of the on-chain event. This context is primarily exposed through the `tx` and `log` objects.

## The `tx` Object (Transaction Data)

The `tx` object is available in **all** monitor scripts. It contains detailed information about the transaction being processed.

| Field | Type | Description |
| :--- | :--- | :--- |
| `hash` | String | The transaction hash. |
| `from` | String | The sender's address. |
| `to` | String | The recipient's address (can be `null` for contract creation). |
| `value` | BigInt | The amount of native currency (e.g., ETH) transferred, in wei. |
| `gas_limit` | Integer | The gas limit for the transaction. |
| `nonce` | Integer | The transaction nonce. |
| `input` | String | The transaction input data (calldata). |
| `block_number` | Integer | The block number the transaction was included in. |
| `transaction_index` | Integer | The index of the transaction within the block. |
| `gas_price` | BigInt | (Legacy Transactions) The gas price in wei. |
| `max_fee_per_gas` | BigInt | (EIP-1559) The maximum fee per gas in wei. |
| `max_priority_fee_per_gas` | BigInt | (EIP-1559) The maximum priority fee per gas in wei. |
| `gas_used` | BigInt | (From Receipt) The actual gas used by the transaction. |
| `status` | Integer | (From Receipt) The transaction status (1 for success, 0 for failure). |
| `effective_gas_price` | BigInt | (From Receipt) The effective gas price paid, in wei. |

> **Note on chain-specific transaction types:** OP-stack deposit transactions (`0x7e`) and
> Arbitrum/Orbit system transactions (`0x6a`) carry no fee fields — `gas_price`,
> `max_fee_per_gas` and `max_priority_fee_per_gas` are absent (`()`) for them. Scripts
> comparing fee fields should handle their absence.

### Example Usage

```rhai
// Check for a transaction from a specific address with a high value
tx.from == "0x1234..." && tx.value > ether(50)

// Check for a failed transaction
tx.status == 0
```

## The `log` Object (Decoded Event Log)

The `log` object is only available for monitors that have an `address` and an `abi` defined. It contains the decoded data from a specific event log emitted by that contract.

| Field | Type | Description |
| :--- | :--- | :--- |
| `address` | String | The address of the contract that emitted the log. |
| `log_index` | Integer | The index of the log within the block. |
| `name` | String | The name of the decoded event (e.g., "Transfer", "Approval"). |
| `params` | Map | A map containing the event's parameters, accessed by name. |

### The `log.params` Map

The `params` field is the most important part of the `log` object. It allows you to access the event's parameters by their names as defined in the ABI.

For an ERC20 `Transfer` event with the signature `Transfer(address indexed from, address indexed to, uint256 value)`, the `log.params` map would contain:

-   `log.params.from` (String)
-   `log.params.to` (String)
-   `log.params.value` (BigInt)

### Example Usage

```rhai
// Check for a specific event
log.name == "Transfer"

// Check for an event with specific parameter values
log.name == "Transfer" && log.params.from == "0xabcd..."

// Check for a large transfer value
log.name == "Transfer" && log.params.value > usdc(1_000_000)
```

For more practical examples of using `tx` and `log` data in Rhai scripts, refer to the [Example Gallery](../examples/gallery.md).

## The `decoded_call` Object (Decoded Calldata)

The `decoded_call` object is available for monitors that have an `address`, an `abi`, and access `decoded_call` field, which contains the decoded data from the transaction's input data (calldata).

If calldata cannot be decoded (e.g., the function selector is unknown), `decoded_call` will be `()`, which is Rhai's `null` equivalent.

| Field | Type | Description |
| :--- | :--- | :--- |
| `name` | String | The name of the decoded function (e.g., "transfer", "approve"). |
| `params` | Map | A map containing the function's parameters, accessed by name. |

### The `decoded_call.params` Map

Similar to `log.params`, this field allows you to access the function's parameters by their names as defined in the ABI.

For a `transfer` function with the signature `transfer(address to, uint256 amount)`, the `decoded_call.params` map would contain:

-   `decoded_call.params.to` (String)
-   `decoded_call.params.amount` (BigInt)

### Example Usage

```rhai
// Check for a call to a specific function, even if decoded_call might be null.
decoded_call.name == "transfer"

// Safely check a nested parameter.
decoded_call.params.amount > ether(100)

// Check if calldata decoding failed.
decoded_call == ()
```
