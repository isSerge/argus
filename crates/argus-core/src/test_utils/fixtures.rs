//! Raw JSON fixtures captured from live chains, used to regression-test
//! parsing of chain-specific transaction types:
//! - OP-stack deposit transactions (type `0x7e`) — Base, block 0x31bde9f
//! - Orbit/Arbitrum system transactions (type `0x6a`) — Robinhood Chain,
//!   block 0x4c204bb (the per-block L1-posting tx)

pub const OP_DEPOSIT_TX_JSON: &str = r#"{
    "blockHash": "0x64d3010c7c0709e760500250dc4177f2479571a79fdcfc48eb73cda64c7332bd",
    "blockNumber": "0x31bde9f",
    "blockTimestamp": "0x6ac21a21",
    "depositReceiptVersion": "0x1",
    "from": "0xdeaddeaddeaddeaddeaddeaddeaddeaddead0001",
    "gas": "0xf4240",
    "gasPrice": "0x0",
    "hash": "0x233ace96f255608781596edfe1520000bd25b69b122cc101be3abb6689a85322",
    "input": "0x3db6be2b000008dd00101c120000000000000005000000006ac2196700000000018e8792000000000000000000000000000000000000000000000000000000000be3256b00000000000000000000000000000000000000000000000000000000008b9262ae74035a834ae8863310574c2213838e9de36478c4f8c048cf3ea5d3ffcec4010000000000000000000000005050f69a9786f081509234f1a7f4684b5e5b76c90000000000000000000000000094",
    "mint": "0x0",
    "nonce": "0x31bdea2",
    "r": "0x0",
    "s": "0x0",
    "sourceHash": "0x391329aebfee79dbb200b510b04d8795c891055a887433c20a7392b77b0f2fec",
    "to": "0x4200000000000000000000000000000000000015",
    "transactionIndex": "0x0",
    "type": "0x7e",
    "v": "0x0",
    "value": "0x0",
    "yParity": "0x0"
}"#;

pub const OP_DEPOSIT_RECEIPT_JSON: &str = r#"{
    "blobGasUsed": "0x4154",
    "blockHash": "0x64d3010c7c0709e760500250dc4177f2479571a79fdcfc48eb73cda64c7332bd",
    "blockNumber": "0x31bde9f",
    "contractAddress": null,
    "cumulativeGasUsed": "0xb48a",
    "daFootprintGasScalar": "0x94",
    "depositNonce": "0x31bdea2",
    "depositReceiptVersion": "0x1",
    "effectiveGasPrice": "0x0",
    "from": "0xdeaddeaddeaddeaddeaddeaddeaddeaddead0001",
    "gasUsed": "0xb48a",
    "l1BaseFeeScalar": "0x8dd",
    "l1BlobBaseFee": "0x8b9262",
    "l1BlobBaseFeeScalar": "0x101c12",
    "l1Fee": "0x0",
    "l1GasPrice": "0xbe3256b",
    "l1GasUsed": "0x71d",
    "logs": [],
    "logsBloom": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
    "status": "0x1",
    "to": "0x4200000000000000000000000000000000000015",
    "transactionHash": "0x233ace96f255608781596edfe1520000bd25b69b122cc101be3abb6689a85322",
    "transactionIndex": "0x0",
    "type": "0x7e"
}"#;

pub const ORBIT_SYSTEM_TX_JSON: &str = r#"{
    "blockHash": "0xd73db86f1603784df5fc164e8fe18756a926ded0922813be2ce269fe5a12c71b",
    "blockNumber": "0x4c204bb",
    "blockTimestamp": "0x6ac21a2a",
    "from": "0x00000000000000000000000000000000000a4b05",
    "gas": "0x0",
    "gasPrice": "0x0",
    "hash": "0xfc214c19fee15e5d244c4ecefc4f979974f4177bd85d311e2dceba121733d4f8",
    "input": "0x6bf6a42d000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000018e87a10000000000000000000000000000000000000000000000000000000004c204bb0000000000000000000000000000000000000000000000000000000000000000",
    "nonce": "0x0",
    "to": "0x00000000000000000000000000000000000a4b05",
    "transactionIndex": "0x0",
    "value": "0x0",
    "type": "0x6a",
    "chainId": "0x1237",
    "v": "0x0",
    "r": "0x0",
    "s": "0x0"
}"#;
