# 13. Large L1→L2 Bridge Deposit on Base

This example monitors large ETH inflows from Ethereum to Base by watching the
canonical `ETHBridgeFinalized` event on the L2 StandardBridge
(`0x4200000000000000000000000000000000000010` — the OP-stack system address,
identical on every OP-stack chain).

### Why this is a chain-specific example

Every Base block contains OP-stack deposit transactions (`type 0x7e`) and
Argus parses them as first-class transactions (see the
[chain compatibility notes](../../docs/src/user_guide/app_yaml.md#chain-compatibility)).
A naive deposit monitor on the `value` field of `0x7e` transactions would
never fire: nearly all L1→L2 ETH moves through the WETH bridge path, so the
deposit transaction itself carries `value == 0` and the mint happens inside
bridge calldata. The `ETHBridgeFinalized` log is the reliable signal.

Expected frequency: ~20 bridge-in events per day, of which a handful exceed
10 ETH. A 24h RPC probe found 3 events ≥ 10 ETH (80–85 ETH each, a recurring
bridge bot moving funds in ~6h intervals).

### Configuration Files

- [`app.yaml`](./app.yaml): Base-tuned application configuration (2s blocks,
  bloXroute archive RPC — public endpoints like publicnode only keep a short
  recent window and `mainnet.base.org` rate-limits `eth_getLogs`).
- [`monitors.yaml`](./monitors.yaml): Log monitor on the L2StandardBridge with
  the [`standard_bridge`](../../abis/standard_bridge.json) ABI.
- [`actions.yaml`](./actions.yaml): Stdout action with a bridge-deposit message.

### Monitor Configuration

```yaml
monitors:
  - name: "Large L1-to-L2 ETH Bridge Deposits on Base"
    network: "base"
    address: "0x4200000000000000000000000000000000000010"
    abi: "standard_bridge"
    filter_script: |
      log.name == "ETHBridgeFinalized" && log.params.amount > ether(10)
    actions:
      - "stdout-action"
```

- **`address` + `abi`**: restrict decoding to the L2 StandardBridge using the
  minimal `standard_bridge.json` ABI, enabling `log.name` / `log.params`.
- **`filter_script`**: matches finalized bridge-ins of more than 10 ETH via
  the [`ether()`](../../docs/src/user_guide/rhai_helpers.md) helper.

### How to Run (Dry-Run Mode)

Test against a historical window containing three ≥ 10 ETH bridge-ins
(84.21, 82.18 and 80.86 ETH):

```bash
cargo run --release -- dry-run --from 52911840 --to 52919700 --config-dir examples/13_base_large_bridge_deposit/
```

Expected output: 3 matches from monitor
"Large L1-to-L2 ETH Bridge Deposits on Base" at blocks 52911844, 52914137 and
52919645, each dispatched to `stdout-action`. (The range spans ~7,900 blocks
— a few minutes of processing.)

### How to Run (Default Mode)

```bash
cargo run --release -- run --config-dir examples/13_base_large_bridge_deposit/
```
