# 14. BNB Validator Finality Reward Deposits

This example monitors BEP-126 fast-finality reward payouts on BNB Smart Chain
by watching `finalityRewardDeposit` events from the `BSCValidatorSet` system
contract (`0x0000000000000000000000000000000000001000`).

### Why this is a chain-specific example

BNB's consensus layer lives on-chain. Every 161-block turn (~72s at ~0.45s
blocks), the validator set contract receives ~326 system transactions: mostly
`deposit(address)` operator maintenance, plus ~2 `distributeFinalityReward`
calls that pay fast-finality attestation rewards to validators. Each
distribution emits one `finalityRewardDeposit(validator, amount)` event per
validator (~21 per distribution), giving a steady, chain-specific heartbeat of
BSC's fast-finality machinery.

Two lessons baked into this example:

- **Packed calldata:** BNB's system-contract calldata uses a non-standard
  encoding (length-prefixed inline arrays instead of ABI head/tail offsets),
  which standard ABI decoding correctly rejects — so `decoded_call` is not
  usable here. Logs are always standard-encoded, making
  `finalityRewardDeposit` the monitorable surface.
- **Mechanism, not money:** the per-validator rewards are tiny (~0.001 BNB).
  This monitor is about visibility into the consensus mechanism, not
  value alerts — a good reminder that chain-specific signals are not
  automatically high-value signals.

### Configuration Files

- [`app.yaml`](./app.yaml): BNB-tuned application configuration (~450ms
  Maxwell-era blocks, blockrazor archive RPC, 25-block `eth_getLogs` cap).
- [`monitors.yaml`](./monitors.yaml): Log monitor on the validator set
  contract.
- [`actions.yaml`](./actions.yaml): Stdout action with a reward message.

### Monitor Configuration

```yaml
monitors:
  - name: "BNB Validator Finality Reward Deposits"
    network: "bnb-smart-chain"
    address: "0x0000000000000000000000000000000000001000"
    abi: "bsc_validator_set"
    filter_script: |
      log.name == "finalityRewardDeposit"
    actions:
      - "stdout-action"
```

- **`address` + `abi`**: restricts decoding to the validator set contract,
  enabling `log.name` / `log.params`, using the minimal
  [`bsc_validator_set`](../../abis/bsc_validator_set.json) ABI.
- **`filter_script`**: matches every per-validator reward deposit. Live
  volume is high by design (~2 distributions per 161-block turn) — add a
  threshold on `log.params.amount` or a throttle policy (see
  [example 5](../5_action_with_throttle_policy/README.md)) to tune it.

### How to Run (Dry-Run Mode)

Test against a historical window containing two full reward distributions:

```bash
cargo run --release -- dry-run --from 129038350 --to 129038680 --config-dir examples/14_bnb_validator_finality_rewards/
```

Expected output: 42 matches from monitor "BNB Validator Finality Reward
Deposits" (21 validator payouts in block 129038400 and 21 in block 129038600,
each ~0.001 BNB), dispatched to `stdout-action`.

### How to Run (Default Mode)

```bash
cargo run --release -- run --config-dir examples/14_bnb_validator_finality_rewards/
```
