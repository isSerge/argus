# 15. New Contract Deployments on Robinhood Chain

This example monitors contract creations on Robinhood Chain (chain ID 4663),
an Arbitrum Orbit rollup producing blocks every ~100ms — the fastest chain
covered by this examples set.

### Why this is a chain-specific example

Robinhood Chain is brand new and being actively built out: deploys arrive in
a steady drip (a 1,000-block probe — under two minutes of chain time — found
3 creations). Watching `tx.to == ()` (creation transactions carry no
recipient) gives you a live feed of the chain's ecosystem forming. Parsing
its blocks at all requires support for the per-block Orbit `0x6a` system
transaction (see the
[chain compatibility notes](../../docs/src/user_guide/app_yaml.md#chain-compatibility)).

### Configuration Files

- [`app.yaml`](./app.yaml): Robinhood-tuned application configuration
  (~100ms blocks, adaptive polling clamped to its 250ms floor).
- [`monitors.yaml`](./monitors.yaml): Plain transaction monitor — no address
  or ABI needed for creations.
- [`actions.yaml`](./actions.yaml): Stdout action with a deployment message.

### Monitor Configuration

```yaml
monitors:
  - name: "New Contract Deployments on Robinhood Chain"
    network: "robinhood-chain"
    filter_script: |
      tx.to == ()
    actions:
      - "stdout-action"
```

- **`filter_script`**: creation transactions have no `to` field, which the
  [`tx` context](../../docs/src/user_guide/rhai_context.md) exposes as the
  unit value `()`.

### How to Run (Dry-Run Mode)

Test against a historical window containing three contract deployments:

```bash
cargo run --release -- dry-run --from 94731510 --to 94732000 --config-dir examples/15_robinhood_contract_deploy/
```

Expected output: 3 matches from monitor "New Contract Deployments on
Robinhood Chain" at blocks 94731514, 94731954 and 94731961, dispatched to
`stdout-action`.

### How to Run (Default Mode)

```bash
cargo run --release -- run --config-dir examples/15_robinhood_contract_deploy/
```
