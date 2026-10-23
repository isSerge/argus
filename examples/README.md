# Examples

This directory contains various examples demonstrating how to configure and use
Argus EVM Monitor.

## List of Examples

- [1. Basic ETH Transfer Monitor](./1_basic_eth_transfer/README.md): This
  example sets up a monitor that triggers when a transaction with a value
  greater than 10 ETH is detected on the Ethereum mainnet.
- [2. Large USDC Transfer Monitor](./2_large_usdc_transfer/README.md): This
  example sets up a monitor that triggers when a `Transfer` event with a value
  greater than 1,000,000 USDC is detected from the USDC contract on the Ethereum
  mainnet.
- [3. WETH Deposit](./3_weth_deposit/README.md): This example sets up a monitor that triggers when a `Deposit` event is detected from the WETH contract on the Ethereum mainnet.
- [4. All ERC20 Transfers Monitor for Specific EOA](./4_all_erc20_transfers_for_eoa/README.md): This example sets up a global log monitor that triggers for any `Transfer` event from any ERC20-compliant contract on the Ethereum mainnet involving specific EOA.
- [5. Monitor with Throttling Policy](./5_action_with_throttle_policy/README.md): This example demonstrates how to configure a action with a `throttle` policy to limit the rate of notifications.
- [6. Action with Aggregation Policy](./6_action_with_aggregation_policy/README.md): This example demonstrates how to use the `sum` and `avg` filters in action templates to aggregate values from multiple monitor matches.
- [7. Address Watchlist Monitor](./7_address_watchlist_monitor/README.md): This example demonstrates how to use an array as a watchlist to monitor for any activity involving a specific set of addresses.
- [8. High Priority Fee](./8_high_priority_fee/README.md): This example demonstrates how to monitor for transactions with unusually high priority fees.
- [9. Admin Function Call Monitor](./9_admin_function_call/README.md): This example demonstrates how to monitor for calls to a specific function on a contract.
- [10. Action with Kafka Publisher](./10_action_with_kafka_publisher/README.md): This example demonstrates how to configure and use a `kafka` action to send notifications to a Kafka topic.
- [11. Action with RabbitMQ Publisher](./11_action_with_rabbitmq_publisher/README.md): This example demonstrates how to configure and use a `rabbitmq` action to send notifications to a RabbitMQ exchange.
- [12. Action with NATS Publisher](./12_action_with_nats_publisher/README.md): This example demonstrates how to configure and use a `nats` action to send notifications to a NATS subject.
- [13. Large L1→L2 Bridge Deposit on Base](./13_base_large_bridge_deposit/README.md): This chain-specific example monitors `ETHBridgeFinalized` events on Base's L2 StandardBridge for bridge-ins over 10 ETH.
- [14. BNB Validator Finality Reward Deposits](./14_bnb_validator_finality_rewards/README.md): This chain-specific example watches `finalityRewardDeposit` events on BNB's `BSCValidatorSet` system contract — a heartbeat of BNB's BEP-126 fast-finality machinery.
- [15. New Contract Deployments on Robinhood Chain](./15_robinhood_contract_deploy/README.md): This example watches contract creations on the newest, fastest chain covered here (~100ms Arbitrum Orbit blocks).

---

## Have a great use case you'd like to share?

If you have a use case that you think would be valuable to add here as an
example, feel free to
[submit an issue](https://github.com/isSerge/argus-rs/issues/new/choose) with
your suggestion!
