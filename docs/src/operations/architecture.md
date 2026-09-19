# Architecture

This document provides a high-level overview of the internal architecture of the Argus application.

## Core Principles

-   **Modular**: The codebase is organized as a Cargo workspace of focused library crates, each with a distinct and well-defined responsibility.
-   **Asynchronous**: Built on top of Tokio, the application is designed to be highly concurrent and non-blocking.
-   **Stateful**: The application's progress is persisted to a local database, allowing for resilience and crash recovery.
-   **Decoupled**: Components communicate through channels, reducing tight coupling and improving maintainability.

## Workspace Layout

```text
argus-rs/
├── src/                  # Binary entry point – CLI, context wiring, supervisor
└── crates/
    ├── argus-core         # Core models, config structs, and abstract traits
    ├── argus-store        # SQLite persistence (implements argus-core traits)
    ├── argus-providers    # Alloy RPC block fetcher (implements DataSource)
    ├── argus-dispatch     # Notification dispatch: webhooks, Kafka, RabbitMQ, NATS
    ├── argus-abi          # ABI repository and log/calldata decoding
    ├── argus-rhai         # Rhai script compiler and filtering engine
    ├── argus-monitor      # Monitor validation, interest registry, MonitorManager
    ├── argus-engine       # Pipeline orchestration: ingestor, processor, alert manager, outbox
    └── argus-api          # Axum HTTP REST API server
```

## Key Components

### `src/` – Binary

The root `src/` directory is a thin application entry point. It imports all library crates and wires them together at startup.

-   **`supervisor`**: The top-level orchestrator. Initializes all services, connects them through async channels, and manages graceful shutdown.
-   **`context`**: Builds the `AppContext` – reads configuration, creates the database connection, RPC provider, and all service instances.
-   **`cmd`**: CLI command definitions (`run`, `dry-run`).
-   **`loader`**: Loads and parses YAML configuration files.
-   **`main.rs`**: Application entry point; handles CLI argument parsing.

### `argus-core`

Defines the shared domain: models (`Monitor`, `BlockData`, `Transaction`, `Log`, `MonitorMatch`, `Action`), config structs, and the abstract `AppRepository`, `KeyValueStore`, and `DataSource` traits. No heavy dependencies; all other crates build on top of this one.

### `argus-store`

Implements `AppRepository` and `KeyValueStore` for SQLite using `sqlx`. All database migrations live in `migrations/` at the workspace root.

### `argus-providers`

Contains the Alloy-based RPC client (`EvmRpcSource`) and concurrent block fetcher. Implements the `DataSource` trait from `argus-core`.

### `argus-abi`

Manages ABI storage and exposes an `AbiService` for decoding event logs and transaction calldata against stored ABIs.

### `argus-rhai`

Wraps the `rhai`, `rhai-bigint`, `rhai-evm`, and `rhai-analyzer` crates to provide a safe `RhaiCompiler` and the `RhaiFilteringEngine` that executes monitor scripts.

### `argus-monitor`

Hosts the `MonitorManager`, the monitor interest registry (which monitors care about which contracts/topics), and the `MonitorValidator` for validating monitor configs before they are accepted.

### `argus-engine`

Wires the data pipeline:

-   **`BlockIngestor`**: Polls the provider for new blocks and sends raw `BlockData` into the pipeline.
-   **`BlockProcessor`**: Correlates transactions with logs and receipts into `CorrelatedBlockData`. Also verifies parent-hash continuity between consecutive blocks to detect reorgs past the confirmation depth (warn log + `reorgs_detected` metric; no behavioral change).
-   **`FilteringEngine`** (via `argus-rhai`): Runs Rhai scripts against each correlated item; emits `MonitorMatch` objects.
-   **`AlertManager`**: Applies throttle/aggregation policies and enqueues alerts to the Outbox.
-   **`OutboxProcessor`**: Drains the persistent Outbox and forwards alerts to `argus-dispatch`.

### `argus-dispatch`

Implements the notification layer. Wraps `omnihook` for Slack/Discord/Telegram payloads, `rdkafka` for Kafka, `lapin` for RabbitMQ, and `async-nats` for NATS. Uses MiniJinja for message templating.

### `argus-api`

An Axum HTTP server exposing the REST management API for monitors, actions, and ABIs.

