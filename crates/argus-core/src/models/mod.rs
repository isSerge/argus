//! This module contains the data models for the Argus application.

pub mod action;
pub mod action_id;
pub mod alert_manager_state;
pub mod block_data;
pub mod correlated_data;
pub mod decoded_block;
pub mod log;
pub mod monitor;
pub mod monitor_match;
pub mod network_id;
pub mod notification;
pub mod transaction;
pub mod transaction_builder;

pub use action_id::ActionId;
pub use alloy::network::AnyTransactionReceipt as TransactionReceipt;
pub use block_data::BlockData;
pub use correlated_data::CorrelatedBlockItem;
pub use decoded_block::CorrelatedBlockData;
pub use log::Log;
pub use network_id::NetworkId;
pub use notification::NotificationMessage;
pub use transaction::Transaction;
