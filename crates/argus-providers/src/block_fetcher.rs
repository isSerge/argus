//! This module provides reusable functions for fetching block data from a
//! DataSource. This is used by the BlockIngestor and Dry-run command to
//! retrieve block data concurrently.

use std::collections::{HashMap, VecDeque};

use alloy::rpc::types::Block;
use argus_core::{
    models::{BlockData, Log},
    providers::traits::{DataSource, DataSourceError},
};
use futures::{
    join,
    stream::{self, StreamExt},
};

/// Configuration for the concurrent block-fetch path.
#[derive(Debug, Clone, Copy)]
pub struct FetchConfig {
    /// Maximum number of in-flight `get_block_by_number` requests.
    pub concurrency: usize,
    /// Maximum number of blocks per `eth_getLogs` RPC call.
    /// Set to `0` to issue a single call covering the whole range.
    pub log_chunk_size: u64,
}

impl FetchConfig {
    pub fn new(concurrency: usize, log_chunk_size: u64) -> Self {
        Self { concurrency, log_chunk_size }
    }
}

/// Fetches all blocks (without logs) for a range concurrently.
async fn fetch_blocks_only<D: DataSource + ?Sized>(
    data_source: &D,
    from_block: u64,
    to_block: u64,
    concurrency: usize,
) -> Result<Vec<Block>, DataSourceError> {
    let block_stream = stream::iter(from_block..=to_block)
        .map(|block_num| data_source.fetch_block_only(block_num));

    let mut buffered = block_stream.buffered(concurrency);
    let mut blocks = Vec::new();
    while let Some(result) = buffered.next().await {
        match result {
            Ok(block) => blocks.push(block),
            Err(e) => {
                tracing::error!(
                    error = %e,
                    from_block,
                    to_block,
                    "Failed to fetch block in range"
                );
                return Err(e);
            }
        }
    }
    Ok(blocks)
}

/// Splits the inclusive range `from_block..=to_block` into sub-ranges of at
/// most `chunk_size` blocks (`chunk_size >= 1`).
fn chunk_range(
    from_block: u64,
    to_block: u64,
    chunk_size: u64,
) -> impl Iterator<Item = (u64, u64)> {
    (from_block..=to_block)
        .step_by(usize::try_from(chunk_size).unwrap_or(usize::MAX))
        .map(move |start| (start, start.saturating_add(chunk_size - 1).min(to_block)))
}

/// Splits a multi-block range into two contiguous halves.
fn halve_range((start, end): (u64, u64)) -> [(u64, u64); 2] {
    let mid = start + (end - start) / 2;
    [(start, mid), (mid + 1, end)]
}

type ChunkOutcome = ((u64, u64), Result<Vec<Log>, DataSourceError>);

/// Fetches a batch of sub-ranges concurrently, tagging each result with its
/// originating range.
async fn fetch_batch<D: DataSource + ?Sized>(
    data_source: &D,
    batch: impl IntoIterator<Item = (u64, u64)>,
    concurrency: usize,
) -> Vec<ChunkOutcome> {
    stream::iter(batch)
        .map(|range| async move {
            (range, data_source.fetch_logs_for_range(range.0, range.1).await)
        })
        .buffer_unordered(concurrency)
        .collect()
        .await
}

/// Fetches all logs for `from_block..=to_block`, splitting the request into
/// sub-ranges of at most `chunk_size` blocks when `chunk_size > 0`.
///
/// Sub-range requests are issued concurrently, bounded by `concurrency`, so
/// a large range never opens more simultaneous `eth_getLogs` connections than
/// the configured fetch concurrency limit.
///
/// When a sub-range request fails, the provider is usually rejecting the
/// response size (result count / payload) rather than the block range itself.
/// The failing range is therefore halved and both halves are re-enqueued for
/// retry with priority, until they either succeed or shrink to a single block;
/// a single-block failure is propagated as-is, which bounds the halving and
/// guarantees termination.
///
/// When `chunk_size == 0` both chunking and adaptive shrinking are disabled:
/// the entire range is covered by a single RPC call (legacy behaviour).
async fn fetch_logs_chunked<D: DataSource + ?Sized>(
    data_source: &D,
    from_block: u64,
    to_block: u64,
    chunk_size: u64,
    concurrency: usize,
) -> Result<Vec<Log>, DataSourceError> {
    if chunk_size == 0 {
        return data_source.fetch_logs_for_range(from_block, to_block).await;
    }

    let concurrency = concurrency.max(1);
    let mut pending: VecDeque<(u64, u64)> = chunk_range(from_block, to_block, chunk_size).collect();
    let mut logs = Vec::new();

    while !pending.is_empty() {
        let batch: Vec<_> = pending.drain(..concurrency.min(pending.len())).collect();

        let mut retries: VecDeque<(u64, u64)> = VecDeque::new();
        for (range, result) in fetch_batch(data_source, batch, concurrency).await {
            match result {
                Ok(chunk_logs) => logs.extend(chunk_logs),
                Err(e) if range.0 == range.1 => return Err(e),
                Err(e) => {
                    let halves = halve_range(range);
                    tracing::warn!(
                        error = %e,
                        from_block = range.0,
                        to_block = range.1,
                        retry_chunk_blocks = halves[0].1 - halves[0].0 + 1,
                        "eth_getLogs sub-range failed; retrying with halved chunk size"
                    );
                    retries.extend(halves);
                }
            }
        }

        // Shrunk ranges are retried ahead of the remaining pending chunks.
        retries.extend(pending);
        pending = retries;
    }

    Ok(logs)
}

/// Fetches a range of blocks concurrently.
///
/// Uses `eth_getLogs(from, to)` in parallel with all `get_block_by_number`
/// calls, replacing the previous per-block log-fetch strategy. This collapses
/// N log RTTs into one and overlaps it with block fetching via `tokio::join!`.
///
/// `log_chunk_size` caps the block range of each individual `eth_getLogs` RPC
/// call. When the overall range (`to_block - from_block`) exceeds this value
/// the log fetch is split into parallel sub-range requests, preventing errors
/// from providers that reject wide log windows (e.g. Alchemy, Ankr). Set to
/// `0` to disable chunking (single call, legacy behaviour).
///
/// Sub-ranges that a provider rejects (e.g. response too large) are
/// automatically halved and retried — down to a single block — before the
/// error is surfaced.
///
/// Returns an error if any block fails to fetch. This ensures consistent
/// behavior across all components and prevents gaps in block processing.
pub async fn fetch_blocks_concurrent<D: DataSource + ?Sized>(
    data_source: &D,
    needs_receipts: bool,
    from_block: u64,
    to_block: u64,
    cfg: FetchConfig,
) -> Result<Vec<BlockData>, DataSourceError> {
    // Fire the range log-fetch (split into provider-safe chunks) and all
    // block-fetches in parallel.
    let (range_logs_result, blocks_result) = join!(
        fetch_logs_chunked(data_source, from_block, to_block, cfg.log_chunk_size, cfg.concurrency),
        fetch_blocks_only(data_source, from_block, to_block, cfg.concurrency)
    );

    let range_logs = range_logs_result?;
    let blocks: Vec<Block> = blocks_result?;

    // Group logs by block number for O(1) lookup when building BlockData.
    let mut logs_by_block: HashMap<u64, Vec<_>> = HashMap::new();
    for log in range_logs {
        if let Some(block_num) = log.block_number() {
            logs_by_block.entry(block_num).or_default().push(log);
        }
    }

    // Fetch all receipts in one batch if needed.
    let mut receipts_map = HashMap::new();
    if needs_receipts {
        let all_tx_hashes: Vec<_> = blocks.iter().flat_map(|b| b.transactions.hashes()).collect();
        if !all_tx_hashes.is_empty() {
            receipts_map = data_source.fetch_receipts(&all_tx_hashes, cfg.concurrency).await?;
        }
    }

    // Combine blocks with their logs and receipts into BlockData.
    let mut block_data_vec: Vec<BlockData> = blocks
        .into_iter()
        .map(|block| {
            let block_num = block.header.number;
            let logs = logs_by_block.remove(&block_num).unwrap_or_default();
            let receipts = block
                .transactions
                .hashes()
                .filter_map(|h| receipts_map.remove(&h).map(|r| (h, r)))
                .collect();
            BlockData::from_raw_data(block, receipts, logs)
        })
        .collect();

    // Sort by block number to ensure correct order after concurrent fetching.
    block_data_vec.sort_by_key(|bd| bd.block.header.number);

    Ok(block_data_vec)
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::{Arc, Mutex},
    };

    use argus_core::{providers::traits::MockDataSource, test_utils::BlockBuilder};

    use super::*;

    type CallLog = Arc<Mutex<Vec<(u64, u64, bool)>>>;

    fn provider_error() -> DataSourceError {
        DataSourceError::Provider(Box::new(io::Error::other(
            "query returned more than 10000 results",
        )))
    }

    /// Mocks `fetch_logs_for_range` so that any range spanning
    /// `fail_width` or more blocks fails; narrower ranges succeed. Every call
    /// is recorded as `(from, to, ok)`.
    fn mock_source(fail_width: u64, calls: CallLog) -> MockDataSource {
        let mut mock = MockDataSource::new();
        mock.expect_fetch_logs_for_range().times(..).returning({
            let calls = calls.clone();
            move |from, to| {
                let ok = to - from < fail_width;
                calls.lock().unwrap().push((from, to, ok));
                if ok { Ok(Vec::new()) } else { Err(provider_error()) }
            }
        });
        mock
    }

    fn recorded(calls: &CallLog) -> Vec<(u64, u64, bool)> {
        calls.lock().unwrap().clone()
    }

    fn assert_full_coverage(calls: &CallLog, from_block: u64, to_block: u64) {
        let mut covered: Vec<u64> = recorded(calls)
            .into_iter()
            .filter(|&(_, _, ok)| ok)
            .flat_map(|(from, to, _)| from..=to)
            .collect();
        covered.sort_unstable();
        let expected: Vec<u64> = (from_block..=to_block).collect();
        assert_eq!(covered, expected, "every block fetched exactly once");
    }

    #[tokio::test]
    async fn logs_chunked_splits_by_chunk_size() {
        let calls: CallLog = Arc::new(Mutex::new(Vec::new()));
        // fail_width = u64::MAX: no range ever fails, requests are never shrunk.
        let source = mock_source(u64::MAX, calls.clone());

        let logs = fetch_logs_chunked(&source, 0, 9, 4, 2).await.unwrap();

        assert!(logs.is_empty());
        let mut ranges: Vec<_> = recorded(&calls).into_iter().map(|(f, t, _)| (f, t)).collect();
        ranges.sort_unstable();
        assert_eq!(ranges, vec![(0, 3), (4, 7), (8, 9)]);
    }

    #[tokio::test]
    async fn logs_chunked_halves_oversized_ranges() {
        // Ranges spanning >= 2 blocks are rejected by the provider; the fetcher
        // must halve them down to single blocks and still cover everything.
        let calls: CallLog = Arc::new(Mutex::new(Vec::new()));
        let source = mock_source(2, calls.clone());

        let logs = fetch_logs_chunked(&source, 0, 9, 4, 1).await.unwrap();

        assert!(logs.is_empty());
        assert!(recorded(&calls).iter().any(|&(_, _, ok)| !ok), "shrinking was exercised");
        assert_full_coverage(&calls, 0, 9);
    }

    #[tokio::test]
    async fn logs_chunked_propagates_error_at_single_block_floor() {
        // fail_width 0: every range, including single blocks, fails.
        let calls: CallLog = Arc::new(Mutex::new(Vec::new()));
        let source = mock_source(0, calls.clone());

        let err = fetch_logs_chunked(&source, 0, 7, 4, 1).await.unwrap_err();

        assert!(matches!(err, DataSourceError::Provider(_)));
        assert!(err.to_string().contains("query returned more than 10000 results"));
        // Deterministic halving down to the single-block floor: (0,3) -> (0,1) -> (0,0).
        assert_eq!(recorded(&calls), vec![(0, 3, false), (0, 1, false), (0, 0, false)]);
    }

    #[tokio::test]
    async fn logs_chunked_zero_disables_adaptivity() {
        let calls: CallLog = Arc::new(Mutex::new(Vec::new()));
        let source = mock_source(0, calls.clone());

        let result = fetch_logs_chunked(&source, 0, 99, 0, 4).await;

        assert!(result.is_err());
        assert_eq!(recorded(&calls), vec![(0, 99, false)], "exactly one unchunked call");
    }

    #[tokio::test]
    async fn blocks_concurrent_with_adaptive_shrinking() {
        let calls: CallLog = Arc::new(Mutex::new(Vec::new()));
        let mut source = mock_source(2, calls.clone());

        source
            .expect_fetch_block_only()
            .times(..)
            .returning(|n| Ok(BlockBuilder::new().number(n).build()));

        // Chunk 4 vs a 4-block range: (10,13) is rejected, halves (10,11) and
        // (12,13) succeed.
        let block_data =
            fetch_blocks_concurrent(&source, false, 10, 13, FetchConfig::new(2, 4)).await.unwrap();

        assert_eq!(block_data.len(), 4);
        assert_eq!(
            block_data.iter().map(|bd| bd.block.header.number).collect::<Vec<_>>(),
            vec![10, 11, 12, 13]
        );
        assert_full_coverage(&calls, 10, 13);
    }
}
