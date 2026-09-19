//! Parent-hash continuity tracking for reorg observability.

use alloy::{primitives::B256, rpc::types::Header};

/// A detected chain discontinuity: a block that does not extend the previously
/// observed one, indicating a reorg deeper than the confirmation depth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Reorg {
    pub(crate) block_number: u64,
    pub(crate) parent_block_number: u64,
    pub(crate) expected_parent_hash: B256,
    pub(crate) actual_parent_hash: B256,
}

/// Tracks the last observed block to detect reorgs past the confirmation depth.
/// The first block observed after startup seeds the tip without a check,
/// unless the tip is restored from persistence via [`ReorgDetector::seeded`].
#[derive(Debug, Default)]
pub(crate) struct ReorgDetector {
    tip: Option<(u64, B256)>,
}

impl ReorgDetector {
    pub(crate) fn seeded(tip: Option<(u64, B256)>) -> Self {
        Self { tip }
    }
    /// Observes a header, returning the discontinuity if the block does not
    /// extend the previously observed one. Advances the tip regardless.
    pub(crate) fn observe(&mut self, header: &Header) -> Option<Reorg> {
        let reorg = self.tip.filter(|&(number, _)| header.number == number + 1).and_then(
            |(number, hash)| {
                (header.parent_hash != hash).then(|| Reorg {
                    block_number: header.number,
                    parent_block_number: number,
                    expected_parent_hash: hash,
                    actual_parent_hash: header.parent_hash,
                })
            },
        );
        self.tip = Some((header.number, header.hash));
        reorg
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::consensus;

    fn hash(byte: u8) -> B256 {
        B256::from([byte; 32])
    }

    fn header(number: u64, hash: B256, parent_hash: B256) -> Header {
        Header {
            hash,
            inner: consensus::Header { number, parent_hash, ..Default::default() },
            ..Default::default()
        }
    }

    #[test]
    fn first_observed_block_seeds_without_detection() {
        let mut detector = ReorgDetector::default();
        assert!(detector.observe(&header(100, hash(1), hash(0))).is_none());
    }

    #[test]
    fn continuous_chain_produces_no_detections() {
        let mut detector = ReorgDetector::default();
        assert!(detector.observe(&header(100, hash(1), hash(0))).is_none());
        assert!(detector.observe(&header(101, hash(2), hash(1))).is_none());
        assert!(detector.observe(&header(102, hash(3), hash(2))).is_none());
    }

    #[test]
    fn parent_hash_mismatch_is_detected_with_details() {
        let mut detector = ReorgDetector::default();
        detector.observe(&header(100, hash(1), hash(0)));

        let reorg = detector.observe(&header(101, hash(3), hash(2))).unwrap();

        assert_eq!(
            reorg,
            Reorg {
                block_number: 101,
                parent_block_number: 100,
                expected_parent_hash: hash(1),
                actual_parent_hash: hash(2),
            }
        );
    }

    #[test]
    fn number_gap_advances_tip_without_detection() {
        let mut detector = ReorgDetector::default();
        detector.observe(&header(100, hash(1), hash(0)));

        assert!(detector.observe(&header(105, hash(2), hash(9))).is_none());
        assert!(detector.observe(&header(106, hash(3), hash(2))).is_none());
    }

    #[test]
    fn seeded_tip_checks_the_first_observed_block() {
        let mut resumed = ReorgDetector::seeded(Some((100, hash(1))));
        assert!(resumed.observe(&header(101, hash(2), hash(1))).is_none());
        assert!(resumed.observe(&header(102, hash(4), hash(3))).is_some());
    }
}
