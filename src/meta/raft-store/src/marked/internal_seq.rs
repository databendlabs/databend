// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// Internal sequence number which is a tuple of seq number and a tombstone flag to track record freshness.
///
/// For kv map:
/// - Normal kv records: `seq` of internal_seq increments on updates.
/// - Tombstone kv records: `seq` of internal_seq is the max seq in state machine + tombstone flag.
///   The system max `seq` won't be updated until a new normal record is inserted.
///
/// For Expire index map:
/// - Normal expire records: internal_seq is max seq.
/// - Tombstone expire records: internal_seq is max seq + tombstone flag.
///
/// Tombstone-flagged internal_seq is always greater than non-flagged if seqs are equal,
/// because the tombstone can only be added after the normal one.
///
/// With such a design, the system seq increases only when a new normal record is inserted, ensuring compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct InternalSeq {
    pub(crate) seq: u64,
    pub(crate) tombstone: bool,
}

impl InternalSeq {
    pub fn normal(seq: u64) -> Self {
        Self {
            seq,
            tombstone: false,
        }
    }

    pub fn tombstone(seq: u64) -> Self {
        Self {
            seq,
            tombstone: true,
        }
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn is_tombstone(&self) -> bool {
        self.tombstone
    }
}

#[cfg(test)]
mod tests {
    use super::InternalSeq;

    #[test]
    fn test_ord() -> Result<(), anyhow::Error> {
        assert!(InternalSeq::normal(5) < InternalSeq::normal(6));
        assert!(InternalSeq::normal(7) > InternalSeq::normal(6));
        assert!(InternalSeq::normal(6) == InternalSeq::normal(6));

        assert!(InternalSeq::normal(6) < InternalSeq::tombstone(6));
        assert!(InternalSeq::normal(6) > InternalSeq::tombstone(5));

        Ok(())
    }
}
