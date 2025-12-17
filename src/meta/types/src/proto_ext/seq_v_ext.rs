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

use std::time::Duration;

use map_api::match_seq::MatchSeq;
use map_api::match_seq::MatchSeqExt;
use map_api::match_seq::errors::ConflictSeq;
use state_machine_api::KVMeta;
use state_machine_api::SeqV;

use crate::protobuf as pb;
use crate::time::flexible_timestamp_to_duration;

impl From<KVMeta> for pb::KvMeta {
    fn from(m: KVMeta) -> Self {
        Self {
            expire_at: m.expire_at,
            proposed_at_ms: m.proposed_at_ms,
        }
    }
}

impl From<pb::KvMeta> for KVMeta {
    fn from(m: pb::KvMeta) -> Self {
        Self {
            expire_at: m.expire_at,
            proposed_at_ms: m.proposed_at_ms,
        }
    }
}

impl pb::KvMeta {
    pub fn new(expire_at: Option<u64>, proposed_at_ms: Option<u64>) -> Self {
        Self {
            expire_at,
            proposed_at_ms,
        }
    }

    #[deprecated]
    pub fn new_expire(expire_at: u64) -> Self {
        Self {
            expire_at: Some(expire_at),
            proposed_at_ms: None,
        }
    }

    /// Returns true if two `KvMeta` instances are close to each other.
    ///
    /// If both have no expiration time, returns true.
    /// If only one has an expiration time, returns false.
    pub fn close_to(&self, other: &Self, tolerance: Duration) -> bool {
        let (a, b) = match (self.expire_at, other.expire_at) {
            (Some(a), Some(b)) => (a, b),
            (None, None) => return true,
            _ => return false,
        };

        let a = flexible_timestamp_to_duration(a);
        let b = flexible_timestamp_to_duration(b);

        let a = a.as_micros() as u64;
        let b = b.as_micros() as u64;

        let diff = a.abs_diff(b);
        diff <= tolerance.as_micros() as u64
    }
}

impl pb::SeqV {
    pub fn new(seq: u64, data: Vec<u8>) -> Self {
        Self {
            seq,
            data,
            meta: None,
        }
    }

    pub fn with_meta(seq: u64, meta: Option<pb::KvMeta>, data: Vec<u8>) -> Self {
        Self { seq, meta, data }
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn close_to(&self, other: &Self, tolerance: Duration) -> bool {
        self.seq == other.seq
            && self.data == other.data
            && match (&self.meta, &other.meta) {
                (Some(meta), Some(other_meta)) => meta.close_to(other_meta, tolerance),
                (None, None) => true,
                _ => false,
            }
    }
}

impl From<SeqV> for pb::SeqV {
    fn from(sv: SeqV) -> Self {
        Self {
            seq: sv.seq,
            data: sv.data,
            meta: sv.meta.map(pb::KvMeta::from),
        }
    }
}

impl From<pb::SeqV> for SeqV {
    /// Convert from protobuf SeqV to the native SeqV we defined.
    fn from(sv: pb::SeqV) -> Self {
        Self {
            seq: sv.seq,
            data: sv.data,
            meta: sv.meta.map(KVMeta::from),
        }
    }
}

impl MatchSeqExt<pb::SeqV> for MatchSeq {
    fn match_seq(&self, sv: &pb::SeqV) -> Result<(), ConflictSeq> {
        self.match_seq(&sv.seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_close_to() {
        let e1 = pb::SeqV::with_meta(
            1,
            Some(pb::KvMeta::new(Some(1723102819), None)),
            b"".to_vec(),
        );
        let e2 = pb::SeqV::with_meta(
            1,
            Some(pb::KvMeta::new(Some(1723102818), None)),
            b"".to_vec(),
        );

        assert!(e1.close_to(&e2, Duration::from_secs(1)));

        let e1 = pb::SeqV::with_meta(
            1,
            Some(pb::KvMeta::new(Some(1723102819), None)),
            b"".to_vec(),
        );
        let e2 = pb::SeqV::with_meta(
            1,
            Some(pb::KvMeta::new(Some(1_723_102_820_000), None)),
            b"".to_vec(),
        );

        assert!(e1.close_to(&e2, Duration::from_secs(1)));
    }
}
