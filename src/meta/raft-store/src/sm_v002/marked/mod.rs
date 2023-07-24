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

#[cfg(test)]
mod marked_test;

mod internal_seq;

use common_meta_types::KVMeta;
use common_meta_types::SeqV;
use common_meta_types::SeqValue;
pub(in crate::sm_v002) use internal_seq::InternalSeq;

use crate::state_machine::ExpireValue;

/// A versioned value wrapper that can mark the value as deleted.
///
/// This `internal_seq` is used internally and is different from the seq in `SeqV`,
/// which is used by application.
/// A deleted tombstone also have `internal_seq`, while for an application, deleted entry has seq=0.
/// A normal entry(non-deleted) has a positive `seq` that is same as the corresponding `internal_seq`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::sm_v002) enum Marked<T = Vec<u8>> {
    TombStone {
        internal_seq: u64,
    },
    Normal {
        internal_seq: u64,
        value: T,
        meta: Option<KVMeta>,
    },
}

impl<T> From<(u64, T, Option<KVMeta>)> for Marked<T> {
    fn from((seq, value, meta): (u64, T, Option<KVMeta>)) -> Self {
        assert_ne!(seq, 0);

        Marked::Normal {
            internal_seq: seq,
            value,
            meta,
        }
    }
}

impl<T> From<SeqV<T>> for Marked<T> {
    fn from(value: SeqV<T>) -> Self {
        Marked::new_normal(value.seq, value.data, value.meta)
    }
}

impl<T> SeqValue<T> for Marked<T> {
    fn seq(&self) -> u64 {
        match self {
            Marked::TombStone { internal_seq: _ } => 0,
            Marked::Normal {
                internal_seq: seq, ..
            } => *seq,
        }
    }

    fn value(&self) -> Option<&T> {
        match self {
            Marked::TombStone { internal_seq: _ } => None,
            Marked::Normal {
                internal_seq: _,
                value,
                meta: _,
            } => Some(value),
        }
    }

    fn meta(&self) -> Option<&KVMeta> {
        match self {
            Marked::TombStone { .. } => None,
            Marked::Normal { meta, .. } => meta.as_ref(),
        }
    }
}

impl<T> Marked<T> {
    pub const fn empty() -> Self {
        Marked::TombStone { internal_seq: 0 }
    }

    /// Get internal sequence number. Both None and Normal have sequence number.
    pub(in crate::sm_v002) fn internal_seq(&self) -> InternalSeq {
        match self {
            Marked::TombStone { internal_seq: seq } => InternalSeq::tombstone(*seq),
            Marked::Normal {
                internal_seq: seq, ..
            } => InternalSeq::normal(*seq),
        }
    }

    pub fn unpack(&self) -> Option<(&T, Option<&KVMeta>)> {
        match self {
            Marked::TombStone { internal_seq: _ } => None,
            Marked::Normal {
                internal_seq: _,
                value,
                meta,
            } => Some((value, meta.as_ref())),
        }
    }

    /// Return the one with the larger sequence number.
    pub fn max<'l>(a: &'l Self, b: &'l Self) -> &'l Self {
        if a.internal_seq() > b.internal_seq() {
            a
        } else {
            b
        }
    }

    pub fn new_tomb_stone(internal_seq: u64) -> Self {
        Marked::TombStone { internal_seq }
    }

    pub fn new_normal(seq: u64, value: T, meta: Option<KVMeta>) -> Self {
        Marked::Normal {
            internal_seq: seq,
            value,
            meta,
        }
    }

    /// Not a normal entry or a tombstone.
    pub fn is_not_found(&self) -> bool {
        matches!(self, Marked::TombStone {
            internal_seq: 0,
            ..
        })
    }

    pub fn is_tomb_stone(&self) -> bool {
        matches!(self, Marked::TombStone { .. })
    }

    pub(crate) fn is_normal(&self) -> bool {
        matches!(self, Marked::Normal { .. })
    }
}

impl<T> From<Marked<T>> for Option<SeqV<T>> {
    fn from(value: Marked<T>) -> Self {
        match value {
            Marked::TombStone { internal_seq: _ } => None,
            Marked::Normal {
                internal_seq: seq,
                value,
                meta,
            } => Some(SeqV::with_meta(seq, meta, value)),
        }
    }
}

impl From<ExpireValue> for Marked<String> {
    fn from(value: ExpireValue) -> Self {
        Marked::new_normal(value.seq, value.key, None)
    }
}

impl From<Marked<String>> for Option<ExpireValue> {
    fn from(value: Marked<String>) -> Self {
        match value {
            Marked::TombStone { internal_seq: _ } => None,
            Marked::Normal {
                internal_seq: seq,
                value,
                meta: _,
            } => Some(ExpireValue::new(value, seq)),
        }
    }
}
