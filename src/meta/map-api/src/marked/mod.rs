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

mod marked_impl;
mod seq_tombstone;

pub(crate) use seq_tombstone::SeqTombstone;

use crate::seq_value::SeqV;
use crate::seq_value::SeqValue;

/// A versioned value wrapper that can mark the value as deleted.
///
/// This `internal_seq` is used internally and is different from the seq in `SeqV`,
/// which is used by application.
/// A deleted tombstone also have `internal_seq`, while for an application, deleted entry has seq=0.
/// A normal entry(non-deleted) has a positive `seq` that is same as the corresponding `internal_seq`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Marked<M, T = Vec<u8>> {
    TombStone {
        internal_seq: u64,
    },
    Normal {
        internal_seq: u64,
        value: T,
        meta: Option<M>,
    },
}

impl<M, T> From<(u64, T, Option<M>)> for Marked<M, T> {
    fn from((seq, value, meta): (u64, T, Option<M>)) -> Self {
        assert_ne!(seq, 0);

        Marked::Normal {
            internal_seq: seq,
            value,
            meta,
        }
    }
}

impl<M, T> From<SeqV<M, T>> for Marked<M, T> {
    fn from(value: SeqV<M, T>) -> Self {
        Marked::new_with_meta(value.seq, value.data, value.meta)
    }
}

impl<M, T> SeqValue<M, T> for Marked<M, T> {
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

    fn into_value(self) -> Option<T> {
        match self {
            Marked::TombStone { internal_seq: _ } => None,
            Marked::Normal {
                internal_seq: _,
                value,
                meta: _,
            } => Some(value),
        }
    }

    fn meta(&self) -> Option<&M> {
        match self {
            Marked::TombStone { .. } => None,
            Marked::Normal { meta, .. } => meta.as_ref(),
        }
    }
}

impl<M, T> Marked<M, T> {
    pub const fn empty() -> Self {
        Marked::TombStone { internal_seq: 0 }
    }

    /// Return a key to determine which one of the values of the same key are the last inserted.
    pub fn order_key(&self) -> SeqTombstone {
        match self {
            Marked::TombStone { internal_seq: seq } => SeqTombstone::tombstone(*seq),
            Marked::Normal {
                internal_seq: seq, ..
            } => SeqTombstone::normal(*seq),
        }
    }

    pub fn unpack(self) -> Option<(T, Option<M>)> {
        match self {
            Marked::TombStone { internal_seq: _ } => None,
            Marked::Normal {
                internal_seq: _,
                value,
                meta,
            } => Some((value, meta)),
        }
    }

    pub fn unpack_ref(&self) -> Option<(&T, Option<&M>)> {
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
    pub fn max(a: Self, b: Self) -> Self {
        if a.order_key() > b.order_key() {
            a
        } else {
            b
        }
    }

    /// Return the one with the larger sequence number.
    // Not used, may be useful.
    #[allow(dead_code)]
    pub fn max_ref<'l>(a: &'l Self, b: &'l Self) -> &'l Self {
        if a.order_key() > b.order_key() {
            a
        } else {
            b
        }
    }

    pub fn new_tombstone(internal_seq: u64) -> Self {
        Marked::TombStone { internal_seq }
    }

    #[allow(dead_code)]
    pub fn new_normal(seq: u64, value: T) -> Self {
        Marked::Normal {
            internal_seq: seq,
            value,
            meta: None,
        }
    }

    pub fn new_with_meta(seq: u64, value: T, meta: Option<M>) -> Self {
        Marked::Normal {
            internal_seq: seq,
            value,
            meta,
        }
    }

    #[allow(dead_code)]
    pub fn with_meta(self, meta: Option<M>) -> Self {
        match self {
            Marked::TombStone { .. } => {
                unreachable!("Tombstone has no meta")
            }
            Marked::Normal {
                internal_seq,
                value,
                ..
            } => Marked::Normal {
                internal_seq,
                value,
                meta,
            },
        }
    }

    /// Return if the entry is neither a normal entry nor a tombstone.
    pub fn is_not_found(&self) -> bool {
        matches!(self, Marked::TombStone { internal_seq: 0 })
    }

    pub fn is_tombstone(&self) -> bool {
        matches!(self, Marked::TombStone { .. })
    }

    #[allow(dead_code)]
    pub(crate) fn is_normal(&self) -> bool {
        matches!(self, Marked::Normal { .. })
    }
}

impl<M, T> From<Marked<M, T>> for Option<SeqV<M, T>> {
    fn from(value: Marked<M, T>) -> Self {
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

#[cfg(test)]
mod tests {

    use super::Marked;

    #[test]
    fn test_marked_new() {
        let m = Marked::new_normal(1, "a");
        assert_eq!(
            Marked::Normal {
                internal_seq: 1,
                value: "a",
                meta: None
            },
            m
        );

        let m = m.with_meta(Some(20u64));

        assert_eq!(
            Marked::Normal {
                internal_seq: 1,
                value: "a",
                meta: Some(20u64)
            },
            m
        );

        let m = Marked::new_with_meta(2, "b", Some(30u64));

        assert_eq!(
            Marked::Normal {
                internal_seq: 2,
                value: "b",
                meta: Some(30u64)
            },
            m
        );

        let m: Marked<u32> = Marked::new_tombstone(3);
        assert_eq!(Marked::TombStone { internal_seq: 3 }, m);
    }
}
