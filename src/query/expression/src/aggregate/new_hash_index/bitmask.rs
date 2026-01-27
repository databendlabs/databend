// Copyright (c) 2016 Amanieu d'Antras
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

use std::num::NonZeroU64;

use crate::aggregate::new_hash_index::group::Group;

const BITMASK_ITER_MASK: u64 = 0x8080_8080_8080_8080;

const BITMASK_STRIDE: usize = 8;

type NonZeroBitMaskWord = NonZeroU64;

#[derive(Copy, Clone)]
pub(super) struct BitMask(pub(super) u64);

impl BitMask {
    #[inline]
    #[must_use]
    pub(super) fn remove_lowest_bit(self) -> Self {
        BitMask(self.0 & (self.0 - 1))
    }

    #[inline]
    pub(super) fn nonzero_trailing_zeros(nonzero: NonZeroBitMaskWord) -> usize {
        if cfg!(target_arch = "arm") && BITMASK_STRIDE % 8 == 0 {
            // SAFETY: A byte-swapped non-zero value is still non-zero.
            let swapped = unsafe { NonZeroBitMaskWord::new_unchecked(nonzero.get().swap_bytes()) };
            swapped.leading_zeros() as usize / BITMASK_STRIDE
        } else {
            nonzero.trailing_zeros() as usize / BITMASK_STRIDE
        }
    }

    pub(super) fn lowest_set_bit(self) -> Option<usize> {
        NonZeroBitMaskWord::new(self.0).map(Self::nonzero_trailing_zeros)
    }
}

impl IntoIterator for BitMask {
    type Item = usize;
    type IntoIter = BitMaskIter;

    #[inline]
    fn into_iter(self) -> BitMaskIter {
        // A BitMask only requires each element (group of bits) to be non-zero.
        // However for iteration we need each element to only contain 1 bit.
        BitMaskIter(BitMask(self.0 & BITMASK_ITER_MASK))
    }
}

/// Iterator over the contents of a `BitMask`, returning the indices of set
/// bits.
#[derive(Clone)]
pub(super) struct BitMaskIter(BitMask);

impl Iterator for BitMaskIter {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        let bit = self.0.lowest_set_bit()?;
        self.0 = self.0.remove_lowest_bit();
        Some(bit)
    }
}

/// Single tag in a control group.
#[derive(Copy, Clone, PartialEq, Eq)]
#[repr(transparent)]
pub(super) struct Tag(pub(super) u8);
impl Tag {
    /// Control tag value for an empty bucket.
    pub(super) const EMPTY: Tag = Tag(0b1111_1111);

    /// Creates a control tag representing a full bucket with the given hash.
    #[inline]
    pub(super) const fn full(hash: u64) -> Tag {
        let top7 = hash >> (8 * 8 - 7);
        Tag((top7 & 0x7f) as u8) // truncation
    }

    pub(super) fn is_empty(&self) -> bool {
        *self == Tag::EMPTY
    }
}
/// Helper function to replicate a tag across a `GroupWord`.
#[inline]
pub(super) fn repeat(tag: Tag) -> u64 {
    u64::from_ne_bytes([tag.0; Group::WIDTH])
}
