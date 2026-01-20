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

use std::hint::likely;
use std::num::NonZeroU64;
use std::ptr::NonNull;

use databend_common_ast::parser::token::GROUP;

use crate::ProbeState;
use crate::aggregate::hash_index::TableAdapter;
use crate::aggregate::row_ptr::RowPtr;

/// Portions of this file are derived from excellent `hashbrown` crate:
/// https://github.com/rust-lang/hashbrown/

/// Single tag in a control group.
#[derive(Copy, Clone, PartialEq, Eq)]
#[repr(transparent)]
struct Tag(u8);
impl Tag {
    /// Control tag value for an empty bucket.
    const EMPTY: Tag = Tag(0b1111_1111);

    /// Creates a control tag representing a full bucket with the given hash.
    #[inline]
    const fn full(hash: u64) -> Tag {
        let top7 = hash >> (8 * 8 - 7);
        Tag((top7 & 0x7f) as u8) // truncation
    }
}

const BITMASK_ITER_MASK: u64 = 0x8080_8080_8080_8080;

const BITMASK_STRIDE: usize = 8;

type NonZeroBitMaskWord = NonZeroU64;

#[derive(Copy, Clone)]
struct BitMask(u64);

impl BitMask {
    #[inline]
    #[must_use]
    fn remove_lowest_bit(self) -> Self {
        BitMask(self.0 & (self.0 - 1))
    }

    #[inline]
    fn nonzero_trailing_zeros(nonzero: NonZeroBitMaskWord) -> usize {
        if cfg!(target_arch = "arm") && BITMASK_STRIDE % 8 == 0 {
            // SAFETY: A byte-swapped non-zero value is still non-zero.
            let swapped = unsafe { NonZeroBitMaskWord::new_unchecked(nonzero.get().swap_bytes()) };
            swapped.leading_zeros() as usize / BITMASK_STRIDE
        } else {
            nonzero.trailing_zeros() as usize / BITMASK_STRIDE
        }
    }

    fn lowest_set_bit(self) -> Option<usize> {
        if let Some(nonzero) = NonZeroBitMaskWord::new(self.0) {
            Some(Self::nonzero_trailing_zeros(nonzero))
        } else {
            None
        }
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
struct BitMaskIter(BitMask);

impl Iterator for BitMaskIter {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        let bit = self.0.lowest_set_bit()?;
        self.0 = self.0.remove_lowest_bit();
        Some(bit)
    }
}

/// Helper function to replicate a tag across a `GroupWord`.
#[inline]
fn repeat(tag: Tag) -> u64 {
    u64::from_ne_bytes([tag.0; Group::WIDTH])
}

#[derive(Copy, Clone)]
struct Group(u64);

impl Group {
    /// Number of bytes in the group.
    const WIDTH: usize = 8;

    fn match_tag(self, tag: Tag) -> BitMask {
        // This algorithm is derived from
        // https://graphics.stanford.edu/~seander/bithacks.html##ValueInWord
        let cmp = self.0 ^ repeat(tag);
        BitMask((cmp.wrapping_sub(repeat(Tag(0x01))) & !cmp & repeat(Tag(0x80))).to_le())
    }

    #[inline]
    pub(crate) fn match_empty(self) -> BitMask {
        BitMask((self.0 & repeat(Tag(0x80))).to_le())
    }

    unsafe fn load(ctrls: &Vec<Tag>, index: usize) -> Self {
        unsafe { Group((ctrls.as_ptr().add(index) as *const u64).read_unaligned()) }
    }
}

#[derive(Clone)]
struct ProbeSeq {
    pos: usize,
    stride: usize,
}

impl ProbeSeq {
    #[inline]
    fn move_next(&mut self, bucket_mask: usize) {
        // We should have found an empty bucket by now and ended the probe.
        debug_assert!(
            self.stride <= bucket_mask,
            "Went past end of probe sequence"
        );

        self.stride += Group::WIDTH;
        self.pos += self.stride;
        self.pos &= bucket_mask;
    }
}

pub struct NewHashIndex {
    ctrls: Vec<Tag>,
    pointers: Vec<RowPtr>,
    capacity: usize,
    bucket_mask: usize,
}

impl NewHashIndex {
    pub fn with_capacity(capacity: usize) -> Self {
        debug_assert!(capacity.is_power_of_two());
        // avoid handling: SMALL TABLE NASTY CORNER CASE
        // This can happen for small (n < WIDTH) tables
        debug_assert!(capacity >= Group::WIDTH);
    }
}

impl NewHashIndex {
    #[inline]
    fn probe_seq(&self, hash: u64) -> ProbeSeq {
        ProbeSeq {
            pos: hash as usize & self.bucket_mask,
            stride: 0,
        }
    }

    #[inline]
    fn find_insert_index_in_group(&self, group: &Group, pos: &usize) -> Option<usize> {
        let bit = group.match_empty().lowest_set_bit();

        if likely(bit.is_some()) {
            Some((pos + bit.unwrap()) & self.bucket_mask)
        } else {
            None
        }
    }

    pub fn find_or_insert(&mut self, hash: u64) -> (usize, bool) {
        let mut insert_index = None;
        let tag_hash = Tag::full(hash);
        let mut probe_seq = self.probe_seq(hash);
        loop {
            let group = unsafe { Group::load(&self.ctrls, probe_seq.pos) };
            for bit in group.match_tag(tag_hash) {
                let index = (probe_seq.pos + bit) & self.bucket_mask;
                return (index, false);
            }
            insert_index = self.find_insert_index_in_group(&group, &probe_seq.pos);
            if insert_index.is_some() {
                return (insert_index.unwrap(), true);
            }
            probe_seq.move_next(self.bucket_mask);
        }
    }

    pub fn probe_slot(&mut self, hash: u64) -> usize {
        todo!()
    }

    pub fn probe_and_create(
        &mut self,
        state: &mut ProbeState,
        row_count: usize,
        mut adapter: impl TableAdapter,
    ) -> usize {
        todo!()
    }
}
