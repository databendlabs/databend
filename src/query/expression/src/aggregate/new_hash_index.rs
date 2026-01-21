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
use crate::aggregate::BATCH_SIZE;
use crate::aggregate::hash_index::HashIndexOps;
use crate::aggregate::hash_index::TableAdapter;
use crate::aggregate::new_hash_index::group::Group;
use crate::aggregate::row_ptr::RowPtr;

// Portions of this file are derived from excellent `hashbrown` crate

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

mod group {

    #[cfg(not(all(
        target_arch = "aarch64",
        target_feature = "neon",
        target_endian = "little",
        not(miri),
    )))]
    pub use generic::Group;
    #[cfg(all(
        target_arch = "aarch64",
        target_feature = "neon",
        // NEON intrinsics are currently broken on big-endian targets.
        // See https://github.com/rust-lang/stdarch/issues/1484.
        target_endian = "little",
        not(miri),
    ))]
    pub use neon::Group;

    mod generic {
        use crate::aggregate::new_hash_index::BitMask;
        use crate::aggregate::new_hash_index::Tag;
        use crate::aggregate::new_hash_index::repeat;

        #[derive(Copy, Clone)]
        pub struct Group(u64);

        impl Group {
            /// Number of bytes in the group.
            pub const WIDTH: usize = 8;

            #[inline]
            pub fn match_tag(self, tag: Tag) -> BitMask {
                // This algorithm is derived from
                // https://graphics.stanford.edu/~seander/bithacks.html##ValueInWord
                let cmp = self.0 ^ repeat(tag);
                BitMask((cmp.wrapping_sub(repeat(Tag(0x01))) & !cmp & repeat(Tag(0x80))).to_le())
            }

            #[inline]
            pub fn match_empty(self) -> BitMask {
                BitMask((self.0 & repeat(Tag(0x80))).to_le())
            }

            #[inline]
            pub unsafe fn load(ctrls: &[Tag], index: usize) -> Self {
                unsafe { Group((ctrls.as_ptr().add(index) as *const u64).read_unaligned()) }
            }
        }
    }

    mod neon {
        use core::arch::aarch64 as neon;
        use std::mem;

        use crate::aggregate::new_hash_index::BitMask;
        use crate::aggregate::new_hash_index::Tag;

        #[derive(Copy, Clone)]
        pub struct Group(neon::uint8x8_t);

        impl Group {
            /// Number of bytes in the group.
            pub const WIDTH: usize = mem::size_of::<Self>();

            #[inline]
            pub fn match_tag(self, tag: Tag) -> BitMask {
                unsafe {
                    let cmp = neon::vceq_u8(self.0, neon::vdup_n_u8(tag.0));
                    BitMask(neon::vget_lane_u64(neon::vreinterpret_u64_u8(cmp), 0))
                }
            }

            #[inline]
            pub fn match_empty(self) -> BitMask {
                unsafe {
                    let cmp = neon::vcltz_s8(neon::vreinterpret_s8_u8(self.0));
                    BitMask(neon::vget_lane_u64(neon::vreinterpret_u64_u8(cmp), 0))
                }
            }

            #[inline]
            pub unsafe fn load(ctrls: &[Tag], index: usize) -> Self {
                unsafe { Group(neon::vld1_u8(ctrls.as_ptr().add(index) as *const u8)) }
            }
        }
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
    count: usize,
}

impl NewHashIndex {
    pub fn with_capacity(capacity: usize) -> Self {
        debug_assert!(capacity.is_power_of_two());
        // avoid handling: SMALL TABLE NASTY CORNER CASE
        // This can happen for small (n < WIDTH) tables
        debug_assert!(capacity >= Group::WIDTH);
        let bucket_mask = capacity - 1;
        let ctrls = vec![Tag::EMPTY; capacity + Group::WIDTH];
        let pointers = vec![RowPtr::null(); capacity];
        Self {
            ctrls,
            pointers,
            capacity,
            bucket_mask,
            count: 0,
        }
    }

    pub fn dummy() -> Self {
        Self {
            ctrls: vec![],
            pointers: vec![],
            capacity: 0,
            bucket_mask: 0,
            count: 0,
        }
    }

    pub fn rebuild_from_iter<I>(capacity: usize, iter: I) -> Self
    where I: IntoIterator<Item = (u64, RowPtr)> {
        let mut hash_index = NewHashIndex::with_capacity(capacity);
        for (hash, row_ptr) in iter {
            let slot = hash_index.probe_empty_and_set_ctrl(hash);
            hash_index.pointers[slot] = row_ptr;
            hash_index.count += 1;
        }
        hash_index
    }
}

impl NewHashIndex {
    #[inline]
    fn ctrl(&mut self, index: usize) -> *mut Tag {
        debug_assert!(index < self.ctrls.len());
        unsafe { self.ctrls.as_mut_ptr().add(index) }
    }

    #[inline]
    fn set_ctrl(&mut self, index: usize, tag: Tag) {
        // This is the same as `(index.wrapping_sub(Group::WIDTH)) % self.num_buckets() + Group::WIDTH`
        // because the number of buckets is a power of two, and `self.bucket_mask = self.num_buckets() - 1`.
        let index2 = ((index.wrapping_sub(Group::WIDTH)) & self.bucket_mask) + Group::WIDTH;

        unsafe {
            *self.ctrl(index) = tag;
            *self.ctrl(index2) = tag;
        }
    }

    #[inline]
    fn h1(&self, hash: u64) -> usize {
        hash as usize & self.bucket_mask
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

    pub fn find_or_insert(&mut self, mut pos: usize, hash: u64) -> (usize, bool) {
        let mut insert_index = None;
        let tag_hash = Tag::full(hash);
        loop {
            let group = unsafe { Group::load(&self.ctrls, pos) };
            for bit in group.match_tag(tag_hash) {
                let index = (pos + bit) & self.bucket_mask;
                return (index, false);
            }
            insert_index = self.find_insert_index_in_group(&group, &pos);
            if let Some(index) = insert_index {
                self.set_ctrl(index, tag_hash);
                return (index, true);
            }

            pos = (pos + Group::WIDTH) & self.bucket_mask;
        }
    }

    pub fn probe_empty_and_set_ctrl(&mut self, hash: u64) -> usize {
        let mut pos = self.h1(hash);
        loop {
            let group = unsafe { Group::load(&self.ctrls, pos) };
            if let Some(index) = self.find_insert_index_in_group(&group, &pos) {
                self.set_ctrl(index, Tag::full(hash));
                return index;
            }
            pos = (pos + Group::WIDTH) & self.bucket_mask;
        }
    }

    pub fn probe_and_create(
        &mut self,
        state: &mut ProbeState,
        row_count: usize,
        adapter: &mut dyn TableAdapter,
    ) -> usize {
        debug_assert!(self.capacity > 0);
        for (i, row) in state.no_match_vector[..row_count].iter_mut().enumerate() {
            *row = i.into();
            state.slots[i] = self.h1(state.group_hashes[i]);
        }

        let mut new_group_count = 0;
        let mut remaining_entries = row_count;

        while remaining_entries > 0 {
            let mut new_entry_count = 0;
            let mut need_compare_count = 0;
            let mut no_match_count = 0;

            for row in state.no_match_vector[..remaining_entries].iter().copied() {
                let slot = &mut state.slots[row];
                let (slot, is_new) = self.find_or_insert(*slot, state.group_hashes[row]);
                state.slots[row] = slot;

                if is_new {
                    state.empty_vector[new_entry_count] = row;
                    new_entry_count += 1;
                } else {
                    state.group_compare_vector[need_compare_count] = row;
                    need_compare_count += 1;
                }
            }

            if new_entry_count != 0 {
                new_group_count += new_entry_count;

                adapter.append_rows(state, new_entry_count);

                for row in state.empty_vector[..new_entry_count].iter().copied() {
                    let slot = state.slots[row];
                    self.set_ctrl(slot, Tag::full(state.group_hashes[row]));
                    self.pointers[slot] = state.addresses[row];
                }
            }

            if need_compare_count > 0 {
                for row in state.group_compare_vector[..need_compare_count]
                    .iter()
                    .copied()
                {
                    let slot = state.slots[row];
                    state.addresses[row] = self.pointers[slot];
                }

                no_match_count = adapter.compare(state, need_compare_count, no_match_count);
            }

            for row in state.no_match_vector[..no_match_count].iter().copied() {
                let slot = state.slots[row];
                state.slots[row] = (slot + 1) & self.bucket_mask;
            }

            remaining_entries = no_match_count;
        }

        self.count += new_group_count;
        new_group_count
    }
}

impl HashIndexOps for NewHashIndex {
    fn capacity(&self) -> usize {
        self.capacity
    }

    fn count(&self) -> usize {
        self.count
    }

    fn resize_threshold(&self) -> usize {
        (self.capacity as f64 / super::LOAD_FACTOR) as usize
    }

    fn allocated_bytes(&self) -> usize {
        self.ctrls.len() * std::mem::size_of::<Tag>()
            + self.pointers.len() * std::mem::size_of::<RowPtr>()
    }

    fn reset(&mut self) {
        if self.capacity == 0 {
            return;
        }
        self.count = 0;
        self.ctrls.fill(Tag::EMPTY);
        self.pointers.fill(RowPtr::null());
    }

    fn probe_and_create(
        &mut self,
        state: &mut ProbeState,
        row_count: usize,
        adapter: &mut dyn TableAdapter,
    ) -> usize {
        NewHashIndex::probe_and_create(self, state, row_count, adapter)
    }
}
