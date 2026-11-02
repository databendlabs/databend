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

use std::intrinsics::prefetch_read_data;
use std::mem::size_of;
use std::simd::cmp::SimdPartialEq;
use std::simd::u8x16;

use crate::aggregate::HASH_INDEX_LOAD_FACTOR;
use crate::aggregate::ProbeState;
use crate::aggregate::hash_index_adapter::TableAdapter;

/// The id of a row as a hash index slot keeps it: 4 bytes while the ids of the table fit, 8 bytes
/// otherwise, see `RowIdCodec`.
pub(in crate::aggregate) trait RowId: Copy {
    const ZERO: Self;

    fn from_id(id: u64) -> Self;

    fn id(self) -> u64;
}

impl RowId for u32 {
    const ZERO: Self = 0;

    #[inline]
    fn from_id(id: u64) -> Self {
        // A truncated id would name another row and mix two groups. The table switches to wide
        // ids before this can happen, so this is a check that is never taken.
        assert!(
            id <= u32::MAX as u64,
            "row id {id} does not fit a compact index"
        );
        id as u32
    }

    #[inline]
    fn id(self) -> u64 {
        self as u64
    }
}

impl RowId for u64 {
    const ZERO: Self = 0;

    #[inline]
    fn from_id(id: u64) -> Self {
        id
    }

    #[inline]
    fn id(self) -> u64 {
        self
    }
}

/// The hash index of an aggregate hash table. Its slots keep the ids of their rows, not their
/// addresses: 4 bytes each, or 8 bytes once a table has more rows than 4-byte ids can name. The
/// width is chosen when the index is built, see `AggregateHashTable::resize`. The slots are laid
/// out in chunks, see `RowIdIndex`.
pub(in crate::aggregate) enum HashIndex {
    Compact(RowIdIndex<u32>),
    Wide(RowIdIndex<u64>),
}

impl HashIndex {
    pub fn with_capacity(capacity: usize) -> Self {
        HashIndex::Compact(RowIdIndex::with_capacity(capacity))
    }

    pub fn wide_with_capacity(capacity: usize) -> Self {
        HashIndex::Wide(RowIdIndex::with_capacity(capacity))
    }

    pub fn dummy() -> Self {
        HashIndex::Compact(RowIdIndex::dummy())
    }

    #[cfg(test)]
    pub fn is_wide(&self) -> bool {
        matches!(self, HashIndex::Wide(_))
    }

    pub fn capacity(&self) -> usize {
        match self {
            HashIndex::Compact(index) => index.capacity,
            HashIndex::Wide(index) => index.capacity,
        }
    }

    pub fn count(&self) -> usize {
        match self {
            HashIndex::Compact(index) => index.count,
            HashIndex::Wide(index) => index.count,
        }
    }

    pub fn resize_threshold(&self) -> usize {
        Self::threshold_for_capacity(self.capacity())
    }

    /// The slots of an index asked for at least `capacity` slots: whole chunks, a power of two.
    pub fn slots_for(capacity: usize) -> usize {
        capacity.div_ceil(CHUNK_SLOTS).next_power_of_two() * CHUNK_SLOTS
    }

    /// The number of groups an index of `capacity` slots holds before it grows.
    pub fn threshold_for_capacity(capacity: usize) -> usize {
        (capacity as f64 / HASH_INDEX_LOAD_FACTOR) as usize
    }

    pub fn allocated_bytes(&self) -> usize {
        match self {
            HashIndex::Compact(index) => index.allocated_bytes(),
            HashIndex::Wide(index) => index.allocated_bytes(),
        }
    }

    pub fn reset(&mut self) {
        match self {
            HashIndex::Compact(index) => index.reset(),
            HashIndex::Wide(index) => index.reset(),
        }
    }

    /// Set a free slot for a row known to be absent, while the index is rebuilt.
    pub fn probe_slot_and_set(&mut self, hash: u64, row_id: u64) {
        match self {
            HashIndex::Compact(index) => index.probe_slot_and_set(hash, row_id),
            HashIndex::Wide(index) => index.probe_slot_and_set(hash, row_id),
        }
    }

    pub fn probe_and_create(
        &mut self,
        state: &mut ProbeState,
        row_count: usize,
        adapter: &mut dyn TableAdapter,
    ) -> usize {
        match self {
            HashIndex::Compact(index) => index.probe_and_create(state, row_count, adapter),
            HashIndex::Wide(index) => index.probe_and_create(state, row_count, adapter),
        }
    }
}

/// The tag of a slot: the top 7 bits of the hash of its group, or `EMPTY`.
#[derive(Copy, Clone, PartialEq, Eq)]
#[repr(transparent)]
struct Tag(u8);

impl Tag {
    const EMPTY: Tag = Tag(0xFF);
    /// The lanes of a chunk past its slots: never a full tag (< 0x80) nor `EMPTY`.
    const PAD: Tag = Tag(0xFE);

    #[inline]
    const fn full(hash: u64) -> Tag {
        Tag((hash >> 57) as u8)
    }
}

/// Slots of a chunk.
const CHUNK_SLOTS: usize = 12;
/// Prefetch the chunks of the rows this far ahead, once the index outgrows the caches.
const PREFETCH_DISTANCE: usize = 16;
const PREFETCH_MIN_BYTES: usize = 1 << 20;

/// The tags of 12 slots, then their row ids: one probe reads one cache line with 4-byte ids
/// (16 + 12 * 4 = 64 bytes), where separate tag and id arrays take two lines and two pages.
#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct Chunk<I> {
    tags: [Tag; 16],
    ids: [I; CHUNK_SLOTS],
}

impl<I: RowId> Chunk<I> {
    const EMPTY: Chunk<I> = {
        let mut tags = [Tag::PAD; 16];
        let mut lane = 0;
        while lane < CHUNK_SLOTS {
            tags[lane] = Tag::EMPTY;
            lane += 1;
        }
        Chunk {
            tags,
            ids: [I::ZERO; CHUNK_SLOTS],
        }
    };

    /// Lanes from `from` whose tag is `tag`, one bit per lane.
    #[inline]
    fn match_tag(&self, tag: Tag, from: usize) -> u64 {
        let tags = u8x16::from_array(self.tags.map(|t| t.0));
        tags.simd_eq(u8x16::splat(tag.0)).to_bitmask() & (!0 << from)
    }
}

/// A hash index whose slots keep row ids of type `I`, in chunks of `CHUNK_SLOTS` slots. A slot
/// is numbered `chunk * 16 + lane`; probing starts at lane 0 of the chunk the hash selects and
/// moves on lane by lane, then chunk by chunk.
pub(in crate::aggregate) struct RowIdIndex<I> {
    chunks: Vec<Chunk<I>>,
    capacity: usize,
    chunk_mask: usize,
    count: usize,
}

impl<I: RowId> RowIdIndex<I> {
    /// An index of at least `capacity` slots.
    pub fn with_capacity(capacity: usize) -> Self {
        let chunks = HashIndex::slots_for(capacity) / CHUNK_SLOTS;
        Self {
            chunks: vec![Chunk::EMPTY; chunks],
            capacity: chunks * CHUNK_SLOTS,
            chunk_mask: chunks - 1,
            count: 0,
        }
    }

    pub fn dummy() -> Self {
        Self {
            chunks: vec![],
            capacity: 0,
            chunk_mask: 0,
            count: 0,
        }
    }

    #[inline]
    fn first_slot(&self, hash: u64) -> usize {
        (hash as usize & self.chunk_mask) << 4
    }

    #[inline]
    fn next_slot(&self, slot: usize) -> usize {
        if (slot & 15) + 1 < CHUNK_SLOTS {
            slot + 1
        } else {
            (((slot >> 4) + 1) & self.chunk_mask) << 4
        }
    }

    #[inline]
    fn id(&self, slot: usize) -> I {
        self.chunks[slot >> 4].ids[slot & 15]
    }

    #[inline]
    fn set_id(&mut self, slot: usize, id: I) {
        self.chunks[slot >> 4].ids[slot & 15] = id;
    }

    /// The first slot from `slot` that holds the tag of `hash`, or else an empty one, which
    /// takes the tag.
    #[inline]
    fn find_or_insert(&mut self, slot: usize, hash: u64) -> (usize, bool) {
        let tag = Tag::full(hash);
        let mut chunk = slot >> 4;
        let mut from = slot & 15;
        loop {
            let c = &mut self.chunks[chunk];
            let matched = c.match_tag(tag, from);
            if matched != 0 {
                return ((chunk << 4) + matched.trailing_zeros() as usize, false);
            }
            let empty = c.match_tag(Tag::EMPTY, from);
            if empty != 0 {
                let lane = empty.trailing_zeros() as usize;
                c.tags[lane] = tag;
                return ((chunk << 4) + lane, true);
            }
            chunk = (chunk + 1) & self.chunk_mask;
            from = 0;
        }
    }

    pub(in crate::aggregate) fn probe_and_create(
        &mut self,
        state: &mut ProbeState,
        row_count: usize,
        adapter: &mut dyn TableAdapter,
    ) -> usize {
        debug_assert!(self.capacity > 0);
        for (i, row) in state.no_match_vector[..row_count].iter_mut().enumerate() {
            *row = i.into();
            state.slots[i] = self.first_slot(state.group_hashes[i]);
        }
        // Each row starts at a random chunk: once the index is out of the caches, fetch the chunks
        // of the rows ahead while probing the current one.
        let mut prefetch = self.allocated_bytes() >= PREFETCH_MIN_BYTES;

        let mut new_group_count = 0;
        let mut remaining_entries = row_count;

        while remaining_entries > 0 {
            let mut new_entry_count = 0;
            let mut need_compare_count = 0;
            let mut no_match_count = 0;

            for i in 0..remaining_entries {
                if prefetch && let Some(ahead) = state.no_match_vector.get(i + PREFETCH_DISTANCE) {
                    let chunk = state.slots[*ahead] >> 4;
                    prefetch_read_data::<_, 3>(self.chunks[..].as_ptr().wrapping_add(chunk));
                }
                let row = state.no_match_vector[i];
                let (slot, is_new) = self.find_or_insert(state.slots[row], state.group_hashes[row]);
                state.slots[row] = slot;

                if is_new {
                    state.empty_vector[new_entry_count] = row;
                    new_entry_count += 1;
                } else {
                    state.group_compare_vector[need_compare_count] = row;
                    need_compare_count += 1;
                }
            }
            // Mismatched rows go on in the chunk they reached.
            prefetch = false;

            if new_entry_count != 0 {
                new_group_count += new_entry_count;

                adapter.append_rows(state, new_entry_count);

                for row in state.empty_vector[..new_entry_count].iter().copied() {
                    self.set_id(state.slots[row], I::from_id(state.row_ids[row]));
                }
            }

            if need_compare_count > 0 {
                for row in state.group_compare_vector[..need_compare_count]
                    .iter()
                    .copied()
                {
                    state.addresses[row] = adapter.row_ptr(self.id(state.slots[row]).id());
                }

                no_match_count = adapter.compare(state, need_compare_count, no_match_count);
            }

            for row in state.no_match_vector[..no_match_count].iter().copied() {
                state.slots[row] = self.next_slot(state.slots[row]);
            }

            remaining_entries = no_match_count;
        }

        self.count += new_group_count;
        new_group_count
    }

    pub fn allocated_bytes(&self) -> usize {
        self.chunks.len() * size_of::<Chunk<I>>()
    }

    pub fn reset(&mut self) {
        self.count = 0;
        self.chunks.fill(Chunk::EMPTY);
    }

    /// Put a row known to be absent into the first empty slot of its probe sequence.
    pub fn probe_slot_and_set(&mut self, hash: u64, row_id: u64) {
        let mut chunk = hash as usize & self.chunk_mask;
        loop {
            let c = &mut self.chunks[chunk];
            let empty = c.match_tag(Tag::EMPTY, 0);
            if empty != 0 {
                let lane = empty.trailing_zeros() as usize;
                c.tags[lane] = Tag::full(hash);
                c.ids[lane] = I::from_id(row_id);
                self.count += 1;
                return;
            }
            chunk = (chunk + 1) & self.chunk_mask;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::aggregate::row_ptr::RowPtr;

    struct TestTableAdapter {
        incoming: Vec<(u64, u64)>,     // (key, hash)
        payload: Vec<(u64, u64, u64)>, // (key, hash, value)
        pin_data: Box<[u8]>,
    }

    impl TestTableAdapter {
        fn new(incoming: Vec<(u64, u64)>, payload: Vec<(u64, u64, u64)>) -> Self {
            Self {
                incoming,
                payload,
                pin_data: vec![0; 1000].into(),
            }
        }

        fn init_state(&self) -> ProbeState {
            let mut state = ProbeState {
                row_count: self.incoming.len(),
                ..Default::default()
            };

            for (i, (_, hash)) in self.incoming.iter().enumerate() {
                state.group_hashes[i] = *hash
            }

            state
        }

        fn init_hash_index<I: RowId>(&self, hash_index: &mut RowIdIndex<I>) {
            for (i, (_, hash, _)) in self.payload.iter().copied().enumerate() {
                hash_index.probe_slot_and_set(hash, i as u64);
            }
        }

        fn get_row_ptr(&self, id: usize) -> RowPtr {
            RowPtr::new(unsafe { self.pin_data.as_ptr().add(id) })
        }

        fn get_payload(&self, row_ptr: RowPtr) -> (u64, u64, u64) {
            let index = row_ptr.as_ptr() as usize - self.pin_data.as_ptr() as usize;
            self.payload[index]
        }
    }

    impl TableAdapter for TestTableAdapter {
        fn append_rows(&mut self, state: &mut ProbeState, new_entry_count: usize) {
            for row in state.empty_vector[..new_entry_count].iter() {
                let (key, hash) = self.incoming[*row];
                let value = key + 20;

                // The id of a row is its position in the payload.
                let id = self.payload.len();
                self.payload.push((key, hash, value));
                state.addresses[*row] = self.get_row_ptr(id);
                state.row_ids[*row] = id as u64;
            }
        }

        fn row_ptr(&self, row_id: u64) -> RowPtr {
            self.get_row_ptr(row_id as usize)
        }

        fn compare(
            &mut self,
            state: &mut ProbeState,
            need_compare_count: usize,
            mut no_match_count: usize,
        ) -> usize {
            for row in state.group_compare_vector[..need_compare_count]
                .iter()
                .copied()
            {
                let incoming = self.incoming[row];
                let (key, _, _) = self.get_payload(state.addresses[row]);

                if incoming.0 == key {
                    continue;
                }

                state.no_match_vector[no_match_count] = row;
                no_match_count += 1;
            }

            no_match_count
        }
    }

    #[derive(Clone)]
    struct TestCase {
        capacity: usize,
        incoming: Vec<(u64, u64)>,     // (key, hash)
        payload: Vec<(u64, u64, u64)>, // (key, hash, value)
        want_count: usize,
        want: HashMap<u64, u64>,
    }

    impl TestCase {
        fn run_hash_index(self) {
            self.clone().run_with::<u32>();
            self.run_with::<u64>();
        }

        fn run_with<I: RowId>(self) {
            let TestCase {
                capacity,
                incoming,
                payload,
                want_count,
                want,
            } = self;
            let mut hash_index = RowIdIndex::<I>::with_capacity(capacity);
            let mut adapter = TestTableAdapter::new(incoming, payload);
            let mut state = adapter.init_state();

            adapter.init_hash_index(&mut hash_index);

            let count =
                hash_index.probe_and_create(&mut state, adapter.incoming.len(), &mut adapter);

            assert_eq!(want_count, count);

            let got = state.addresses[..state.row_count]
                .iter()
                .map(|row_ptr| {
                    let (key, _, value) = adapter.get_payload(*row_ptr);
                    (key, value)
                })
                .collect::<HashMap<_, _>>();

            assert_eq!(want, got);
        }
    }

    #[test]
    fn test_hash_index() {
        TestCase {
            capacity: 16,
            incoming: vec![(1, 123), (2, 456), (3, 123), (4, 44)],
            payload: vec![(4, 44, 77)],
            want_count: 3,
            want: HashMap::from_iter([(1, 21), (2, 22), (3, 23), (4, 77)]),
        }
        .run_hash_index();

        TestCase {
            capacity: 16,
            incoming: vec![(1, 11 << 48), (2, 22 << 48), (3, 33 << 48), (4, 44 << 48)],
            payload: vec![(4, 44 << 48, 77)],
            want_count: 3,
            want: HashMap::from_iter([(1, 21), (2, 22), (3, 23), (4, 77)]),
        }
        .run_hash_index();
    }

    #[test]
    fn test_hash_index_chunk_overflow_and_wrap() {
        // Every key has the same tag and starts at the same chunk, so each probe compares and
        // steps over all the keys before it, past the end of a chunk into the next one.
        let same = |chunk: u64| (0x55u64 << 57) | chunk;
        let existing = |n: u64, chunk: u64| {
            (0..n)
                .map(|k| (k, same(chunk), k + 100))
                .collect::<Vec<_>>()
        };
        let incoming = |from: u64, to: u64, chunk: u64| {
            (from..to).map(|k| (k, same(chunk))).collect::<Vec<_>>()
        };

        // Two chunks of 12 slots: 8 keys present, 8 found again and 8 new ones overflow from
        // chunk 0 into chunk 1.
        TestCase {
            capacity: 24,
            incoming: incoming(0, 16, 0),
            payload: existing(8, 0),
            want_count: 8,
            want: (0..16)
                .map(|k| (k, if k < 8 { k + 100 } else { k + 20 }))
                .collect(),
        }
        .run_hash_index();

        // Starting at the last chunk wraps around to chunk 0.
        TestCase {
            capacity: 24,
            incoming: incoming(0, 22, 1),
            payload: existing(10, 1),
            want_count: 12,
            want: (0..22)
                .map(|k| (k, if k < 10 { k + 100 } else { k + 20 }))
                .collect(),
        }
        .run_hash_index();
    }
}
