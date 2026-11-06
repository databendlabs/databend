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
use std::simd::ToBytes;
use std::simd::cmp::SimdPartialEq;
use std::simd::num::SimdInt;
use std::simd::num::SimdUint;
use std::simd::u8x16;
use std::simd::u16x8;

use crate::aggregate::HASH_INDEX_LOAD_FACTOR;
use crate::aggregate::ProbeState;
use crate::aggregate::hash_index_adapter::TableAdapter;
use crate::aggregate::partitioned_payload::RowReserver;
use crate::aggregate::row_ptr::RowDecoder;

/// The id of a row as a hash index slot keeps it: 4 bytes while the ids of the table fit, 8 bytes
/// otherwise, see `RowIds`.
pub(in crate::aggregate) trait RowId: Copy + PartialEq {
    const ZERO: Self;

    fn from_id(id: u64) -> Self;

    fn id(self) -> u64;
}

impl RowId for u32 {
    const ZERO: Self = 0;

    #[inline]
    fn from_id(id: u64) -> Self {
        // A truncated id would name another row and mix two groups. `resize` and
        // `restart_index` switch the table to wide ids before that can happen.
        debug_assert!(
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

    /// The bytes `with_capacity(capacity)` allocates.
    pub fn allocated_bytes_for(capacity: usize) -> usize {
        Self::slots_for(capacity) / CHUNK_SLOTS * size_of::<Chunk<u32>>()
    }

    pub fn reset(&mut self) {
        match self {
            HashIndex::Compact(index) => index.reset(),
            HashIndex::Wide(index) => index.reset(),
        }
    }

    /// Fetch the chunk where a row of hash `hash` starts its probe, to set it later: a rebuild
    /// does this a few rows ahead, as the index is mostly out of the caches.
    #[inline]
    pub fn prefetch(&self, hash: u64) {
        match self {
            HashIndex::Compact(index) => index.prefetch(hash),
            HashIndex::Wide(index) => index.prefetch(hash),
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
pub(in crate::aggregate) const CHUNK_SLOTS: usize = 12;
/// Prefetch the chunks of the rows this far ahead, once the index outgrows the caches.
pub(in crate::aggregate) const PREFETCH_DISTANCE: usize = 16;
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

    /// Slots from `from` whose tag is `tag`, four bits per slot: the slot of a match is
    /// `trailing_zeros / 4`. Narrowing the compared lanes by a shift moves them to a scalar
    /// register in one step, where a one-bit-per-lane mask takes a reduction on NEON.
    #[inline]
    fn match_tag(&self, tag: Tag, from: usize) -> u64 {
        const SLOTS: u64 = (1 << (CHUNK_SLOTS * 4)) - 1;
        let tags = u8x16::from_array(self.tags.map(|t| t.0));
        let eq = tags.simd_eq(u8x16::splat(tag.0)).to_simd().cast::<u8>();
        let nibbles = (u16x8::from_ne_bytes(eq) >> 4).cast::<u8>();
        u64::from_ne_bytes(nibbles.to_array()) & SLOTS & (!0 << (from * 4))
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

    /// The first slot from `slot` that holds the tag of `hash`, with its id, or else the first
    /// empty one. `from` is the lane of `slot`, given apart so that a probe that starts at lane 0
    /// can make it a constant.
    #[inline]
    fn find(&self, slot: usize, hash: u64, from: usize) -> (usize, Option<I>) {
        debug_assert_eq!(from, slot & 15);
        let tag = Tag::full(hash);
        let mut chunk = slot >> 4;
        let mut from = from;
        loop {
            let c = &self.chunks[chunk];
            let matched = c.match_tag(tag, from);
            if matched != 0 {
                let lane = (matched.trailing_zeros() / 4) as usize;
                return ((chunk << 4) + lane, Some(c.ids[lane]));
            }
            let empty = c.match_tag(Tag::EMPTY, from);
            if empty != 0 {
                let lane = (empty.trailing_zeros() / 4) as usize;
                return ((chunk << 4) + lane, None);
            }
            chunk = (chunk + 1) & self.chunk_mask;
            from = 0;
        }
    }

    /// Probe the rows `start..remaining` of `state.no_match_vector` from their slots: a row that
    /// meets its tag goes to `group_compare_vector` with its row and states decoded; a row that
    /// meets an empty slot reserves a row for its group, puts the id in the slot, and goes to
    /// `empty_vector`. Stops at a row whose partition has no free row, returning the row and the
    /// partition, before it changes anything.
    ///
    /// The first round starts every row at lane 0 (`FIRST`), and only the first round prefetches
    /// (`PREFETCH`): each case has a loop of its own, without per-row checks for the others. A
    /// table without aggregate states (`STATES`) has no state addresses to find.
    #[inline(always)]
    fn probe_round<const FIRST: bool, const PREFETCH: bool, const STATES: bool>(
        &mut self,
        state: &mut ProbeState,
        (start, remaining): (usize, usize),
        (rows, reserver): (RowDecoder, &mut RowReserver),
        counts: &mut (usize, usize),
    ) -> Option<(usize, usize)> {
        let (mut new_entry_count, mut need_compare_count) = *counts;
        let mut stop = None;
        for i in start..remaining {
            let row = state.no_match_vector[i];
            // Only the rows of this round: the rest of the vector holds rows of earlier calls,
            // possibly of another table.
            if PREFETCH && i + PREFETCH_DISTANCE < remaining {
                let chunk = state.slots[state.no_match_vector[i + PREFETCH_DISTANCE]] >> 4;
                prefetch_read_data::<_, 3>(self.chunks[..].as_ptr().wrapping_add(chunk));
            }
            let hash = state.group_hashes[row];
            let first = state.slots[row];
            let from = if FIRST { 0 } else { first & 15 };
            let (slot, id) = self.find(first, hash, from);

            match id {
                // The chunk of the slot was just read: decode the id now, not in a pass of its
                // own. A slot claimed earlier in this round has its row reserved, and the row is
                // written before the compare.
                Some(id) => {
                    if STATES {
                        (state.addresses[row], state.state_places[row]) =
                            rows.row_and_states(id.id());
                    } else {
                        state.addresses[row] = rows.row(id.id());
                    }
                    state.slots[row] = slot;
                    state.group_compare_vector[need_compare_count] = row;
                    need_compare_count += 1;
                }
                None => {
                    let (id, address, page) = match reserver.reserve(hash) {
                        Ok(reserved) => reserved,
                        Err(partition) => {
                            stop = Some((i, partition));
                            break;
                        }
                    };
                    let c = &mut self.chunks[slot >> 4];
                    c.tags[slot & 15] = Tag::full(hash);
                    c.ids[slot & 15] = I::from_id(id);
                    state.addresses[row] = address;
                    if STATES {
                        state.state_places[row] = rows.row_and_states(id).1;
                        state.page_index[row] = page;
                    }
                    state.slots[row] = slot;
                    state.empty_vector[new_entry_count] = row;
                    new_entry_count += 1;
                }
            }
        }
        *counts = (new_entry_count, need_compare_count);
        stop
    }

    /// Find or create the groups of the first `row_count` rows of `state`, and set their
    /// `addresses`, and their `state_places` if the table has aggregate states.
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
        let prefetch = self.allocated_bytes() >= PREFETCH_MIN_BYTES;
        adapter.begin_rows();

        let mut new_group_count = 0;
        let mut remaining_entries = row_count;
        let mut first_round = true;

        while remaining_entries > 0 {
            let mut counts = (0, 0);
            let mut start = 0;
            loop {
                let (rows, mut reserver) = adapter.rows();
                let has_states = rows.has_states();
                let r = (rows, &mut reserver);
                let span = (start, remaining_entries);
                let c = &mut counts;
                let stop = match (first_round, prefetch, has_states) {
                    (true, true, true) => self.probe_round::<true, true, true>(state, span, r, c),
                    (true, true, false) => self.probe_round::<true, true, false>(state, span, r, c),
                    (true, false, true) => self.probe_round::<true, false, true>(state, span, r, c),
                    (true, false, false) => {
                        self.probe_round::<true, false, false>(state, span, r, c)
                    }
                    // Mismatched rows go on in the chunk they reached.
                    (false, _, true) => self.probe_round::<false, false, true>(state, span, r, c),
                    (false, _, false) => self.probe_round::<false, false, false>(state, span, r, c),
                };
                let Some((row, partition)) = stop else {
                    break;
                };
                adapter.open_rows(partition);
                start = row;
            }
            first_round = false;
            let (new_entry_count, need_compare_count) = counts;

            if new_entry_count != 0 {
                new_group_count += new_entry_count;
                adapter.append_rows(state, new_entry_count);
            }

            let mut no_match_count = 0;
            if need_compare_count > 0 {
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

    #[inline]
    fn prefetch(&self, hash: u64) {
        let chunk = hash as usize & self.chunk_mask;
        prefetch_read_data::<_, 3>(self.chunks[..].as_ptr().wrapping_add(chunk));
    }

    /// Put a row known to be absent into the first empty slot of its probe sequence.
    pub fn probe_slot_and_set(&mut self, hash: u64, row_id: u64) {
        let mut chunk = hash as usize & self.chunk_mask;
        loop {
            let c = &mut self.chunks[chunk];
            let empty = c.match_tag(Tag::EMPTY, 0);
            if empty != 0 {
                let lane = (empty.trailing_zeros() / 4) as usize;
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
    use crate::aggregate::payload::RowCursor;
    use crate::aggregate::row_ptr::RowPtr;

    struct TestTableAdapter {
        incoming: Vec<(u64, u64)>,     // (key, hash)
        payload: Vec<(u64, u64, u64)>, // (key, hash, value)
        pin_data: Box<[u8]>,
        pages: Vec<*const u8>,
        cursor: RowCursor,
    }

    impl TestTableAdapter {
        fn new(incoming: Vec<(u64, u64)>, payload: Vec<(u64, u64, u64)>) -> Self {
            let pin_data: Box<[u8]> = vec![0; 1000].into();
            let pages = vec![pin_data.as_ptr()];
            Self {
                incoming,
                payload,
                pin_data,
                pages,
                cursor: RowCursor::EMPTY,
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
        fn begin_rows(&mut self) {
            // The id of a row is its position in the payload.
            let id = self.payload.len();
            self.cursor = RowCursor {
                next: self.get_row_ptr(id).as_ptr(),
                id: id as u64,
                end: self.pin_data.len() as u64,
                page: 0,
            };
        }

        fn open_rows(&mut self, _: usize) {
            unreachable!("the test payload has a single page");
        }

        fn rows(&mut self) -> (RowDecoder<'_>, RowReserver<'_>) {
            (
                RowDecoder::new(&self.pages, 32, 1),
                RowReserver::single(&mut self.cursor, 1),
            )
        }

        fn append_rows(&mut self, state: &mut ProbeState, new_entry_count: usize) {
            for row in state.empty_vector[..new_entry_count].iter() {
                let (key, hash) = self.incoming[*row];
                let value = key + 20;

                // The probe reserved the rows in the order of the new groups.
                assert_eq!(state.addresses[*row], self.get_row_ptr(self.payload.len()));
                self.payload.push((key, hash, value));
            }
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
    fn test_hash_index_rows_meet_slots_claimed_in_the_batch() {
        // Key 7 comes three times and key 8 once, all with the same hash: the later rows meet the
        // slot that the first row of key 7 claimed before its row was written.
        TestCase {
            capacity: 16,
            incoming: vec![(7, 99), (7, 99), (8, 99), (7, 99)],
            payload: vec![],
            want_count: 2,
            want: HashMap::from_iter([(7, 27), (8, 28)]),
        }
        .run_hash_index();

        // The same with a group already in the slot before them.
        TestCase {
            capacity: 16,
            incoming: vec![(5, 99), (7, 99), (7, 99), (5, 99)],
            payload: vec![(5, 99, 77)],
            want_count: 1,
            want: HashMap::from_iter([(5, 77), (7, 27)]),
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
