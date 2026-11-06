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

use crate::aggregate::BATCH_SIZE;
use crate::aggregate::HASH_INDEX_LOAD_FACTOR;
use crate::aggregate::ProbeState;
use crate::aggregate::hash_index_adapter::TableAdapter;
use crate::aggregate::partitioned_payload::RowReserver;
use crate::aggregate::row_ptr::RowDecoder;

/// The slots of a chunk, whatever the width of its ids: an index of `n` chunks has
/// `n * CHUNK_SLOTS` slots.
pub(in crate::aggregate) const CHUNK_SLOTS: usize = 12;
/// Prefetch the chunks of the rows this far ahead, once the index outgrows the caches.
pub(in crate::aggregate) const PREFETCH_DISTANCE: usize = 16;
const PREFETCH_MIN_BYTES: usize = 1 << 20;

/// The row ids of the lanes of a chunk, see `Chunk`.
pub(in crate::aggregate) trait ChunkIds: Copy {
    /// The lanes of a chunk with these ids, at most one per tag.
    const LANES: usize;
    /// The largest id a lane holds.
    const MAX_ID: u64;
    const EMPTY: Self;

    fn get(&self, lane: usize) -> u64;

    fn set(&mut self, lane: usize, id: u64);
}

/// 12 ids of 4 bytes.
#[repr(C)]
#[derive(Clone, Copy)]
pub(in crate::aggregate) struct Ids32([u32; 12]);

impl ChunkIds for Ids32 {
    const LANES: usize = 12;
    const MAX_ID: u64 = u32::MAX as u64;
    const EMPTY: Self = Ids32([0; 12]);

    #[inline]
    fn get(&self, lane: usize) -> u64 {
        self.0[lane] as u64
    }

    #[inline]
    fn set(&mut self, lane: usize, id: u64) {
        // A truncated id would name another row and mix two groups. The table picks a width
        // that holds every id it may meet before the index is built again.
        debug_assert!(id <= Self::MAX_ID, "row id {id} does not fit 4 bytes");
        self.0[lane] = id as u32;
    }
}

/// 12 ids of 8 bytes, for tables of more rows than 4 bytes can name.
#[repr(C)]
#[derive(Clone, Copy)]
pub(in crate::aggregate) struct Ids64([u64; 12]);

impl ChunkIds for Ids64 {
    const LANES: usize = 12;
    const MAX_ID: u64 = u64::MAX;
    const EMPTY: Self = Ids64([0; 12]);

    #[inline]
    fn get(&self, lane: usize) -> u64 {
        self.0[lane]
    }

    #[inline]
    fn set(&mut self, lane: usize, id: u64) {
        self.0[lane] = id;
    }
}

/// The tag of a full lane: the top 7 bits of the hash of its group. An empty lane has
/// `EMPTY_TAG`, which no full lane has as its high bit is clear.
#[inline]
const fn tag(hash: u64) -> u8 {
    (hash >> 57) as u8
}

const EMPTY_TAG: u8 = 0x80;

/// The tags of 16 lanes, then the row ids of the lanes the ids have room for: one probe reads
/// one cache line with ids of up to 4 bytes.
#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct Chunk<I> {
    tags: [u8; 16],
    ids: I,
}

const _: () = assert!(size_of::<Chunk<Ids32>>() == 64);
const _: () = assert!(size_of::<Chunk<Ids64>>() == 128);

impl<I: ChunkIds> Chunk<I> {
    const EMPTY: Chunk<I> = Chunk {
        tags: [EMPTY_TAG; 16],
        ids: I::EMPTY,
    };
    /// The nibbles of the lanes that have an id, see `match_tag`.
    const LANE_MASK: u64 = u64::MAX >> (64 - I::LANES * 4);

    /// Lanes from `from` whose tag is `tag`, four bits per lane: the lane of a match is
    /// `trailing_zeros / 4`. Narrowing the compared lanes by a shift moves them to a scalar
    /// register in one step, where a one-bit-per-lane mask takes a reduction on NEON.
    #[inline]
    fn match_tag(&self, tag: u8, from: usize) -> u64 {
        let tags = u8x16::from_array(self.tags);
        let eq = tags.simd_eq(u8x16::splat(tag)).to_simd().cast::<u8>();
        let nibbles = (u16x8::from_ne_bytes(eq) >> 4).cast::<u8>();
        u64::from_ne_bytes(nibbles.to_array()) & Self::LANE_MASK & (u64::MAX << (from * 4))
    }
}

/// The hash index of an aggregate hash table. Its slots keep the ids of their rows, not their
/// addresses, in as few bytes as the ids of the table need: the width is chosen when the index
/// is built, from the largest id the table may give its rows until the index is built again,
/// see `AggregateHashTable::resize`. Every width is the same index, see `RowIdIndex`.
pub(in crate::aggregate) enum HashIndex {
    Id32(RowIdIndex<Ids32>),
    Id64(RowIdIndex<Ids64>),
}

macro_rules! with_index {
    ($self:expr, $index:ident => $body:expr) => {
        match $self {
            HashIndex::Id32($index) => $body,
            HashIndex::Id64($index) => $body,
        }
    };
}

impl HashIndex {
    /// An index of at least `capacity` slots, for rows of ids up to `max_id`.
    pub fn with_capacity(capacity: usize, max_id: u64) -> Self {
        let chunks = Self::slots_for(capacity) / CHUNK_SLOTS;
        if max_id <= Ids32::MAX_ID {
            HashIndex::Id32(RowIdIndex::with_chunks(chunks))
        } else {
            HashIndex::Id64(RowIdIndex::with_chunks(chunks))
        }
    }

    pub fn dummy() -> Self {
        HashIndex::Id32(RowIdIndex::with_chunks(0))
    }

    /// Whether the slots hold ids up to `max_id`.
    pub fn holds_id(&self, max_id: u64) -> bool {
        max_id <= with_index!(self, index => index.max_id())
    }

    /// The bytes of an id in a slot.
    #[cfg(test)]
    pub fn id_bytes(&self) -> usize {
        match self {
            HashIndex::Id32(_) => 4,
            HashIndex::Id64(_) => 8,
        }
    }

    pub fn capacity(&self) -> usize {
        with_index!(self, index => index.capacity())
    }

    pub fn count(&self) -> usize {
        with_index!(self, index => index.count)
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
        with_index!(self, index => index.allocated_bytes())
    }

    /// The bytes `with_capacity(capacity, max_id)` allocates for ids of up to 4 bytes.
    pub fn allocated_bytes_for(capacity: usize) -> usize {
        Self::slots_for(capacity) / CHUNK_SLOTS * size_of::<Chunk<Ids32>>()
    }

    pub fn reset(&mut self) {
        with_index!(self, index => index.reset())
    }

    /// Fetch the chunk where a row of hash `hash` starts its probe, to set it later: a rebuild
    /// does this a few rows ahead, as the index is mostly out of the caches.
    #[inline]
    pub fn prefetch(&self, hash: u64) {
        with_index!(self, index => index.prefetch(hash))
    }

    /// Set a free slot for a row known to be absent, while the index is rebuilt.
    pub fn probe_slot_and_set(&mut self, hash: u64, row_id: u64) {
        with_index!(self, index => index.probe_slot_and_set(hash, row_id))
    }

    pub fn probe_and_create(
        &mut self,
        state: &mut ProbeState,
        row_count: usize,
        adapter: &mut dyn TableAdapter,
    ) -> usize {
        with_index!(self, index => index.probe_and_create(state, row_count, adapter))
    }
}

/// A hash index whose slots keep row ids in chunks of ids `I`, a power of two of them. A slot is
/// numbered `chunk * 16 + lane`; a probe starts at lane 0 of the chunk the hash selects and moves
/// on lane by lane, then chunk by chunk. Groups are never removed, so a probe ends at the first
/// slot of its tag whose row has its group, or at the first empty slot.
pub(in crate::aggregate) struct RowIdIndex<I> {
    chunks: Vec<Chunk<I>>,
    chunk_mask: usize,
    count: usize,
}

impl<I: ChunkIds> RowIdIndex<I> {
    fn with_chunks(chunks: usize) -> Self {
        debug_assert!(chunks == 0 || chunks.is_power_of_two());
        Self {
            chunks: vec![Chunk::EMPTY; chunks],
            chunk_mask: chunks.saturating_sub(1),
            count: 0,
        }
    }

    fn max_id(&self) -> u64 {
        I::MAX_ID
    }

    fn capacity(&self) -> usize {
        self.chunks.len() * CHUNK_SLOTS
    }

    #[inline]
    fn first_slot(&self, hash: u64) -> usize {
        (hash as usize & self.chunk_mask) << 4
    }

    #[inline]
    fn next_slot(&self, slot: usize) -> usize {
        if (slot & 15) + 1 < I::LANES {
            slot + 1
        } else {
            (((slot >> 4) + 1) & self.chunk_mask) << 4
        }
    }

    /// The first slot from `slot` that holds the tag of `hash`, whether it does, and its id; or
    /// else the first empty slot. Plain values, not an `Option`: an enum joined from the two
    /// exits goes through the stack in the probe loop. A probe of the `first` round starts at
    /// lane 0 of its chunk.
    #[inline(always)]
    fn find(&self, slot: usize, hash: u64, first: bool) -> (usize, bool, u64) {
        let tag = tag(hash);
        let mut chunk = slot >> 4;
        let mut from = if first { 0 } else { slot & 15 };
        loop {
            debug_assert!(chunk < self.chunks.len());
            // SAFETY: a chunk number comes from `first_slot`, `next_slot` or this loop, which
            // all mask it with `chunk_mask`, and there are `chunk_mask + 1` chunks: see
            // `probe_and_create`, which probes no empty index.
            let c = unsafe { self.chunks.get_unchecked(chunk) };
            let matched = c.match_tag(tag, from);
            if matched != 0 {
                let lane = (matched.trailing_zeros() / 4) as usize;
                return ((chunk << 4) + lane, true, c.ids.get(lane));
            }
            let empty = c.match_tag(EMPTY_TAG, from);
            if empty != 0 {
                let lane = (empty.trailing_zeros() / 4) as usize;
                return ((chunk << 4) + lane, false, 0);
            }
            chunk = (chunk + 1) & self.chunk_mask;
            from = 0;
        }
    }

    #[inline]
    fn set(&mut self, slot: usize, hash: u64, id: u64) {
        let c = &mut self.chunks[slot >> 4];
        c.tags[slot & 15] = tag(hash);
        c.ids.set(slot & 15, id);
    }

    /// Match the first `remaining` rows of `state.no_match_vector` from their slots. A row that
    /// meets its tag goes to `group_compare_vector`; a row that meets an empty slot reserves a
    /// row for its group, puts its id in the slot, and goes to `empty_vector`. Either way the
    /// row gets the id of the row of the slot in `row_ids`. Returns the counts of the two
    /// vectors.
    ///
    /// Matching does not decode the ids, see `decode_round`: the loop then keeps what it needs
    /// in registers, as the matching pass of a hash table of row addresses does.
    #[inline(always)]
    fn match_round(
        &mut self,
        state: &mut ProbeState,
        remaining: usize,
        reserver: &mut RowReserver,
        (first, prefetch): (bool, bool),
    ) -> (usize, usize) {
        let mut new_entry_count = 0;
        let mut need_compare_count = 0;
        // Only the rows of this round: the rest of the vector holds rows of earlier calls,
        // possibly of another table.
        let round = &state.no_match_vector[..remaining];
        for (i, &row) in round.iter().enumerate() {
            if prefetch && let Some(&ahead) = round.get(i + PREFETCH_DISTANCE) {
                let chunk = state.slots[ahead] >> 4;
                prefetch_read_data::<_, 3>(self.chunks.as_ptr().wrapping_add(chunk));
            }
            let hash = state.group_hashes[row];
            let (slot, found, id) = self.find(state.slots[row], hash, first);
            state.slots[row] = slot;
            // A round has at most `BATCH_SIZE` rows, and each row adds to one of the two counts.
            debug_assert!(new_entry_count + need_compare_count < BATCH_SIZE);
            let id = if found {
                // A slot claimed earlier in this round has its row reserved, and the row is
                // written before the compare.
                // SAFETY: see above, `need_compare_count < BATCH_SIZE`.
                unsafe {
                    *state
                        .group_compare_vector
                        .get_unchecked_mut(need_compare_count) = row
                };
                need_compare_count += 1;
                id
            } else {
                let (id, page) = reserver.reserve(hash);
                self.set(slot, hash, id);
                state.page_index[row] = page;
                // SAFETY: see above, `new_entry_count < BATCH_SIZE`.
                unsafe { *state.empty_vector.get_unchecked_mut(new_entry_count) = row };
                new_entry_count += 1;
                id
            };
            state.row_ids[row] = id;
        }
        (new_entry_count, need_compare_count)
    }

    /// Set the addresses of the rows, and of their states, of the first `remaining` rows of
    /// `state.no_match_vector` from their ids. A table without aggregate states gets state
    /// addresses it never reads, which costs less than a branch on every row.
    #[inline(always)]
    fn decode_round(state: &mut ProbeState, remaining: usize, rows: RowDecoder) {
        for row in state.no_match_vector[..remaining].iter().copied() {
            (state.addresses[row], state.state_places[row]) =
                rows.row_and_states(state.row_ids[row]);
        }
    }

    /// Probe the first `remaining` rows of `state.no_match_vector`: match them, decode their ids,
    /// write the rows of their new groups and compare the others. Returns the rows that go on to
    /// the next round, in `state.no_match_vector`.
    #[inline(always)]
    fn probe_round(
        &mut self,
        state: &mut ProbeState,
        remaining: usize,
        adapter: &mut dyn TableAdapter,
        new_group_count: &mut usize,
        round: (bool, bool),
    ) -> usize {
        let (new_entry_count, need_compare_count) = {
            let (rows, mut reserver) = adapter.rows();
            let counts = self.match_round(state, remaining, &mut reserver, round);
            Self::decode_round(state, remaining, rows);
            counts
        };

        if new_entry_count != 0 {
            *new_group_count += new_entry_count;
            adapter.append_rows(state, new_entry_count);
        }

        let mut no_match_count = 0;
        if need_compare_count > 0 {
            no_match_count = adapter.compare(state, need_compare_count, no_match_count);
        }

        for row in state.no_match_vector[..no_match_count].iter().copied() {
            state.slots[row] = self.next_slot(state.slots[row]);
        }
        no_match_count
    }

    /// Find or create the groups of the first `row_count` rows of `state`, and set their
    /// `addresses`, and their `state_places` if the table has aggregate states.
    fn probe_and_create(
        &mut self,
        state: &mut ProbeState,
        row_count: usize,
        adapter: &mut dyn TableAdapter,
    ) -> usize {
        // `find` relies on it.
        assert!(!self.chunks.is_empty(), "probe of an empty hash index");
        for (i, row) in state.no_match_vector[..row_count].iter_mut().enumerate() {
            *row = i.into();
            state.slots[i] = self.first_slot(state.group_hashes[i]);
        }
        // Each row starts at a random chunk: once the index is out of the caches, fetch the
        // chunks of the rows ahead while probing the current one. Rows that mismatch go on in
        // the chunk they reached, so only the first round prefetches.
        let prefetch = self.allocated_bytes() >= PREFETCH_MIN_BYTES;
        adapter.begin_rows(row_count);

        // The first round, where every row starts at lane 0, is a loop of its own: the rounds
        // after it only take the rows whose group was not where their tag was.
        let mut new_group_count = 0;
        let mut remaining = self.probe_round(
            state,
            row_count,
            adapter,
            &mut new_group_count,
            (true, prefetch),
        );
        while remaining > 0 {
            remaining = self.probe_round(
                state,
                remaining,
                adapter,
                &mut new_group_count,
                (false, false),
            );
        }

        self.count += new_group_count;
        new_group_count
    }

    fn allocated_bytes(&self) -> usize {
        self.chunks.len() * size_of::<Chunk<I>>()
    }

    fn reset(&mut self) {
        self.count = 0;
        self.chunks.fill(Chunk::EMPTY);
    }

    #[inline]
    fn prefetch(&self, hash: u64) {
        let chunk = hash as usize & self.chunk_mask;
        prefetch_read_data::<_, 3>(self.chunks.as_ptr().wrapping_add(chunk));
    }

    /// Put a row known to be absent into the first empty slot of its probe sequence.
    fn probe_slot_and_set(&mut self, hash: u64, row_id: u64) {
        let mut chunk = hash as usize & self.chunk_mask;
        loop {
            let empty = self.chunks[chunk].match_tag(EMPTY_TAG, 0);
            if empty != 0 {
                let lane = (empty.trailing_zeros() / 4) as usize;
                self.set((chunk << 4) + lane, hash, row_id);
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
    use crate::aggregate::payload::Payload;
    use crate::aggregate::payload::RowCursor;
    use crate::aggregate::row_ptr::RowIds;
    use crate::aggregate::row_ptr::RowPtr;

    struct TestTableAdapter {
        incoming: Vec<(u64, u64)>,     // (key, hash)
        payload: Vec<(u64, u64, u64)>, // (key, hash, value)
        pin_data: Box<[u8]>,
        row_ids: RowIds,
        cursor: RowCursor,
        no_payloads: Vec<Payload>,
    }

    impl TestTableAdapter {
        fn new(incoming: Vec<(u64, u64)>, payload: Vec<(u64, u64, u64)>) -> Self {
            let pin_data: Box<[u8]> = vec![0; 1000].into();
            // One page of rows of one byte: the id of a row is its position in `pin_data`.
            let row_ids = RowIds::single_page(pin_data.as_ptr(), 16, 1);
            Self {
                incoming,
                payload,
                pin_data,
                row_ids,
                cursor: RowCursor::EMPTY,
                no_payloads: vec![],
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

        fn init_hash_index<I: ChunkIds>(&self, hash_index: &mut RowIdIndex<I>) {
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
        fn begin_rows(&mut self, _: usize) {
            // The page holds every row of a test, so no page is opened.
            self.cursor = RowCursor {
                id: self.payload.len() as u64,
                end: self.pin_data.len() as u64,
                page: 0,
            };
        }

        fn rows(&mut self) -> (RowDecoder<'_>, RowReserver<'_>) {
            let (rows, numbering) = self.row_ids.split();
            (
                rows,
                RowReserver::single(
                    &mut self.no_payloads,
                    std::slice::from_mut(&mut self.cursor),
                    numbering,
                ),
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
            self.clone().run_with::<Ids32>();
            self.run_with::<Ids64>();
        }

        fn run_with<I: ChunkIds>(self) {
            let TestCase {
                capacity,
                incoming,
                payload,
                want_count,
                want,
            } = self;
            let chunks = HashIndex::slots_for(capacity) / CHUNK_SLOTS;
            let mut hash_index = RowIdIndex::<I>::with_chunks(chunks);
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
        fn run<I: ChunkIds>() {
            // Every key has the same tag and starts at the same chunk, so each probe compares
            // and steps over all the keys before it, past the end of a chunk into the next one.
            let lanes = I::LANES as u64;
            let same = |chunk: u64| (0x55u64 << 57) | chunk;
            let existing = |n: u64, chunk: u64| {
                (0..n)
                    .map(|k| (k, same(chunk), k + 100))
                    .collect::<Vec<_>>()
            };
            let incoming = |n: u64, chunk: u64| (0..n).map(|k| (k, same(chunk))).collect();
            let want = |n: u64, present: u64| {
                (0..n)
                    .map(|k| (k, if k < present { k + 100 } else { k + 20 }))
                    .collect()
            };

            // Two chunks: some keys present and found again, 8 new ones overflow from chunk 0
            // into chunk 1.
            TestCase {
                capacity: 2 * CHUNK_SLOTS,
                incoming: incoming(lanes + 4, 0),
                payload: existing(lanes - 4, 0),
                want_count: 8,
                want: want(lanes + 4, lanes - 4),
            }
            .run_with::<I>();

            // Starting at the last chunk wraps around to chunk 0.
            TestCase {
                capacity: 2 * CHUNK_SLOTS,
                incoming: incoming(lanes + 10, 1),
                payload: existing(lanes - 2, 1),
                want_count: 12,
                want: want(lanes + 10, lanes - 2),
            }
            .run_with::<I>();
        }
        run::<Ids32>();
        run::<Ids64>();
    }

    #[test]
    fn test_hash_index_width_holds_its_ids() {
        for (max_id, bytes) in [(0, 4), (Ids32::MAX_ID, 4), (Ids32::MAX_ID + 1, 8)] {
            let index = HashIndex::with_capacity(16, max_id);
            assert_eq!(index.id_bytes(), bytes);
            assert!(index.holds_id(max_id));
            assert_eq!(index.capacity(), 2 * CHUNK_SLOTS);
        }
    }
}
