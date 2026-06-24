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
use std::mem::size_of;

use crate::aggregate::HASH_INDEX_LOAD_FACTOR;
use crate::aggregate::ProbeState;
use crate::aggregate::hash_index::bitmask::Tag;
use crate::aggregate::hash_index::group::Group;
use crate::aggregate::hash_index_adapter::TableAdapter;
use crate::aggregate::row_ptr::RowPtr;

pub(in crate::aggregate) struct HashIndex {
    ctrls: Vec<Tag>,
    pointers: Vec<RowPtr>,
    capacity: usize,
    bucket_mask: usize,
    count: usize,
}

impl HashIndex {
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
}

impl HashIndex {
    #[inline]
    fn set_ctrl(&mut self, index: usize, tag: Tag) {
        // Mirror: keep the tail padding in sync with the head so that
        // Group::load across the boundary sees consistent ctrl bytes.
        let index2 = ((index.wrapping_sub(Group::WIDTH)) & self.bucket_mask) + Group::WIDTH;

        self.ctrls[index] = tag;
        self.ctrls[index2] = tag;
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

    /// Find the index of a given `hash` from the `pos` or return a new slot if not exist
    /// If not exists, the ctrl byte will be set directly
    pub fn find_or_insert(&mut self, mut pos: usize, hash: u64) -> (usize, bool) {
        let mut insert_index = None;
        let tag_hash = Tag::full(hash);
        loop {
            let group = unsafe { Group::load(&self.ctrls, pos) };
            if let Some(bit) = group.match_tag(tag_hash).into_iter().next() {
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

    /// Probes the hash table for an empty slot using SIMD groups (batches) and sets the control byte.
    ///
    /// Returns the index of the found slot.
    pub fn probe_empty_batch(&mut self, hash: u64) -> usize {
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

    /// Probes the hash table linearly (scalar probing) for an empty slot and sets the control byte.
    /// Returns the absolute index of the slot.
    ///
    /// # Performance Note
    /// This method is primarily used during resize operations. In such cases, the map is very
    /// sparse, meaning collisions are rare.
    ///
    /// While SIMD probing (`probe_empty_batch`) is efficient for skipping full groups, it has
    /// overhead. When the map is sparse, we expect to find an empty slot almost immediately
    /// (often the first probe). In this specific situation, a simple scalar probe is faster
    pub fn probe_empty(&mut self, hash: u64) -> usize {
        let tag_hash = Tag::full(hash);
        let mut pos = self.h1(hash);
        loop {
            if self.ctrls[pos].is_empty() {
                self.set_ctrl(pos, tag_hash);
                return pos;
            }
            pos = (pos + 1) & self.bucket_mask;
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

impl HashIndex {
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn resize_threshold(&self) -> usize {
        (self.capacity as f64 / HASH_INDEX_LOAD_FACTOR) as usize
    }

    pub fn allocated_bytes(&self) -> usize {
        self.ctrls.len() * size_of::<Tag>() + self.pointers.len() * size_of::<RowPtr>()
    }

    pub fn reset(&mut self) {
        if self.capacity == 0 {
            return;
        }
        self.count = 0;
        self.ctrls.fill(Tag::EMPTY);
        self.pointers.fill(RowPtr::null());
    }

    pub fn probe_slot_and_set(&mut self, hash: u64, row_ptr: RowPtr) {
        let index = self.probe_empty(hash);
        self.pointers[index] = row_ptr;
        self.count += 1;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    struct TestTableAdapter {
        incoming: Vec<(u64, u64)>,     // (key, hash)
        payload: Vec<(u64, u64, u64)>, // (key, hash, value)
        init_count: usize,
        pin_data: Box<[u8]>,
    }

    impl TestTableAdapter {
        fn new(incoming: Vec<(u64, u64)>, payload: Vec<(u64, u64, u64)>) -> Self {
            Self {
                incoming,
                init_count: payload.len(),
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

        fn init_hash_index(&self, hash_index: &mut HashIndex) {
            for (i, (_, hash, _)) in self.payload.iter().copied().enumerate() {
                hash_index.probe_slot_and_set(hash, self.get_row_ptr(false, i));
            }
        }

        fn get_row_ptr(&self, incoming: bool, row: usize) -> RowPtr {
            RowPtr::new(unsafe {
                self.pin_data
                    .as_ptr()
                    .add(if incoming { row + self.init_count } else { row })
            })
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

                self.payload.push((key, hash, value));
                state.addresses[*row] = self.get_row_ptr(true, row.to_usize());
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
            let TestCase {
                capacity,
                incoming,
                payload,
                want_count,
                want,
            } = self;
            let mut hash_index = HashIndex::with_capacity(capacity);
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
}
