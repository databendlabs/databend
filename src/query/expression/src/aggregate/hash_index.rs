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

use std::fmt::Debug;

use super::LOAD_FACTOR;
use super::PartitionedPayload;
use super::ProbeState;
use super::RowPtr;
use super::payload_row::CompareState;
use crate::ProjectedBlock;

pub(super) struct HashIndex {
    partitions: Vec<Partition>,
    partition_selector: PartitionSelector,
    pub count: usize,
    pub capacity: usize,
}

const INCREMENT_BITS: usize = 5;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(super) struct Slot {
    pub partition: usize,
    pub offset: usize,
}

#[derive(Clone)]
struct Partition {
    entries: Vec<Entry>,
    capacity_mask: usize,
}

impl Partition {
    fn with_capacity(capacity: usize) -> Self {
        debug_assert!(capacity.is_power_of_two());
        Self {
            entries: vec![Entry::default(); capacity],
            capacity_mask: capacity - 1,
        }
    }

    fn allocated_bytes(&self) -> usize {
        self.entries.len() * std::mem::size_of::<Entry>()
    }
}

#[derive(Clone, Copy)]
struct PartitionSelector {
    mask: u64,
    shift: u64,
}

impl PartitionSelector {
    fn new(partition_count: usize) -> Self {
        debug_assert!(partition_count.is_power_of_two());
        let bits = partition_count.trailing_zeros() as u64;
        if bits == 0 {
            return Self { mask: 0, shift: 0 };
        }
        let shift = 64 - bits;
        let mask = ((1u64 << bits) - 1) << shift;

        Self { mask, shift }
    }

    #[inline(always)]
    fn index(&self, hash: u64) -> usize {
        if self.mask == 0 {
            0
        } else {
            ((hash & self.mask) >> self.shift) as usize
        }
    }
}

/// Derive an odd probing step from the high bits of the hash so the walk spans all slots.
///
/// this will generate a step in the range [1, 2^INCREMENT_BITS) based on hash and always odd.
#[inline(always)]
fn step(hash: u64) -> usize {
    ((hash >> (64 - INCREMENT_BITS)) as usize) | 1
}

/// Move to the next slot with wrap-around using the power-of-two capacity mask.
///
/// soundness: capacity is always a power of two, so mask is capacity - 1
#[inline(always)]
fn next_slot(slot: usize, hash: u64, mask: usize) -> usize {
    (slot + step(hash)) & mask
}

impl HashIndex {
    pub fn with_capacity(capacity: usize, max_partition_capacity: usize) -> Self {
        debug_assert!(capacity.is_power_of_two());
        let max_partition_capacity = max_partition_capacity.max(1);

        let mut partition_count = 1;
        while capacity / partition_count > max_partition_capacity {
            partition_count <<= 1;
        }

        let partition_capacity = capacity / partition_count;
        let partition_selector = PartitionSelector::new(partition_count);
        let partitions = (0..partition_count)
            .map(|_| Partition::with_capacity(partition_capacity))
            .collect();

        Self {
            partitions,
            partition_selector,
            count: 0,
            capacity,
        }
    }

    fn find_or_insert(&mut self, mut slot: Slot, hash: u64) -> (Slot, bool) {
        let salt = Entry::hash_to_salt(hash);
        let partition = self.partition_mut(slot.partition);
        let entries = partition.entries.as_mut_slice();
        loop {
            debug_assert!(entries.get(slot.offset).is_some());
            // SAFETY: slot is always in range
            let entry = unsafe { entries.get_unchecked_mut(slot.offset) };
            if entry.is_occupied() {
                if entry.get_salt() == salt {
                    return (slot, false);
                } else {
                    slot.offset = next_slot(slot.offset, hash, partition.capacity_mask);
                    continue;
                }
            } else {
                entry.set_salt(salt);
                return (slot, true);
            }
        }
    }

    #[inline(always)]
    fn init_slot(&self, hash: u64) -> Slot {
        let partition = self.partition_selector.index(hash);
        let offset = hash as usize & self.partition(partition).capacity_mask;
        Slot { partition, offset }
    }

    pub fn probe_slot(&mut self, hash: u64) -> Slot {
        let mut slot = self.init_slot(hash);
        let partition = self.partition_mut(slot.partition);
        let mask = partition.capacity_mask;
        let entries = partition.entries.as_mut_slice();
        while entries[slot.offset].is_occupied() {
            slot.offset = next_slot(slot.offset, hash, mask);
        }
        slot
    }

    #[inline(always)]
    fn partition(&self, index: usize) -> &Partition {
        debug_assert!(index < self.partitions.len());
        // SAFETY: index checked above
        unsafe { self.partitions.get_unchecked(index) }
    }

    #[inline(always)]
    fn partition_mut(&mut self, index: usize) -> &mut Partition {
        debug_assert!(index < self.partitions.len());
        // SAFETY: index checked above
        unsafe { self.partitions.get_unchecked_mut(index) }
    }

    pub fn mut_entry(&mut self, slot: Slot) -> &mut Entry {
        &mut self.partition_mut(slot.partition).entries[slot.offset]
    }

    pub fn reset(&mut self) {
        self.count = 0;
        for partition in self.partitions.iter_mut() {
            partition.entries.fill(Entry::default());
        }
    }

    pub fn resize_threshold(&self) -> usize {
        (self.capacity as f64 / LOAD_FACTOR) as usize
    }

    pub fn allocated_bytes(&self) -> usize {
        self.partitions.iter().map(Partition::allocated_bytes).sum()
    }

    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }
}

/// Upper 16 bits are salt
const SALT_MASK: u64 = 0xFFFF000000000000;
/// Lower 48 bits are the pointer
const POINTER_MASK: u64 = 0x0000FFFFFFFFFFFF;

// The high 16 bits are the salt, the low 48 bits are the pointer address
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub(super) struct Entry(pub(super) u64);

impl Entry {
    pub fn hash_to_salt(hash: u64) -> u16 {
        (hash >> 48) as _
    }

    pub fn get_salt(&self) -> u16 {
        (self.0 >> 48) as _
    }

    pub fn set_salt(&mut self, salt: u16) {
        self.0 = POINTER_MASK | (salt as u64) << 48;
    }

    pub fn set_hash(&mut self, hash: u64) {
        self.0 = hash | POINTER_MASK
    }

    pub fn is_occupied(&self) -> bool {
        self.0 != 0
    }

    pub fn get_pointer(&self) -> RowPtr {
        RowPtr::new((self.0 & POINTER_MASK) as *mut u8)
    }

    pub fn set_pointer(&mut self, ptr: RowPtr) {
        let ptr_value = ptr.as_ptr() as u64;
        // Pointer shouldn't use upper bits
        debug_assert!(ptr_value & SALT_MASK == 0);
        // Value should have all 1's in the pointer area
        debug_assert!(self.0 & POINTER_MASK == POINTER_MASK);

        self.0 &= ptr_value | SALT_MASK;
    }
}

impl Debug for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Entry")
            .field(&self.get_salt())
            .field(&self.get_pointer())
            .finish()
    }
}

pub(super) trait TableAdapter {
    fn append_rows(&mut self, state: &mut ProbeState, new_entry_count: usize);

    fn compare(
        &mut self,
        state: &mut ProbeState,
        need_compare_count: usize,
        no_match_count: usize,
    ) -> usize;
}

impl HashIndex {
    pub fn probe_and_create(
        &mut self,
        state: &mut ProbeState,
        row_count: usize,
        mut adapter: impl TableAdapter,
    ) -> usize {
        for (i, row) in state.no_match_vector[..row_count].iter_mut().enumerate() {
            *row = i.into();
            state.slots[i] = self.init_slot(state.group_hashes[i]);
        }

        let mut new_group_count = 0;
        let mut remaining_entries = row_count;

        while remaining_entries > 0 {
            let mut new_entry_count = 0;
            let mut need_compare_count = 0;
            let mut no_match_count = 0;

            // 1. inject new_group_count, new_entry_count, need_compare_count, no_match_count
            for row in state.no_match_vector[..remaining_entries].iter().copied() {
                let slot = &mut state.slots[row];
                let is_new;

                (*slot, is_new) = self.find_or_insert(*slot, state.group_hashes[row]);
                if is_new {
                    state.empty_vector[new_entry_count] = row;
                    new_entry_count += 1;
                } else {
                    state.group_compare_vector[need_compare_count] = row;
                    need_compare_count += 1;
                }
            }

            // 2. append new_group_count to payload
            if new_entry_count != 0 {
                new_group_count += new_entry_count;

                adapter.append_rows(state, new_entry_count);

                for row in state.empty_vector[..new_entry_count].iter().copied() {
                    let entry = self.mut_entry(state.slots[row]);
                    entry.set_pointer(state.addresses[row]);
                    debug_assert_eq!(entry.get_pointer(), state.addresses[row]);
                }
            }

            // 3. set address of compare vector
            if need_compare_count > 0 {
                for row in state.group_compare_vector[..need_compare_count]
                    .iter()
                    .copied()
                {
                    let entry = self.mut_entry(state.slots[row]);

                    debug_assert!(entry.is_occupied());
                    debug_assert_eq!(entry.get_salt(), (state.group_hashes[row] >> 48) as u16);
                    state.addresses[row] = entry.get_pointer();
                }

                // 4. compare
                no_match_count = adapter.compare(state, need_compare_count, no_match_count);
            }

            // 5. Linear probing with hash-derived step
            for row in state.no_match_vector[..no_match_count].iter().copied() {
                let slot = &mut state.slots[row];
                let hash = state.group_hashes[row];
                let mask = self.partition(slot.partition).capacity_mask;
                slot.offset = next_slot(slot.offset, hash, mask);
            }
            remaining_entries = no_match_count;
        }

        self.count += new_group_count;

        new_group_count
    }
}

pub(super) struct AdapterImpl<'a> {
    pub payload: &'a mut PartitionedPayload,
    pub group_columns: ProjectedBlock<'a>,
}

impl<'a> TableAdapter for AdapterImpl<'a> {
    fn append_rows(&mut self, state: &mut ProbeState, count: usize) {
        self.payload.append_rows(state, count, self.group_columns);
    }

    fn compare(
        &mut self,
        state: &mut ProbeState,
        need_compare_count: usize,
        no_match_count: usize,
    ) -> usize {
        // todo: compare hash first if NECESSARY
        CompareState {
            address: &state.addresses,
            compare: &mut state.group_compare_vector,
            no_matched: &mut state.no_match_vector,
        }
        .row_match_entries(
            self.group_columns,
            &self.payload.row_layout,
            (need_compare_count, no_match_count),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;

    use super::*;
    use crate::ProbeState;

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
                let slot = hash_index.probe_slot(hash);

                // set value
                let entry = hash_index.mut_entry(slot);
                debug_assert!(!entry.is_occupied());
                entry.set_hash(hash);
                let row_ptr = self.get_row_ptr(false, i);
                entry.set_pointer(row_ptr);
            }
        }

        fn get_row_ptr(&self, incoimg: bool, row: usize) -> RowPtr {
            RowPtr::new(unsafe {
                self.pin_data
                    .as_ptr()
                    .add(if incoimg { row + self.init_count } else { row }) as _
            })
        }

        fn get_payload(&self, row_ptr: RowPtr) -> (u64, u64, u64) {
            let index = row_ptr.as_ptr() as usize - self.pin_data.as_ptr() as usize;
            self.payload[index]
        }
    }

    impl TableAdapter for &mut TestTableAdapter {
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

                let (key, hash, _) = self.get_payload(state.addresses[row]);

                const POINTER_MASK: u64 = 0x0000FFFFFFFFFFFF;
                assert_eq!(incoming.1 | POINTER_MASK, hash | POINTER_MASK);
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
            let mut hash_index = HashIndex::with_capacity(capacity, usize::MAX);

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
    fn test_probe_walk_covers_full_capacity() {
        // This test make sure that we can always cover all slots in the table
        let capacity = 16;
        let hash_index = HashIndex::with_capacity(capacity, 1 << 16);
        let capacity_mask = hash_index.partition(0).capacity_mask;

        for high_bits in 0u64..(1 << INCREMENT_BITS) {
            let hash = high_bits << (64 - INCREMENT_BITS);
            let mut slot = hash_index.init_slot(hash);
            let mut visited = HashSet::with_capacity(capacity);

            for _ in 0..capacity {
                assert!(
                    visited.insert(slot),
                    "hash {hash:#x} revisited slot {slot} before covering the table"
                );
                slot.offset = next_slot(slot.offset, hash, capacity_mask);
            }

            assert_eq!(
                capacity,
                visited.len(),
                "hash {hash:#x} failed to cover every slot for capacity {capacity}"
            );
            assert_eq!(
                hash_index.init_slot(hash),
                slot,
                "hash {hash:#x} walk did not return to its start after {capacity} steps"
            );
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
    fn test_hash_index_partitioning_respects_threshold() {
        let threshold = 1 << 16;
        let capacity = threshold * 2;
        let hash_index = HashIndex::with_capacity(capacity, threshold);

        assert_eq!(hash_index.partition_count(), 2);
        assert_eq!(hash_index.partition(0).entries.len(), threshold);
    }
}
