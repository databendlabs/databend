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

use std::collections::HashMap;

use super::hash_index::*;
use super::RowPtr;
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
        let mut state = ProbeState::default();
        state.row_count = self.incoming.len();
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
        for row in state.empty_vector[..new_entry_count].iter().copied() {
            let (key, hash) = self.incoming[row];
            let value = key + 20;

            self.payload.push((key, hash, value));
            state.addresses[row] = self.get_row_ptr(true, row);
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

            let row_ptr = state.addresses[row];

            let (key, hash, _) = self.get_payload(row_ptr);

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
    fn run(self) {
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

        let count = hash_index.probe_and_create(&mut state, adapter.incoming.len(), &mut adapter);

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
fn test_probe_and_create() {
    TestCase {
        capacity: 16,
        incoming: vec![(1, 123), (2, 456), (3, 123), (4, 44)],
        payload: vec![(4, 44, 77)],
        want_count: 3,
        want: HashMap::from_iter([(1, 21), (2, 22), (3, 23), (4, 77)]),
    }
    .run();

    TestCase {
        capacity: 16,
        incoming: vec![(1, 11 << 48), (2, 22 << 48), (3, 33 << 48), (4, 44 << 48)],
        payload: vec![(4, 44 << 48, 77)],
        want_count: 3,
        want: HashMap::from_iter([(1, 21), (2, 22), (3, 23), (4, 77)]),
    }
    .run();
}
