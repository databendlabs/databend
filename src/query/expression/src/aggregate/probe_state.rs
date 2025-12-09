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

use super::row_ptr::RowPtr;
use super::StateAddr;
use super::BATCH_SIZE;

pub type SelectVector = [usize; BATCH_SIZE];

/// ProbeState is the state to probe HT
/// It could be reuse during multiple probe process
#[derive(Debug)]
pub struct ProbeState {
    pub(super) group_hashes: [u64; BATCH_SIZE],
    pub(super) addresses: [RowPtr; BATCH_SIZE],
    pub(super) page_index: [usize; BATCH_SIZE],
    pub(super) state_places: [StateAddr; BATCH_SIZE],

    pub(super) empty_vector: SelectVector,

    pub(super) group_compare_vector: SelectVector,
    pub(super) no_match_vector: SelectVector,
    pub(super) row_count: usize,

    pub partition_entries: Vec<(usize, SelectVector)>,
    pool: Vec<Vec<usize>>,
}

impl Default for ProbeState {
    fn default() -> Self {
        Self {
            group_hashes: [0; BATCH_SIZE],
            addresses: [RowPtr::null(); BATCH_SIZE],
            page_index: [0; BATCH_SIZE],
            state_places: [StateAddr::null(); BATCH_SIZE],
            group_compare_vector: [0; BATCH_SIZE],
            no_match_vector: [0; BATCH_SIZE],
            empty_vector: [0; BATCH_SIZE],

            row_count: 0,
            partition_entries: vec![],
            pool: vec![],
        }
    }
}

unsafe impl Send for ProbeState {}
unsafe impl Sync for ProbeState {}

impl ProbeState {
    pub fn new_boxed() -> Box<ProbeState> {
        let mut state = Box::<ProbeState>::new_zeroed();
        unsafe {
            let uninit = state.assume_init_mut();
            uninit.partition_entries = vec![];
            uninit.pool = vec![];

            for ptr in &mut uninit.addresses {
                *ptr = RowPtr::null();
            }
            for addr in &mut uninit.state_places {
                *addr = StateAddr::null()
            }

            state.assume_init()
        }
    }

    pub(super) fn reset_partitions(&mut self, partition_count: usize) {
        if partition_count > self.partition_entries.capacity() {
            self.partition_entries
                .reserve(partition_count - self.partition_entries.capacity());
        }
        unsafe {
            self.partition_entries.set_len(partition_count);
        }
        for (count, _) in &mut self.partition_entries {
            *count = 0;
        }
    }

    pub(super) fn get_temp(&mut self) -> Vec<usize> {
        match self.pool.pop() {
            Some(mut vec) => {
                vec.clear();
                vec
            }
            None => vec![],
        }
    }

    pub(super) fn save_temp(&mut self, vec: Vec<usize>) {
        self.pool.push(vec);
    }
}
