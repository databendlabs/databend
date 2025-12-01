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
    pub(super) temp_vector: SelectVector,
    pub(super) slots: SelectVector,

    pub(super) row_count: usize,

    pub partition_entries: Vec<SelectVector>,
    pub partition_count: Vec<usize>,
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
            temp_vector: [0; BATCH_SIZE],
            slots: [0; BATCH_SIZE],

            row_count: 0,
            partition_entries: vec![],
            partition_count: vec![],
        }
    }
}

unsafe impl Send for ProbeState {}
unsafe impl Sync for ProbeState {}

impl ProbeState {
    pub fn new_boxed() -> Box<ProbeState> {
        let mut state = Box::<ProbeState>::new_zeroed();
        unsafe {
            let state_mut = state.assume_init_mut();
            state_mut.partition_entries = vec![];
            state_mut.partition_count = vec![];

            for ptr in &mut state_mut.addresses {
                *ptr = RowPtr::null();
            }
            for addr in &mut state_mut.state_places {
                *addr = StateAddr::null()
            }

            state.assume_init()
        }
    }

    pub fn reset_partitions(&mut self, partition_count: usize) {
        if self.partition_entries.len() < partition_count {
            self.partition_entries
                .resize(partition_count, [0; BATCH_SIZE]);
            self.partition_count.resize(partition_count, 0);
        }

        for i in 0..partition_count {
            self.partition_count[i] = 0;
        }
    }
}
