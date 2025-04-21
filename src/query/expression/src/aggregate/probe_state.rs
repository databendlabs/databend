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

use crate::new_sel;
use crate::SelectVector;
use crate::StateAddr;
use crate::BATCH_SIZE;

/// ProbeState is the state to probe HT
/// It could be reuse during multiple probe process
pub struct ProbeState {
    pub group_hashes: Vec<u64>,
    pub addresses: Vec<*const u8>,
    pub page_index: Vec<usize>,
    pub state_places: Vec<StateAddr>,
    pub group_compare_vector: SelectVector,
    pub no_match_vector: SelectVector,
    pub empty_vector: SelectVector,
    pub temp_vector: SelectVector,
    pub row_count: usize,

    pub partition_entries: Vec<SelectVector>,
    pub partition_count: Vec<usize>,
}

impl Default for ProbeState {
    fn default() -> Self {
        Self {
            group_hashes: vec![0_u64; BATCH_SIZE],
            addresses: vec![std::ptr::null::<u8>(); BATCH_SIZE],
            page_index: vec![0; BATCH_SIZE],
            state_places: vec![StateAddr::new(0); BATCH_SIZE],
            group_compare_vector: new_sel(),
            no_match_vector: new_sel(),
            empty_vector: new_sel(),
            temp_vector: new_sel(),
            partition_entries: vec![],
            partition_count: vec![],
            row_count: 0,
        }
    }
}

unsafe impl Send for ProbeState {}
unsafe impl Sync for ProbeState {}

impl ProbeState {
    pub fn set_incr_empty_vector(&mut self, row_count: usize) {
        for i in 0..row_count {
            self.empty_vector[i] = i;
        }
    }

    pub fn reset_partitions(&mut self, partition_count: usize) {
        if self.partition_entries.len() < partition_count {
            self.partition_count.resize(partition_count, 0);
            self.partition_entries.resize_with(partition_count, new_sel);
        }

        for i in 0..partition_count {
            self.partition_count[i] = 0;
        }
    }
}
