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

use crate::SelectVector;
use crate::StateAddr;

/// ProbeState is the state to probe HT
/// It could be reuse during multiple probe process
#[derive(Debug)]
pub struct ProbeState {
    pub group_hashes: Vec<u64>,
    pub addresses: Vec<*const u8>,
    pub state_places: Vec<StateAddr>,
    pub group_compare_vector: SelectVector,
    pub no_match_vector: SelectVector,
    pub empty_vector: SelectVector,
    pub temp_vector: SelectVector,

    pub row_count: usize,
}

unsafe impl Send for ProbeState {}
unsafe impl Sync for ProbeState {}

impl ProbeState {
    pub fn with_capacity(len: usize) -> Self {
        Self {
            group_hashes: vec![0; len],
            addresses: vec![std::ptr::null::<u8>(); len],
            state_places: vec![StateAddr::new(0); len],
            group_compare_vector: vec![0; len],
            no_match_vector: vec![0; len],
            empty_vector: vec![0; len],
            temp_vector: vec![0; len],
            row_count: 0,
        }
    }

    pub fn adjust_vector(&mut self, row_count: usize) {
        if self.group_hashes.len() < row_count {
            self.group_hashes.resize(row_count, 0);
            self.addresses.resize(row_count, std::ptr::null::<u8>());
            self.state_places.resize(row_count, StateAddr::new(0));

            self.group_compare_vector.resize(row_count, 0);
            self.no_match_vector.resize(row_count, 0);
            self.empty_vector.resize(row_count, 0);
            self.temp_vector.resize(row_count, 0);
        }

        self.row_count = row_count;
    }
}
