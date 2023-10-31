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

use crate::select_vector::SelectVector;
use crate::Column;
use crate::StateAddr;

/// ProbeState is the state to probe HT
/// It could be reuse during multiple probe process
#[derive(Default, Debug)]
pub struct ProbeState {
    pub ht_offsets: Vec<usize>,
    pub hash_salts: Vec<u16>,
    pub addresses: Vec<*const u8>,
    pub state_places: Vec<StateAddr>,
    pub group_compare_vector: SelectVector,
    pub no_match_vector: SelectVector,
    pub empty_vector: SelectVector,
    pub new_groups: SelectVector,

    pub group_columns: Vec<Column>,
    pub row_count: usize,
}

impl ProbeState {
    pub fn ajust_group_columns(
        &mut self,
        group_columns: &[Column],
        hashes: &[u64],
        row_count: usize,
        ht_size: usize,
    ) {
        self.group_columns = group_columns.to_owned();
        self.ajust_row_count(row_count);

        for ((hash, salt), ht_offset) in hashes
            .iter()
            .zip(self.hash_salts.iter_mut())
            .zip(self.ht_offsets.iter_mut())
        {
            *salt = (*hash >> (64 - 16)) as u16;
            *ht_offset = (*hash & (ht_size as u64 - 1)) as usize;
        }
    }

    pub fn ajust_row_count(&mut self, row_count: usize) {
        if self.row_count < row_count {
            self.ht_offsets.resize(row_count, 0);
            self.hash_salts.resize(row_count, 0);
            self.addresses.resize(row_count, 0 as *const u8);
            self.state_places.resize(row_count, StateAddr::new(0));

            self.group_compare_vector.resize(row_count);
            self.no_match_vector.resize(row_count);
            self.empty_vector.resize(row_count);
            self.new_groups.resize(row_count);
        }
        self.row_count = row_count;
    }
}
