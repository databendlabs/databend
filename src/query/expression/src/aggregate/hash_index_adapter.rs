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

use super::PartitionedPayload;
use super::ProbeState;
use super::payload_row::CompareState;
use crate::ProjectedBlock;

pub(super) trait TableAdapter {
    fn append_rows(&mut self, state: &mut ProbeState, new_entry_count: usize);

    fn compare(
        &mut self,
        state: &mut ProbeState,
        need_compare_count: usize,
        no_match_count: usize,
    ) -> usize;
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
