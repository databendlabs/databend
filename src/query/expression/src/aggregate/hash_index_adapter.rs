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

use super::BATCH_SIZE;
use super::PartitionedPayload;
use super::ProbeState;
use super::StateAddr;
use super::partitioned_payload::RowReserver;
use super::payload::GroupRows;
use super::payload_row::CompareState;
use super::row_ptr::RowDecoder;

pub(super) trait TableAdapter {
    fn append_rows(&mut self, state: &mut ProbeState, new_entry_count: usize);

    /// Start reserving the rows of new groups, see `PartitionedPayload::begin_rows`.
    fn begin_rows(&mut self);

    /// Reserve the rows of `partition` in a new page.
    fn open_rows(&mut self, partition: usize);

    /// Decodes the row ids of the index, and reserves the rows of new groups.
    fn rows(&mut self) -> (RowDecoder<'_>, RowReserver<'_>);

    fn compare(
        &mut self,
        state: &mut ProbeState,
        need_compare_count: usize,
        no_match_count: usize,
    ) -> usize;
}

pub(super) struct AdapterImpl<'a> {
    pub payload: &'a mut PartitionedPayload,
    pub groups: GroupRows<'a>,
    /// The states to copy into the new groups, see `AggregateHashTable::combine_payload`.
    pub source_states: Option<&'a [StateAddr; BATCH_SIZE]>,
}

impl<'a> TableAdapter for AdapterImpl<'a> {
    fn append_rows(&mut self, state: &mut ProbeState, count: usize) {
        if self.source_states.is_some() {
            for row in &state.empty_vector[..count] {
                state.new_groups[*row] = true;
            }
        }
        self.payload
            .append_rows(state, count, self.groups, self.source_states);
    }

    fn begin_rows(&mut self) {
        self.payload.begin_rows();
    }

    fn open_rows(&mut self, partition: usize) {
        self.payload.open_rows(partition);
    }

    #[inline]
    fn rows(&mut self) -> (RowDecoder<'_>, RowReserver<'_>) {
        self.payload.rows()
    }

    fn compare(
        &mut self,
        state: &mut ProbeState,
        need_compare_count: usize,
        no_match_count: usize,
    ) -> usize {
        let compare = CompareState {
            address: &state.addresses,
            compare: &mut state.group_compare_vector,
            no_matched: &mut state.no_match_vector,
        };
        let counts = (need_compare_count, no_match_count);
        match self.groups {
            GroupRows::Columns(columns) => {
                compare.row_match_entries(columns, &self.payload.row_layout, counts)
            }
            GroupRows::Rows(rows) => compare.row_match_rows(
                rows,
                &self.payload.group_types,
                &self.payload.row_layout,
                counts,
            ),
        }
    }
}
