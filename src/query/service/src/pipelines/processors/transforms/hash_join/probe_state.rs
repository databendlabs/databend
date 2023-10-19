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

use common_arrow::arrow::bitmap::Bitmap;
use common_expression::FunctionContext;
use common_hashtable::RowPtr;

use super::desc::MARKER_KIND_FALSE;
use crate::sql::plans::JoinType;

/// ProbeState used for probe phase of hash join.
/// We may need some reusable state for probe phase.
pub struct ProbeState {
    pub(crate) max_block_size: usize,
    pub(crate) probe_indexes: Vec<u32>,
    pub(crate) build_indexes: Vec<RowPtr>,
    pub(crate) valids: Option<Bitmap>,
    pub(crate) true_validity: Bitmap,
    pub(crate) func_ctx: FunctionContext,
    // In the probe phase, the probe block with N rows could join result into M rows
    // e.g.: [0, 1, 2, 3]  results into [0, 1, 2, 2, 3]
    // probe_indexes: the result index to the probe block row -> [0, 1, 2, 2, 3]
    // row_state:  the state (counter) of the probe block row -> [1, 1, 2, 1]
    pub(crate) row_state: Option<Vec<usize>>,
    pub(crate) row_state_indexes: Option<Vec<usize>>,
    pub(crate) probe_unmatched_indexes: Option<Vec<u32>>,
    pub(crate) markers: Option<Vec<u8>>,
    pub(crate) string_items_buf: Option<Vec<(u64, usize)>>,
}

impl ProbeState {
    pub fn clear(&mut self) {
        self.valids = None;
    }

    pub fn create(
        max_block_size: usize,
        join_type: &JoinType,
        with_conjunct: bool,
        has_string_column: bool,
        func_ctx: FunctionContext,
    ) -> Self {
        let true_validity = Bitmap::new_constant(true, max_block_size);
        let (row_state, row_state_indexes, probe_unmatched_indexes) = match &join_type {
            JoinType::Left | JoinType::LeftSingle | JoinType::Full => {
                if with_conjunct {
                    (
                        Some(vec![0; max_block_size]),
                        Some(vec![0; max_block_size]),
                        None,
                    )
                } else {
                    (
                        Some(vec![0; max_block_size]),
                        None,
                        Some(vec![0; max_block_size]),
                    )
                }
            }
            _ => (None, None, None),
        };
        let markers = if matches!(&join_type, JoinType::RightMark) {
            Some(vec![MARKER_KIND_FALSE; max_block_size])
        } else {
            None
        };
        let string_items_buf = if has_string_column {
            Some(vec![(0, 0); max_block_size])
        } else {
            None
        };
        ProbeState {
            max_block_size,
            probe_indexes: vec![0; max_block_size],
            build_indexes: vec![
                RowPtr {
                    chunk_index: 0,
                    row_index: 0,
                };
                max_block_size
            ],
            valids: None,
            true_validity,
            func_ctx,
            row_state,
            row_state_indexes,
            probe_unmatched_indexes,
            markers,
            string_items_buf,
        }
    }

    // Reset some states which changed during probe.
    // Only be called when spill is enabled.
    pub fn reset(&mut self) {
        self.probe_indexes = vec![0; self.max_block_size];
        self.build_indexes = vec![
            RowPtr {
                chunk_index: 0,
                row_index: 0,
            };
            self.max_block_size
        ];
        self.valids = None;
        self.row_state = None;
        self.row_state_indexes = None;
        self.probe_unmatched_indexes = None;
        self.markers = None;
    }
}
