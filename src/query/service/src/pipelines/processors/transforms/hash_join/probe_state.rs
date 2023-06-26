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
use common_arrow::arrow::bitmap::MutableBitmap;
use common_expression::FunctionContext;
use common_hashtable::RowPtr;

use crate::pipelines::processors::transforms::hash_join::desc::JOIN_MAX_BLOCK_SIZE;
use crate::sql::plans::JoinType;

use super::desc::MARKER_KIND_FALSE;

/// ProbeState used for probe phase of hash join.
/// We may need some reusable state for probe phase.
pub struct ProbeState {
    pub(crate) probe_indexes: Vec<(u32, u32)>,
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
    pub(crate) probe_unmatched_indexes: Option<Vec<(u32, u32)>>,
    pub(crate) markers: Option<Vec<u8>>,
}

impl ProbeState {
    pub fn clear(&mut self) {
        self.valids = None;
    }

    pub fn create(join_type: &JoinType, with_conjunct: bool, func_ctx: FunctionContext) -> Self {
        let mut true_validity = MutableBitmap::new();
        true_validity.extend_constant(JOIN_MAX_BLOCK_SIZE, true);
        let true_validity: Bitmap = true_validity.into();
        let (row_state, row_state_indexes, probe_unmatched_indexes) = match &join_type {
            JoinType::Left | JoinType::Single | JoinType::Full => {
                if with_conjunct {
                    (
                        Some(vec![0; JOIN_MAX_BLOCK_SIZE]),
                        Some(vec![0; JOIN_MAX_BLOCK_SIZE]),
                        None,
                    )
                } else {
                    (
                        Some(vec![0; JOIN_MAX_BLOCK_SIZE]),
                        None,
                        Some(vec![(0, 0); JOIN_MAX_BLOCK_SIZE]),
                    )
                }
            }
            _ => (None, None, None),
        };
        let markers = if matches!(&join_type, JoinType::RightMark) {
            Some(vec![MARKER_KIND_FALSE; JOIN_MAX_BLOCK_SIZE])
        } else {
            None
        };
        ProbeState {
            probe_indexes: vec![(0, 0); JOIN_MAX_BLOCK_SIZE],
            build_indexes: vec![
                RowPtr {
                    chunk_index: 0,
                    row_index: 0,
                };
                JOIN_MAX_BLOCK_SIZE
            ],
            valids: None,
            true_validity,
            func_ctx,
            row_state,
            row_state_indexes,
            markers,
            probe_unmatched_indexes,
        }
    }
}
