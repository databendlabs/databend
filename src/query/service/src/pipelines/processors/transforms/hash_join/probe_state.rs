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
use common_hashtable::MarkerKind;
use common_hashtable::RowPtr;

use crate::pipelines::processors::transforms::hash_join::desc::JOIN_MAX_BLOCK_SIZE;

/// ProbeState used for probe phase of hash join.
/// We may need some reuseable state for probe phase.
pub struct ProbeState {
    pub(crate) probe_indexes: Vec<(u32, u32)>,
    pub(crate) build_indexes: Vec<RowPtr>,
    pub(crate) valids: Option<Bitmap>,
    // In the probe phase, the probe block with N rows could join result into M rows
    // e.g.: [0, 1, 2, 3]  results into [0, 1, 2, 2, 3]
    // probe_indexes: the result index to the probe block row -> [0, 1, 2, 2, 3]
    // row_state:  the state (counter) of the probe block row -> [1, 1, 2, 1]
    pub(crate) row_state: Vec<u32>,
    pub(crate) markers: Option<Vec<MarkerKind>>,
}

impl ProbeState {
    pub fn clear(&mut self) {
        self.row_state.clear();
        self.valids = None;
    }

    pub fn with_capacity(capacity: usize) -> Self {
        ProbeState {
            probe_indexes: vec![(0, 0); JOIN_MAX_BLOCK_SIZE],
            build_indexes: vec![
                RowPtr {
                    chunk_index: 0,
                    row_index: 0,
                    marker: None
                };
                JOIN_MAX_BLOCK_SIZE
            ],
            row_state: Vec::with_capacity(capacity),
            valids: None,
            markers: None,
        }
    }
}
