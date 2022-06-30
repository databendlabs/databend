// Copyright 2022 Datafuse Labs.
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

use super::row::RowPtr;

/// ProbeState used for probe phase of hash join.
/// We may need some reuseable state for probe phase.
pub struct ProbeState {
    pub(crate) probe_indexs: Vec<u32>,
    pub(crate) build_indexs: Vec<RowPtr>,

    // In the probe phase, the probe block with N rows could join result into M rows
    // e.g.: [0, 1, 2, 3]  results into [0, 1, 2, 2, 3]
    // probe_indexs: the result index to the probe block row -> [0, 1, 2, 2, 3]
    // row_state:  the state (counter) of the probe block row -> [1, 1, 2, 1]
    pub(crate) row_state: Vec<u32>,
}

impl ProbeState {
    pub fn clear(&mut self) {
        self.probe_indexs.clear();
        self.build_indexs.clear();
        self.row_state.clear();
    }

    pub fn with_capacity(capacity: usize) -> Self {
        ProbeState {
            probe_indexs: Vec::with_capacity(capacity),
            build_indexs: Vec::with_capacity(capacity),
            row_state: Vec::with_capacity(capacity),
        }
    }
}
