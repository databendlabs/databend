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

use databend_common_expression::types::DataType;
use databend_common_expression::ColumnVec;
use databend_common_expression::DataBlock;

pub struct BuildState {
    // The `generation_state` is used to generate build side `DataBlock`.
    pub(crate) generation_state: BuildBlockGenerationState,
    /// OuterScan map, initialized at `HashJoinBuildState`, used in `HashJoinProbeState`
    pub(crate) outer_scan_map: Vec<Vec<bool>>,
    /// LeftMarkScan map, initialized at `HashJoinBuildState`, used in `HashJoinProbeState`
    pub(crate) mark_scan_map: Vec<Vec<u8>>,
}

impl BuildState {
    pub fn new() -> Self {
        Self {
            generation_state: BuildBlockGenerationState::new(),
            outer_scan_map: Vec::new(),
            mark_scan_map: Vec::new(),
        }
    }
}

pub struct BuildBlockGenerationState {
    pub(crate) build_num_rows: usize,
    /// Data of the build side.
    pub(crate) chunks: Vec<DataBlock>,
    // we converted all chunks into ColumnVec for every column.
    pub(crate) build_columns: Vec<ColumnVec>,
    pub(crate) build_columns_data_type: Vec<DataType>,
    // after projected by build_projection, whether we still have data.
    pub(crate) is_build_projected: bool,
}

impl BuildBlockGenerationState {
    fn new() -> Self {
        Self {
            build_num_rows: 0,
            chunks: Vec::new(),
            build_columns: Vec::new(),
            build_columns_data_type: Vec::new(),
            is_build_projected: true,
        }
    }
}
