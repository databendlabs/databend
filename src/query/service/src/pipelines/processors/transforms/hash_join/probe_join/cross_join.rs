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

use common_exception::Result;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::Value;

use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::JoinHashTable;

impl JoinHashTable {
    pub(crate) fn probe_cross_join(
        &self,
        input: &DataBlock,
        _probe_state: &mut ProbeState,
    ) -> Result<Vec<DataBlock>> {
        let build_blocks = self.row_space.datablocks();
        let build_num_rows = self
            .build_num_rows
            .load(std::sync::atomic::Ordering::Relaxed);
        let input_num_rows = input.num_rows();
        if build_num_rows == 0 || input_num_rows == 0 {
            return Ok(vec![]);
        }

        let column_nums = input.num_columns();
        let mut columns = Vec::with_capacity(self.probe_projected_columns.len());
        for index in 0..column_nums {
            if !self.probe_projected_columns.contains(&index) {
                continue;
            }
            columns.push(input.get_by_offset(index).clone())
        }

        let probe_block = if columns.is_empty() {
            DataBlock::empty()
        } else {
            DataBlock::new(columns, input_num_rows)
        };
        let build_block = DataBlock::concat(&build_blocks)?;

        let mut result_blocks = Vec::with_capacity(input_num_rows);
        for i in 0..input_num_rows {
            result_blocks.push(self.merge_with_constant_block(
                &build_block,
                &probe_block,
                i,
                build_num_rows,
            )?);
        }
        Ok(result_blocks)
    }

    // Merge build block and probe block (1 row block)
    pub(crate) fn merge_with_constant_block(
        &self,
        build_block: &DataBlock,
        probe_block: &DataBlock,
        take_index: usize,
        build_num_rows: usize,
    ) -> Result<DataBlock> {
        let columns = Vec::with_capacity(build_block.num_columns() + probe_block.num_columns());
        let mut replicated_probe_block = DataBlock::new(columns, build_num_rows);

        for col in probe_block.columns() {
            let value_ref = col.value.as_ref();
            let scalar = unsafe { value_ref.index_unchecked(take_index) };
            replicated_probe_block.add_column(BlockEntry::new(
                col.data_type.clone(),
                Value::Scalar(scalar.to_owned()),
            ));
        }
        for col in build_block.columns() {
            replicated_probe_block.add_column(col.clone());
        }
        Ok(replicated_probe_block)
    }
}
