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

use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::DataSchemaRefExt;

use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::JoinHashTable;

impl JoinHashTable {
    pub(crate) fn probe_cross_join(
        &self,
        input: &DataBlock,
        _probe_state: &mut ProbeState,
    ) -> Result<Vec<DataBlock>> {
        let build_blocks = self.row_space.datablocks();
        let num_rows = build_blocks
            .iter()
            .fold(0, |acc, block| acc + block.num_rows());
        if build_blocks.is_empty() || num_rows == 0 {
            let mut fields = input
                .columns()
                .iter()
                .enumerate()
                .map(|(i, entry)| DataField::new(&i.to_string(), entry.data_type.clone()))
                .collect::<Vec<_>>();
            fields.extend(self.row_space.data_schema.fields().clone());
            return Ok(vec![DataBlock::empty_with_schema(
                DataSchemaRefExt::create(fields.clone()),
            )]);
        }
        let build_block = DataBlock::concat(&build_blocks)?;
        let mut results = Vec::with_capacity(input.num_rows());
        for i in 0..input.num_rows() {
            let probe_block = DataBlock::take(input, &[i as u32])?;
            results.push(self.merge_with_constant_block(&build_block, &probe_block)?);
        }
        Ok(results)
    }

    // Merge build block and probe block (1 row block)
    pub(crate) fn merge_with_constant_block(
        &self,
        build_block: &DataBlock,
        probe_block: &DataBlock,
    ) -> Result<DataBlock> {
        let columns = Vec::with_capacity(build_block.num_columns() + probe_block.num_columns());
        let mut replicated_probe_block = DataBlock::new(columns, build_block.num_rows());
        for col in probe_block.columns() {
            replicated_probe_block.add_column(col.clone());
        }
        for col in build_block.columns() {
            replicated_probe_block.add_column(col.clone());
        }
        Ok(replicated_probe_block)
    }
}
