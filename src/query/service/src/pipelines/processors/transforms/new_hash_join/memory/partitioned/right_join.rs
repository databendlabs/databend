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

use std::sync::Arc;

use databend_common_base::base::ProgressValues;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FilterExecutor;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::left_join::final_result_block;
use super::left_join::null_block;
use super::partitioned_build::PartitionedBuild;
use super::partitioned_build::flat_to_row_ptr;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::new_hash_join::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::Join;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::OneBlockJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::grace::grace_memory::GraceMemoryJoin;
use crate::pipelines::processors::transforms::wrap_nullable_block;

pub struct PartitionedRightJoin {
    build: PartitionedBuild,
    filter_executor: Option<FilterExecutor>,
    finished: bool,
}

impl PartitionedRightJoin {
    pub fn create(
        method: HashMethodKind,
        desc: Arc<HashJoinDesc>,
        function_ctx: FunctionContext,
        max_block_size: usize,
    ) -> Self {
        let filter_executor = desc.other_predicate.as_ref().map(|predicate| {
            FilterExecutor::new(
                predicate.clone(),
                function_ctx.clone(),
                max_block_size,
                None,
                &BUILTIN_FUNCTIONS,
                false,
            )
        });
        PartitionedRightJoin {
            build: PartitionedBuild::create(method, desc, function_ctx),
            filter_executor,
            finished: false,
        }
    }
}

impl Join for PartitionedRightJoin {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        self.build.add_block(data)
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        self.build.final_build()?;
        self.build.init_visited();
        Ok(None)
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        if data.is_empty() || self.build.num_rows == 0 {
            return Ok(Box::new(EmptyJoinStream));
        }

        let desc = self.build.desc.clone();
        let (matched_probe, matched_build) = self.build.probe_and_mark_visited(&data)?;

        if matched_probe.is_empty() {
            return Ok(Box::new(EmptyJoinStream));
        }

        let probe_projected = data.project(&desc.probe_projection);
        let probe_block = match probe_projected.num_columns() {
            0 => None,
            _ => Some(wrap_nullable_block(&DataBlock::take(
                &probe_projected,
                matched_probe.as_slice(),
            )?)),
        };

        let build_block = self.build.gather_build_block(&matched_build);

        let mut result = final_result_block(
            &desc,
            probe_block,
            build_block,
            matched_build.len(),
        );

        if let Some(filter) = self.filter_executor.as_mut() {
            result = filter.filter(result)?;
            if result.is_empty() {
                return Ok(Box::new(EmptyJoinStream));
            }
        }

        Ok(Box::new(OneBlockJoinStream(Some(result))))
    }

    fn final_probe(&mut self) -> Result<Option<Box<dyn JoinStream + '_>>> {
        if self.finished || self.build.num_rows == 0 {
            return Ok(None);
        }
        self.finished = true;

        let desc = self.build.desc.clone();
        let mut unvisited_ptrs = Vec::new();

        // Scan visited array (1-based indexing)
        for i in 1..=self.build.num_rows {
            if !self.build.hash_table.is_visited(i) {
                unvisited_ptrs.push(flat_to_row_ptr(i));
            }
        }

        if unvisited_ptrs.is_empty() {
            return Ok(None);
        }

        // Build NULL probe block
        let mut probe_types = Vec::new();
        for (i, field) in desc.probe_schema.fields().iter().enumerate() {
            if desc.probe_projection.contains(&i) {
                probe_types.push(field.data_type().clone());
            }
        }
        let probe_block = null_block(&probe_types, unvisited_ptrs.len());
        let build_block = self.build.gather_build_block(&unvisited_ptrs);

        let result = final_result_block(
            &desc,
            probe_block,
            build_block,
            unvisited_ptrs.len(),
        );

        Ok(Some(Box::new(OneBlockJoinStream(Some(result)))))
    }
}

impl GraceMemoryJoin for PartitionedRightJoin {
    fn reset_memory(&mut self) {
        self.finished = false;
        self.build.reset();
    }
}
