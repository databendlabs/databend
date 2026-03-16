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
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FilterExecutor;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::left_join::final_result_block;
use super::partitioned_build::PartitionedBuild;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::new_hash_join::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::Join;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::OneBlockJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::grace::grace_memory::GraceMemoryJoin;

pub struct PartitionedLeftSemiJoin {
    build: PartitionedBuild,
    filter_executor: Option<FilterExecutor>,
}

impl PartitionedLeftSemiJoin {
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
        PartitionedLeftSemiJoin {
            build: PartitionedBuild::create(method, desc, function_ctx),
            filter_executor,
        }
    }
}

impl Join for PartitionedLeftSemiJoin {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        self.build.add_block(data)
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        self.build.final_build()?;
        Ok(None)
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        if data.is_empty() || self.build.num_rows == 0 {
            return Ok(Box::new(EmptyJoinStream));
        }

        let desc = &self.build.desc;
        let (matched_probe, matched_build, _) = self.build.probe(&data)?;

        if matched_probe.is_empty() {
            return Ok(Box::new(EmptyJoinStream));
        }

        let probe_projected = data.project(&desc.probe_projection);

        if let Some(filter) = self.filter_executor.as_mut() {
            // With filter: build full result, apply filter, deduplicate probe indices
            let probe_block = match probe_projected.num_columns() {
                0 => None,
                _ => Some(DataBlock::take(&probe_projected, matched_probe.as_slice())?),
            };
            let build_block = self.build.gather_build_block(&matched_build);
            let result = final_result_block(
                &self.build.desc,
                probe_block,
                build_block,
                matched_probe.len(),
            );

            let count = filter.select(&result)?;
            if count == 0 {
                return Ok(Box::new(EmptyJoinStream));
            }

            let true_sel = filter.true_selection();
            let mut selected = vec![false; probe_projected.num_rows()];
            for idx in true_sel.iter().take(count) {
                selected[matched_probe[*idx as usize] as usize] = true;
            }

            let bitmap = Bitmap::from_trusted_len_iter(selected.into_iter());
            match bitmap.true_count() {
                0 => Ok(Box::new(EmptyJoinStream)),
                _ => Ok(Box::new(OneBlockJoinStream(Some(
                    probe_projected.filter_with_bitmap(&bitmap)?,
                )))),
            }
        } else {
            // Without filter: deduplicate matched probe indices
            let mut selected = vec![false; probe_projected.num_rows()];
            for idx in &matched_probe {
                selected[*idx as usize] = true;
            }
            let bitmap = Bitmap::from_trusted_len_iter(selected.into_iter());
            Ok(Box::new(OneBlockJoinStream(Some(
                probe_projected.filter_with_bitmap(&bitmap)?,
            ))))
        }
    }
}

impl GraceMemoryJoin for PartitionedLeftSemiJoin {
    fn reset_memory(&mut self) {
        self.build.reset();
    }
}
