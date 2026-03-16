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
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::FilterExecutor;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_expression::types::NullableColumn;
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::partitioned_build::PartitionedBuild;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::new_hash_join::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::Join;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::OneBlockJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::grace::grace_memory::GraceMemoryJoin;

pub struct PartitionedInnerJoin {
    build: PartitionedBuild,
    filter_executor: Option<FilterExecutor>,
}

impl PartitionedInnerJoin {
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
        PartitionedInnerJoin {
            build: PartitionedBuild::create(method, desc, function_ctx),
            filter_executor,
        }
    }

    fn result_block(
        desc: &HashJoinDesc,
        probe_block: Option<DataBlock>,
        build_block: Option<DataBlock>,
        num_rows: usize,
    ) -> DataBlock {
        let mut result_block = match (probe_block, build_block) {
            (Some(mut p), Some(b)) => {
                p.merge_block(b);
                p
            }
            (Some(p), None) => p,
            (None, Some(b)) => b,
            (None, None) => DataBlock::new(vec![], num_rows),
        };

        for (index, (is_probe_nullable, is_build_nullable)) in desc.probe_to_build.iter().cloned() {
            let entry = match (is_probe_nullable, is_build_nullable) {
                (true, true) | (false, false) => result_block.get_by_offset(index).clone(),
                (true, false) => result_block.get_by_offset(index).clone().remove_nullable(),
                (false, true) => {
                    let entry = result_block.get_by_offset(index);
                    let col = entry.to_column();
                    match col.is_null() || col.is_nullable() {
                        true => entry.clone(),
                        false => BlockEntry::from(NullableColumn::new_column(
                            col,
                            Bitmap::new_constant(true, result_block.num_rows()),
                        )),
                    }
                }
            };
            result_block.add_entry(entry);
        }
        result_block
    }
}

impl Join for PartitionedInnerJoin {
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

        let (matched_probe, matched_build, _) = self.build.probe(&data)?;

        if matched_probe.is_empty() {
            return Ok(Box::new(EmptyJoinStream));
        }

        let projected = data.project(&self.build.desc.probe_projection);
        let probe_block = match projected.num_columns() {
            0 => None,
            _ => Some(DataBlock::take(&projected, matched_probe.as_slice())?),
        };

        let build_block = self.build.gather_build_block(&matched_build);

        let mut result = Self::result_block(
            &self.build.desc,
            probe_block,
            build_block,
            matched_probe.len(),
        );

        if let Some(filter) = self.filter_executor.as_mut() {
            result = filter.filter(result)?;
            if result.is_empty() {
                return Ok(Box::new(EmptyJoinStream));
            }
        }

        Ok(Box::new(OneBlockJoinStream(Some(result))))
    }
}

impl GraceMemoryJoin for PartitionedInnerJoin {
    fn reset_memory(&mut self) {
        self.build.reset();
    }
}
