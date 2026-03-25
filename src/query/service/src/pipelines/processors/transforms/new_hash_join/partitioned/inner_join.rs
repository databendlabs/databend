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
use crate::pipelines::processors::transforms::new_hash_join::common::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::common::join::Join;
use crate::pipelines::processors::transforms::new_hash_join::common::join::JoinStream;
use crate::pipelines::processors::transforms::new_hash_join::common::probe_stream::ProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::common::probe_stream::ProbedRows;

pub struct PartitionedInnerJoin {
    build: PartitionedBuild,
    filter_executor: Option<FilterExecutor>,
    max_block_size: usize,
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
            max_block_size,
        }
    }
}

pub fn result_block(
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

struct PartitionedInnerJoinStream<'a> {
    desc: Arc<HashJoinDesc>,
    probe_data_block: DataBlock,
    build: &'a PartitionedBuild,
    probe_stream: Box<dyn ProbeStream + Send + Sync + 'a>,
    probed_rows: ProbedRows,
    filter_executor: Option<&'a mut FilterExecutor>,
    max_block_size: usize,
}

impl<'a> JoinStream for PartitionedInnerJoinStream<'a> {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        loop {
            self.probed_rows.clear();
            self.probe_stream
                .advance(&mut self.probed_rows, self.max_block_size)?;

            if self.probed_rows.is_empty() {
                return Ok(None);
            }

            let probe_block = match self.probe_data_block.num_columns() {
                0 => None,
                _ => Some(DataBlock::take(
                    &self.probe_data_block,
                    self.probed_rows.matched_probe.as_slice(),
                )?),
            };
            let build_block = self
                .build
                .gather_build_block(&self.probed_rows.matched_build);
            let num_rows = self.probed_rows.matched_probe.len();

            let mut block = result_block(&self.desc, probe_block, build_block, num_rows);

            if let Some(filter) = self.filter_executor.as_mut() {
                block = filter.filter(block)?;
                if block.is_empty() {
                    continue;
                }
            }

            return Ok(Some(block));
        }
    }
}

impl Join for PartitionedInnerJoin {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        self.build.add_block(data)
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        self.build.final_build()
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        if data.is_empty() || self.build.num_rows == 0 {
            return Ok(Box::new(EmptyJoinStream));
        }

        let probe_stream = self.build.create_probe_matched(&data)?;
        let probe_data_block = data.project(&self.build.desc.probe_projection);

        Ok(Box::new(PartitionedInnerJoinStream {
            desc: self.build.desc.clone(),
            probe_data_block,
            build: &self.build,
            probe_stream,
            probed_rows: ProbedRows::new(
                Vec::with_capacity(self.max_block_size),
                Vec::with_capacity(self.max_block_size),
                Vec::with_capacity(self.max_block_size),
            ),
            filter_executor: self.filter_executor.as_mut(),
            max_block_size: self.max_block_size,
        }))
    }
}
