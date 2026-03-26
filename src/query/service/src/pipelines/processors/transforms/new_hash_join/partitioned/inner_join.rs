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
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_expression::types::NullableColumn;

use super::partitioned_build::PartitionedHashJoinState;
use super::partitioned_build::ProbeData;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::new_hash_join::common::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::common::join::InnerHashJoinFilterStream;
use crate::pipelines::processors::transforms::new_hash_join::common::join::Join;
use crate::pipelines::processors::transforms::new_hash_join::common::join::JoinStream;
use crate::pipelines::processors::transforms::new_hash_join::common::probe_stream::ProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::common::probe_stream::ProbedRows;
use crate::pipelines::processors::transforms::unpartitioned::PerformanceContext;

pub struct PartitionedInnerJoin {
    build: PartitionedHashJoinState,
    desc: Arc<HashJoinDesc>,
    function_ctx: Arc<FunctionContext>,
    context: PerformanceContext,
}

impl PartitionedInnerJoin {
    pub fn create(
        method: HashMethodKind,
        desc: Arc<HashJoinDesc>,
        function_ctx: FunctionContext,
        max_block_size: usize,
    ) -> Self {
        let context =
            PerformanceContext::create(max_block_size, desc.clone(), function_ctx.clone());

        let function_ctx = Arc::new(function_ctx);

        PartitionedInnerJoin {
            function_ctx: function_ctx.clone(),
            build: PartitionedHashJoinState::create(method, desc.clone(), function_ctx),
            max_block_size,
            desc,
            context,
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

        let probe_keys = self.desc.probe_key(&data, &self.function_ctx)?;

        let mut keys = DataBlock::new(probe_keys, data.num_rows());
        let valids = match self.desc.from_correlated_subquery {
            true => None,
            false => self.desc.build_valids_by_keys(&keys)?,
        };

        self.desc.remove_keys_nullable(&mut keys);
        let probe_block = data.project(&self.desc.probe_projection);

        let probe_data = ProbeData::new(keys, valids);
        let probe_keys_stream = self.build.probe::<true>(probe_data)?;
        let joined_stream = PartitionedInnerJoinStream::create(
            probe_block,
            &self.build,
            probe_keys_stream,
            self.desc.clone(),
            &mut self.context.probe_result,
        );

        match &mut self.context.filter_executor {
            None => Ok(joined_stream),
            Some(filter_executor) => Ok(InnerHashJoinFilterStream::create(
                joined_stream,
                filter_executor,
            )),
        }
    }
}

struct PartitionedInnerJoinStream<'a> {
    desc: Arc<HashJoinDesc>,
    probe_data_block: DataBlock,
    build: &'a PartitionedHashJoinState,
    probe_keys_stream: Box<dyn ProbeStream + 'a>,
    probed_rows: &'a mut ProbedRows,
}

impl<'a> PartitionedInnerJoinStream<'a> {
    pub fn create(
        probe_data_block: DataBlock,
        build: &'a PartitionedHashJoinState,
        probe_keys_stream: Box<dyn ProbeStream + 'a>,
        desc: Arc<HashJoinDesc>,
        probed_rows: &'a mut ProbedRows,
    ) -> Box<dyn JoinStream + 'a> {
        Box::new(PartitionedInnerJoinStream {
            desc,
            build,
            probed_rows,
            probe_data_block,
            probe_keys_stream,
        })
    }
}

impl<'a> JoinStream for PartitionedInnerJoinStream<'a> {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        loop {
            self.probed_rows.clear();
            let max_rows = self.probed_rows.matched_probe.capacity();
            self.probe_keys_stream.advance(self.probed_rows, max_rows)?;

            if self.probed_rows.is_empty() {
                return Ok(None);
            }

            if self.probed_rows.is_all_unmatched() {
                continue;
            }

            let probe_block = match self.probe_data_block.num_columns() {
                0 => None,
                _ => Some(DataBlock::take(
                    &self.probe_data_block,
                    self.probed_rows.matched_probe.as_slice(),
                )?),
            };

            let build_block = match self.build.columns.is_empty() {
                true => None,
                false => {
                    let row_ptrs = self.probed_rows.matched_build.as_slice();
                    Some(DataBlock::take_column_vec(
                        self.build.columns.as_slice(),
                        self.build.column_types.as_slice(),
                        row_ptrs,
                    ))
                }
            };

            let mut result_block = match (probe_block, build_block) {
                (Some(mut probe_block), Some(build_block)) => {
                    probe_block.merge_block(build_block);
                    probe_block
                }
                (Some(probe_block), None) => probe_block,
                (None, Some(build_block)) => build_block,
                (None, None) => DataBlock::new(vec![], self.probed_rows.matched_build.len()),
            };

            for (index, (is_probe_nullable, is_build_nullable)) in
                self.desc.probe_to_build.iter().cloned()
            {
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

            return Ok(Some(result_block));
        }
    }
}
