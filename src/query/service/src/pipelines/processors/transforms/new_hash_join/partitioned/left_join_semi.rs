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

use super::partitioned_build::PartitionedHashJoinState;
use super::partitioned_build::ProbeData;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::new_hash_join::common::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::common::join::Join;
use crate::pipelines::processors::transforms::new_hash_join::common::join::JoinStream;
use crate::pipelines::processors::transforms::new_hash_join::common::probe_stream::ProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::common::probe_stream::ProbedRows;
use crate::pipelines::processors::transforms::new_hash_join::unpartitioned::memory::left_join::final_result_block;
use crate::pipelines::processors::transforms::unpartitioned::PerformanceContext;

pub struct PartitionedLeftSemiJoin {
    build: PartitionedHashJoinState,
    desc: Arc<HashJoinDesc>,
    function_ctx: Arc<FunctionContext>,
    context: PerformanceContext,
}

impl PartitionedLeftSemiJoin {
    pub fn create(
        method: HashMethodKind,
        desc: Arc<HashJoinDesc>,
        function_ctx: FunctionContext,
        max_block_size: usize,
    ) -> Self {
        let context =
            PerformanceContext::create(max_block_size, desc.clone(), function_ctx.clone());

        let function_ctx = Arc::new(function_ctx);

        PartitionedLeftSemiJoin {
            function_ctx: function_ctx.clone(),
            build: PartitionedHashJoinState::create(method, desc.clone(), function_ctx),
            desc,
            context,
        }
    }
}

impl Join for PartitionedLeftSemiJoin {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        self.build.add_block::<false>(data)
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        self.build.final_build()
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        if data.is_empty() || self.build.num_rows == 0 {
            return Ok(Box::new(EmptyJoinStream));
        }

        let num_probe_rows = data.num_rows();
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

        match &mut self.context.filter_executor {
            None => Ok(LeftSemiHashJoinStream::create(
                probe_block,
                probe_keys_stream,
                &mut self.context.probe_result,
            )),
            Some(filter_executor) => Ok(LeftSemiFilterHashJoinStream::create(
                probe_block,
                &self.build,
                probe_keys_stream,
                self.desc.clone(),
                &mut self.context.probe_result,
                filter_executor,
                num_probe_rows,
            )),
        }
    }
}

struct LeftSemiHashJoinStream<'a> {
    probe_data_block: DataBlock,
    probe_keys_stream: Box<dyn ProbeStream + 'a>,
    probed_rows: &'a mut ProbedRows,
}

impl<'a> LeftSemiHashJoinStream<'a> {
    pub fn create(
        probe_data_block: DataBlock,
        probe_keys_stream: Box<dyn ProbeStream + 'a>,
        probed_rows: &'a mut ProbedRows,
    ) -> Box<dyn JoinStream + 'a> {
        Box::new(LeftSemiHashJoinStream {
            probed_rows,
            probe_data_block,
            probe_keys_stream,
        })
    }
}

impl<'a> JoinStream for LeftSemiHashJoinStream<'a> {
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

            return Ok(Some(DataBlock::take(
                &self.probe_data_block,
                self.probed_rows.matched_probe.as_slice(),
            )?));
        }
    }
}

struct LeftSemiFilterHashJoinStream<'a> {
    desc: Arc<HashJoinDesc>,
    probe_data_block: Option<DataBlock>,
    build: &'a PartitionedHashJoinState,
    probe_keys_stream: Box<dyn ProbeStream + 'a>,
    probed_rows: &'a mut ProbedRows,
    filter_executor: &'a mut FilterExecutor,
    selected: Vec<bool>,
}

impl<'a> LeftSemiFilterHashJoinStream<'a> {
    pub fn create(
        probe_data_block: DataBlock,
        build: &'a PartitionedHashJoinState,
        probe_keys_stream: Box<dyn ProbeStream + 'a>,
        desc: Arc<HashJoinDesc>,
        probed_rows: &'a mut ProbedRows,
        filter_executor: &'a mut FilterExecutor,
        num_probe_rows: usize,
    ) -> Box<dyn JoinStream + 'a> {
        Box::new(LeftSemiFilterHashJoinStream {
            desc,
            build,
            probed_rows,
            filter_executor,
            probe_keys_stream,
            probe_data_block: Some(probe_data_block),
            selected: vec![false; num_probe_rows],
        })
    }
}

impl<'a> JoinStream for LeftSemiFilterHashJoinStream<'a> {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        let Some(probe_data_block) = self.probe_data_block.take() else {
            return Ok(None);
        };

        loop {
            self.probed_rows.clear();
            let max_rows = self.probed_rows.matched_probe.capacity();
            self.probe_keys_stream.advance(self.probed_rows, max_rows)?;

            if self.probed_rows.is_empty() {
                break;
            }

            if self.probed_rows.is_all_unmatched() {
                continue;
            }

            let probe_block = match probe_data_block.num_columns() {
                0 => None,
                _ => Some(DataBlock::take(
                    &probe_data_block,
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

            let num_matched = self.probed_rows.matched_probe.len();
            let result = final_result_block(&self.desc, probe_block, build_block, num_matched);

            let selected_rows = self.filter_executor.select(&result)?;

            if selected_rows == result.num_rows() {
                for probe_idx in &self.probed_rows.matched_probe {
                    self.selected[*probe_idx as usize] = true;
                }
            } else if selected_rows != 0 {
                let selection = self.filter_executor.true_selection();
                for idx in selection[..selected_rows].iter() {
                    let idx = self.probed_rows.matched_probe[*idx as usize];
                    self.selected[idx as usize] = true;
                }
            }
        }

        let bitmap = Bitmap::from_trusted_len_iter(self.selected.iter().copied());

        match bitmap.true_count() {
            0 => Ok(None),
            _ => Ok(Some(probe_data_block.filter_with_bitmap(&bitmap)?)),
        }
    }
}
