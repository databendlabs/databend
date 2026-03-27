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
use databend_common_expression::ColumnVec;
use databend_common_expression::DataBlock;
use databend_common_expression::FilterExecutor;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_expression::types::DataType;

use super::partitioned_build::PartitionedHashJoinState;
use super::partitioned_build::ProbeData;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::hash_join_table::RowPtr;
use crate::pipelines::processors::transforms::new_hash_join::common::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::common::join::Join;
use crate::pipelines::processors::transforms::new_hash_join::common::join::JoinStream;
use crate::pipelines::processors::transforms::new_hash_join::common::probe_stream::ProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::common::probe_stream::ProbedRows;
use crate::pipelines::processors::transforms::new_hash_join::unpartitioned::memory::left_join::final_result_block;
use crate::pipelines::processors::transforms::unpartitioned::PerformanceContext;

pub struct PartitionedRightSemiJoin {
    build: PartitionedHashJoinState,
    max_block_size: usize,
    desc: Arc<HashJoinDesc>,
    function_ctx: Arc<FunctionContext>,
    context: PerformanceContext,
    finished: bool,
}

impl PartitionedRightSemiJoin {
    pub fn create(
        method: HashMethodKind,
        desc: Arc<HashJoinDesc>,
        function_ctx: FunctionContext,
        max_block_size: usize,
    ) -> Self {
        let context =
            PerformanceContext::create(max_block_size, desc.clone(), function_ctx.clone());

        let function_ctx = Arc::new(function_ctx);

        PartitionedRightSemiJoin {
            function_ctx: function_ctx.clone(),
            build: PartitionedHashJoinState::create(method, desc.clone(), function_ctx),
            max_block_size,
            desc,
            context,
            finished: false,
        }
    }
}

impl Join for PartitionedRightSemiJoin {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        self.build.add_block::<true>(data)
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
        let valids = self.desc.build_valids_by_keys(&keys)?;

        self.desc.remove_keys_nullable(&mut keys);
        let probe_block = data.project(&self.desc.probe_projection);

        let probe_data = ProbeData::new(keys, valids);
        let probe_keys_stream = self.build.probe::<true, false>(probe_data)?;

        match self.context.filter_executor.as_mut() {
            None => Ok(SemiRightHashJoinStream::<false>::create(
                probe_block,
                &self.build,
                probe_keys_stream,
                self.desc.clone(),
                &mut self.context.probe_result,
                None,
            )),
            Some(filter_executor) => Ok(SemiRightHashJoinStream::<true>::create(
                probe_block,
                &self.build,
                probe_keys_stream,
                self.desc.clone(),
                &mut self.context.probe_result,
                Some(filter_executor),
            )),
        }
    }

    fn final_probe(&mut self) -> Result<Option<Box<dyn JoinStream + '_>>> {
        if self.finished || self.build.num_rows == 0 {
            return Ok(None);
        }
        self.finished = true;

        Ok(Some(Box::new(PartitionedRightSemiFinalStream {
            columns: &self.build.columns,
            column_types: &self.build.column_types,
            visited: &self.build.visited,
            chunk_idx: 0,
            row_idx: 0,
            max_block_size: self.max_block_size,
        })))
    }
}

pub(super) struct SemiRightHashJoinStream<'a, const CONJUNCT: bool> {
    desc: Arc<HashJoinDesc>,
    probe_data_block: DataBlock,
    build: &'a PartitionedHashJoinState,
    probe_keys_stream: Box<dyn ProbeStream + 'a>,
    probed_rows: &'a mut ProbedRows,
    filter_executor: Option<&'a mut FilterExecutor>,
}

impl<'a, const CONJUNCT: bool> SemiRightHashJoinStream<'a, CONJUNCT> {
    pub fn create(
        probe_data_block: DataBlock,
        build: &'a PartitionedHashJoinState,
        probe_keys_stream: Box<dyn ProbeStream + 'a>,
        desc: Arc<HashJoinDesc>,
        probed_rows: &'a mut ProbedRows,
        filter_executor: Option<&'a mut FilterExecutor>,
    ) -> Box<dyn JoinStream + 'a> {
        Box::new(SemiRightHashJoinStream::<'a, CONJUNCT> {
            desc,
            build,
            probed_rows,
            probe_data_block,
            probe_keys_stream,
            filter_executor,
        })
    }
}

impl<'a, const CONJUNCT: bool> JoinStream for SemiRightHashJoinStream<'a, CONJUNCT> {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        loop {
            self.probed_rows.clear();
            let max_rows = self.probed_rows.matched_probe.capacity();
            self.probe_keys_stream.advance(self.probed_rows, max_rows)?;

            if self.probed_rows.is_empty() {
                return Ok(None);
            }

            if self.probed_rows.matched_probe.is_empty() {
                continue;
            }

            if !CONJUNCT {
                for row_ptr in &self.probed_rows.matched_build {
                    unsafe {
                        *self.build.visited[row_ptr.chunk_index as usize]
                            .as_ptr()
                            .add(row_ptr.row_index as usize)
                            .cast_mut() = 1;
                    }
                }
                continue;
            }

            let Some(filter_executor) = self.filter_executor.as_mut() else {
                for row_ptr in &self.probed_rows.matched_build {
                    unsafe {
                        *self.build.visited[row_ptr.chunk_index as usize]
                            .as_ptr()
                            .add(row_ptr.row_index as usize)
                            .cast_mut() = 1;
                    }
                }
                continue;
            };

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

            let result_block = final_result_block(
                &self.desc,
                probe_block,
                build_block,
                self.probed_rows.matched_build.len(),
            );

            if !result_block.is_empty() {
                let result_count = filter_executor.select(&result_block)?;

                if result_count == 0 {
                    continue;
                }

                let true_sel = filter_executor.true_selection();

                for idx in true_sel.iter().take(result_count) {
                    let row_ptr = self.probed_rows.matched_build[*idx as usize];
                    unsafe {
                        *self.build.visited[row_ptr.chunk_index as usize]
                            .as_ptr()
                            .add(row_ptr.row_index as usize)
                            .cast_mut() = 1;
                    }
                }
            }
        }
    }
}

struct PartitionedRightSemiFinalStream<'a> {
    columns: &'a Vec<ColumnVec>,
    column_types: &'a Vec<DataType>,
    visited: &'a Vec<Vec<u8>>,
    chunk_idx: usize,
    row_idx: usize,
    max_block_size: usize,
}

impl<'a> JoinStream for PartitionedRightSemiFinalStream<'a> {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        let mut row_ptrs = Vec::with_capacity(self.max_block_size);
        while self.chunk_idx < self.visited.len() && row_ptrs.len() < self.max_block_size {
            let chunk = &self.visited[self.chunk_idx];
            while self.row_idx < chunk.len() && row_ptrs.len() < self.max_block_size {
                if chunk[self.row_idx] != 0 {
                    row_ptrs.push(RowPtr {
                        chunk_index: self.chunk_idx as u32,
                        row_index: self.row_idx as u32,
                    });
                }
                self.row_idx += 1;
            }
            if self.row_idx >= chunk.len() {
                self.chunk_idx += 1;
                self.row_idx = 0;
            }
        }

        if row_ptrs.is_empty() {
            return Ok(None);
        }

        if self.columns.is_empty() {
            return Ok(Some(DataBlock::new(vec![], row_ptrs.len())));
        }
        Ok(Some(DataBlock::take_column_vec(
            self.columns,
            self.column_types,
            &row_ptrs,
        )))
    }
}
