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
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_expression::types::DataType;

use super::partitioned_build::PartitionedHashJoinState;
use super::partitioned_build::ProbeData;
use super::right_join_semi::SemiRightHashJoinStream;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::hash_join_table::RowPtr;
use crate::pipelines::processors::transforms::new_hash_join::common::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::common::join::Join;
use crate::pipelines::processors::transforms::new_hash_join::common::join::JoinStream;
use crate::pipelines::processors::transforms::unpartitioned::PerformanceContext;

pub struct PartitionedRightAntiJoin {
    build: PartitionedHashJoinState,
    max_block_size: usize,
    desc: Arc<HashJoinDesc>,
    function_ctx: Arc<FunctionContext>,
    context: PerformanceContext,
    finished: bool,
}

impl PartitionedRightAntiJoin {
    pub fn create(
        method: HashMethodKind,
        desc: Arc<HashJoinDesc>,
        function_ctx: FunctionContext,
        max_block_size: usize,
    ) -> Self {
        let context =
            PerformanceContext::create(max_block_size, desc.clone(), function_ctx.clone());

        let function_ctx = Arc::new(function_ctx);

        PartitionedRightAntiJoin {
            function_ctx: function_ctx.clone(),
            build: PartitionedHashJoinState::create(method, desc.clone(), function_ctx),
            max_block_size,
            desc,
            context,
            finished: false,
        }
    }
}

impl Join for PartitionedRightAntiJoin {
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
        let probe_keys_stream = self.build.probe::<true>(probe_data)?;

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

        Ok(Some(Box::new(PartitionedRightAntiFinalStream {
            columns: &self.build.columns,
            column_types: &self.build.column_types,
            visited: &self.build.visited,
            chunk_idx: 0,
            row_idx: 0,
            max_block_size: self.max_block_size,
        })))
    }
}

struct PartitionedRightAntiFinalStream<'a> {
    columns: &'a Vec<ColumnVec>,
    column_types: &'a Vec<DataType>,
    visited: &'a Vec<Vec<u8>>,
    chunk_idx: usize,
    row_idx: usize,
    max_block_size: usize,
}

impl<'a> JoinStream for PartitionedRightAntiFinalStream<'a> {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        let mut row_ptrs = Vec::with_capacity(self.max_block_size);
        while self.chunk_idx < self.visited.len() && row_ptrs.len() < self.max_block_size {
            let chunk = &self.visited[self.chunk_idx];
            while self.row_idx < chunk.len() && row_ptrs.len() < self.max_block_size {
                if chunk[self.row_idx] == 0 {
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
