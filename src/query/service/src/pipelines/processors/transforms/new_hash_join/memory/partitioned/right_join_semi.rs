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
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::compact_probe_stream::create_compact_probe_matched;
use super::inner_join::result_block;
use super::partitioned_build::PartitionedBuild;
use super::partitioned_build::flat_to_row_ptr;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::new_hash_join::grace::grace_memory::GraceMemoryJoin;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbedRows;
use crate::pipelines::processors::transforms::new_hash_join::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::Join;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;

pub struct PartitionedRightSemiJoin {
    build: PartitionedBuild,
    filter_executor: Option<FilterExecutor>,
    max_block_size: usize,
    finished: bool,
}

impl PartitionedRightSemiJoin {
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
        PartitionedRightSemiJoin {
            build: PartitionedBuild::create(method, desc, function_ctx),
            filter_executor,
            max_block_size,
            finished: false,
        }
    }
}

/// Probe stream that marks visited build rows, outputs nothing.
struct PartitionedRightSemiProbeStream<'a> {
    desc: Arc<HashJoinDesc>,
    probe_data_block: DataBlock,
    columns: &'a Vec<ColumnVec>,
    column_types: &'a Vec<DataType>,
    visited: &'a mut Vec<u8>,
    probe_stream: Box<dyn ProbeStream + Send + Sync + 'a>,
    probed_rows: ProbedRows,
    filter_executor: Option<&'a mut FilterExecutor>,
    max_block_size: usize,
}

impl<'a> JoinStream for PartitionedRightSemiProbeStream<'a> {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        loop {
            self.probed_rows.clear();
            self.probe_stream
                .advance(&mut self.probed_rows, self.max_block_size)?;

            if self.probed_rows.is_empty() {
                return Ok(None);
            }

            if self.probed_rows.matched_probe.is_empty() {
                continue;
            }

            if let Some(filter) = self.filter_executor.as_mut() {
                let num_matched = self.probed_rows.matched_probe.len();
                let probe_block = match self.probe_data_block.num_columns() {
                    0 => None,
                    _ => Some(DataBlock::take(
                        &self.probe_data_block,
                        self.probed_rows.matched_probe.as_slice(),
                    )?),
                };
                let build_block = if self.columns.is_empty() {
                    None
                } else {
                    Some(DataBlock::take_column_vec(
                        self.columns,
                        self.column_types,
                        &self.probed_rows.matched_build,
                    ))
                };
                let block = result_block(&self.desc, probe_block, build_block, num_matched);
                let count = filter.select(&block)?;
                if count > 0 {
                    let true_sel = filter.true_selection();
                    for &sel_idx in true_sel.iter().take(count) {
                        let row_ptr = &self.probed_rows.matched_build[sel_idx as usize];
                        let flat_idx = (row_ptr.chunk_index as usize)
                            * super::partitioned_build::CHUNK_SIZE
                            + row_ptr.row_index as usize
                            + 1;
                        self.visited[flat_idx] = 1;
                    }
                }
            } else {
                for row_ptr in &self.probed_rows.matched_build {
                    let flat_idx = (row_ptr.chunk_index as usize)
                        * super::partitioned_build::CHUNK_SIZE
                        + row_ptr.row_index as usize
                        + 1;
                    self.visited[flat_idx] = 1;
                }
            }
            // Right semi outputs nothing during probe
        }
    }
}

/// Final stream: output visited build rows.
struct PartitionedRightSemiFinalStream<'a> {
    columns: &'a Vec<ColumnVec>,
    column_types: &'a Vec<DataType>,
    visited: &'a [u8],
    num_rows: usize,
    scan_idx: usize,
    max_block_size: usize,
}

impl<'a> JoinStream for PartitionedRightSemiFinalStream<'a> {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        let mut row_ptrs = Vec::with_capacity(self.max_block_size);
        while self.scan_idx <= self.num_rows && row_ptrs.len() < self.max_block_size {
            if self.visited[self.scan_idx] != 0 {
                row_ptrs.push(flat_to_row_ptr(self.scan_idx));
            }
            self.scan_idx += 1;
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

impl Join for PartitionedRightSemiJoin {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        self.build.add_block(data)
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        let progress = self.build.final_build()?;
        if progress.is_none() {
            self.build.init_visited();
        }
        Ok(progress)
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        if data.is_empty() || self.build.num_rows == 0 {
            return Ok(Box::new(EmptyJoinStream));
        }

        let probe_stream = create_compact_probe_matched(
            &self.build.hash_table,
            &self.build.build_keys_states,
            &self.build.method,
            &self.build.desc,
            &self.build.function_ctx,
            &data,
        )?;
        let probe_data_block = data.project(&self.build.desc.probe_projection);

        Ok(Box::new(PartitionedRightSemiProbeStream {
            desc: self.build.desc.clone(),
            probe_data_block,
            columns: &self.build.columns,
            column_types: &self.build.column_types,
            visited: &mut self.build.visited,
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

    fn final_probe(&mut self) -> Result<Option<Box<dyn JoinStream + '_>>> {
        if self.finished || self.build.num_rows == 0 {
            return Ok(None);
        }
        self.finished = true;

        Ok(Some(Box::new(PartitionedRightSemiFinalStream {
            columns: &self.build.columns,
            column_types: &self.build.column_types,
            visited: &self.build.visited,
            num_rows: self.build.num_rows,
            scan_idx: 1,
            max_block_size: self.max_block_size,
        })))
    }
}

impl GraceMemoryJoin for PartitionedRightSemiJoin {
    fn reset_memory(&mut self) {
        self.finished = false;
        self.build.reset();
    }
}
