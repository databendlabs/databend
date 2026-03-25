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
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::inner_join::result_block;
use super::partitioned_build::PartitionedBuild;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::new_hash_join::common::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::common::join::Join;
use crate::pipelines::processors::transforms::new_hash_join::common::join::JoinStream;
use crate::pipelines::processors::transforms::new_hash_join::common::join::OneBlockJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::common::probe_stream::ProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::common::probe_stream::ProbedRows;
use crate::pipelines::processors::transforms::wrap_true_validity;

pub fn null_block(types: &[DataType], num_rows: usize) -> Option<DataBlock> {
    if types.is_empty() {
        return None;
    }
    let columns = types
        .iter()
        .map(|t| BlockEntry::new_const_column(t.wrap_nullable(), Scalar::Null, num_rows))
        .collect::<Vec<_>>();
    Some(DataBlock::new(columns, num_rows))
}

pub struct PartitionedLeftJoin {
    build: PartitionedBuild,
    filter_executor: Option<FilterExecutor>,
    max_block_size: usize,
}

impl PartitionedLeftJoin {
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
        PartitionedLeftJoin {
            build: PartitionedBuild::create(method, desc, function_ctx),
            filter_executor,
            max_block_size,
        }
    }
}

fn wrap_nullable_build(build_block: DataBlock, num_rows: usize) -> DataBlock {
    let true_validity = Bitmap::new_constant(true, num_rows);
    let entries = build_block
        .columns()
        .iter()
        .map(|c| wrap_true_validity(c, num_rows, &true_validity));
    DataBlock::from_iter(entries, num_rows)
}

struct PartitionedLeftJoinStream<'a> {
    desc: Arc<HashJoinDesc>,
    probe_data_block: DataBlock,
    build: &'a PartitionedBuild,
    probe_stream: Box<dyn ProbeStream + Send + Sync + 'a>,
    probed_rows: ProbedRows,
    filter_executor: Option<&'a mut FilterExecutor>,
    max_block_size: usize,
    // Accumulated unmatched probe indices (no hash match)
    unmatched_indices: Vec<u64>,
    // Per-probe-row state for filter case: 0=unseen, 1=matched-no-pass, 2=passed
    row_state: Vec<u8>,
    has_filter: bool,
    probe_done: bool,
    unmatched_offset: usize,
}

impl<'a> PartitionedLeftJoinStream<'a> {
    fn output_unmatched(&mut self) -> Result<Option<DataBlock>> {
        // Collect all unmatched indices: from ProbeStream + filter-failed matched rows
        if self.unmatched_offset == 0 && self.has_filter {
            for i in 0..self.row_state.len() {
                if self.row_state[i] == 1 {
                    self.unmatched_indices.push(i as u64);
                }
            }
        }

        if self.unmatched_offset >= self.unmatched_indices.len() {
            return Ok(None);
        }

        let end = (self.unmatched_offset + self.max_block_size).min(self.unmatched_indices.len());
        let batch = &self.unmatched_indices[self.unmatched_offset..end];
        self.unmatched_offset = end;

        let probe_block = match self.probe_data_block.num_columns() {
            0 => None,
            _ => Some(DataBlock::take(&self.probe_data_block, batch)?),
        };
        let build_block = null_block(&self.build.column_types, batch.len());
        Ok(Some(result_block(
            &self.desc,
            probe_block,
            build_block,
            batch.len(),
        )))
    }
}

impl<'a> JoinStream for PartitionedLeftJoinStream<'a> {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        if self.probe_done {
            return self.output_unmatched();
        }

        loop {
            self.probed_rows.clear();
            self.probe_stream
                .advance(&mut self.probed_rows, self.max_block_size)?;

            if self.probed_rows.is_empty() {
                self.probe_done = true;
                return self.output_unmatched();
            }

            // Save unmatched indices
            self.unmatched_indices
                .extend_from_slice(&self.probed_rows.unmatched);

            if self.probed_rows.matched_probe.is_empty() {
                continue;
            }

            let num_matched = self.probed_rows.matched_probe.len();
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
            let build_block = build_block.map(|b| wrap_nullable_build(b, num_matched));

            let mut block = result_block(&self.desc, probe_block, build_block, num_matched);

            if let Some(filter) = self.filter_executor.as_mut() {
                // Track matched rows
                for &idx in &self.probed_rows.matched_probe {
                    let i = idx as usize;
                    if self.row_state[i] == 0 {
                        self.row_state[i] = 1;
                    }
                }

                let count = filter.select(&block)?;
                if count == 0 {
                    continue;
                }

                let true_sel = filter.true_selection();
                for &sel_idx in true_sel.iter().take(count) {
                    let probe_idx = self.probed_rows.matched_probe[sel_idx as usize] as usize;
                    self.row_state[probe_idx] = 2;
                }

                let origin_rows = block.num_rows();
                block = filter.take(block, origin_rows, count)?;
            }

            if !block.is_empty() {
                return Ok(Some(block));
            }
        }
    }
}

impl Join for PartitionedLeftJoin {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        self.build.add_block(data)
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        self.build.final_build()
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        if data.is_empty() {
            return Ok(Box::new(EmptyJoinStream));
        }

        let desc = &self.build.desc;

        if self.build.num_rows == 0 {
            let num_rows = data.num_rows();
            let types: Vec<_> = desc
                .build_schema
                .fields
                .iter()
                .map(|x| x.data_type().clone())
                .collect();
            let build_block =
                null_block(&types, num_rows).map(|b| b.project(&desc.build_projection));
            let probe_block = Some(data.project(&desc.probe_projection));
            let block = result_block(desc, probe_block, build_block, num_rows);
            return Ok(Box::new(OneBlockJoinStream(Some(block))));
        }

        let num_probe_rows = data.num_rows();
        let has_filter = self.filter_executor.is_some();
        let probe_stream = self.build.create_probe(&data)?;
        let probe_data_block = data.project(&desc.probe_projection);

        Ok(Box::new(PartitionedLeftJoinStream {
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
            unmatched_indices: Vec::new(),
            row_state: if has_filter {
                vec![0u8; num_probe_rows]
            } else {
                Vec::new()
            },
            has_filter,
            probe_done: false,
            unmatched_offset: 0,
        }))
    }
}
