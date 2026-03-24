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

use super::inner_join::result_block;
use super::partitioned_build::PartitionedBuild;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::new_hash_join::grace::grace_memory::GraceMemoryJoin;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbedRows;
use crate::pipelines::processors::transforms::new_hash_join::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::Join;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;

pub struct PartitionedLeftSemiJoin {
    build: PartitionedBuild,
    filter_executor: Option<FilterExecutor>,
    max_block_size: usize,
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
            max_block_size,
        }
    }
}

struct PartitionedLeftSemiJoinStream<'a> {
    probe_data_block: DataBlock,
    build: &'a PartitionedBuild,
    desc: Arc<HashJoinDesc>,
    probe_stream: Box<dyn ProbeStream + Send + Sync + 'a>,
    probed_rows: ProbedRows,
    filter_executor: Option<&'a mut FilterExecutor>,
    max_block_size: usize,
    selected: Vec<bool>,
    probe_done: bool,
}

impl<'a> JoinStream for PartitionedLeftSemiJoinStream<'a> {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        if self.probe_done {
            return Ok(None);
        }

        loop {
            self.probed_rows.clear();
            self.probe_stream
                .advance(&mut self.probed_rows, self.max_block_size)?;

            if self.probed_rows.is_empty() {
                self.probe_done = true;
                let bitmap = Bitmap::from_trusted_len_iter(self.selected.iter().copied());
                return match bitmap.true_count() {
                    0 => Ok(None),
                    _ => Ok(Some(
                        self.probe_data_block.clone().filter_with_bitmap(&bitmap)?,
                    )),
                };
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
                let build_block = self
                    .build
                    .gather_build_block(&self.probed_rows.matched_build);
                let block = result_block(&self.desc, probe_block, build_block, num_matched);

                let count = filter.select(&block)?;
                if count > 0 {
                    let true_sel = filter.true_selection();
                    for &sel_idx in true_sel.iter().take(count) {
                        let probe_idx = self.probed_rows.matched_probe[sel_idx as usize] as usize;
                        self.selected[probe_idx] = true;
                    }
                }
            } else {
                for &idx in &self.probed_rows.matched_probe {
                    self.selected[idx as usize] = true;
                }
            }
        }
    }
}

impl Join for PartitionedLeftSemiJoin {
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

        let num_probe_rows = data.num_rows();
        let probe_stream = self.build.create_probe_matched(&data)?;
        let probe_data_block = data.project(&self.build.desc.probe_projection);

        Ok(Box::new(PartitionedLeftSemiJoinStream {
            probe_data_block,
            build: &self.build,
            desc: self.build.desc.clone(),
            probe_stream,
            probed_rows: ProbedRows::new(
                Vec::with_capacity(self.max_block_size),
                Vec::with_capacity(self.max_block_size),
                Vec::with_capacity(self.max_block_size),
            ),
            filter_executor: self.filter_executor.as_mut(),
            max_block_size: self.max_block_size,
            selected: vec![false; num_probe_rows],
            probe_done: false,
        }))
    }
}

impl GraceMemoryJoin for PartitionedLeftSemiJoin {
    fn reset_memory(&mut self) {
        self.build.reset();
    }
}
