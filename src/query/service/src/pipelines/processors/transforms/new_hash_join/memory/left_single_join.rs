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

use std::ops::Deref;
use std::sync::Arc;

use databend_common_base::base::ProgressValues;
use databend_common_base::hints::assume;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FilterExecutor;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_expression::with_join_hash_method;

use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::transforms::HashJoinHashTable;
use crate::pipelines::processors::transforms::Join;
use crate::pipelines::processors::transforms::hash_join_table::RowPtr;
use crate::pipelines::processors::transforms::memory::basic::BasicHashJoin;
use crate::pipelines::processors::transforms::memory::left_join::final_result_block;
use crate::pipelines::processors::transforms::memory::left_join::null_block;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::ProbeData;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbedRows;
use crate::pipelines::processors::transforms::new_hash_join::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::OneBlockJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::performance::PerformanceContext;
use crate::pipelines::processors::transforms::wrap_nullable_block;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;

pub struct LeftSingleHashJoin {
    pub(crate) basic_hash_join: BasicHashJoin,
    pub(crate) desc: Arc<HashJoinDesc>,
    pub(crate) function_ctx: FunctionContext,
    pub(crate) basic_state: Arc<BasicHashJoinState>,
    pub(crate) performance_context: PerformanceContext,
}

impl LeftSingleHashJoin {
    pub fn create(
        ctx: &QueryContext,
        function_ctx: FunctionContext,
        method: HashMethodKind,
        desc: Arc<HashJoinDesc>,
        state: Arc<BasicHashJoinState>,
    ) -> Result<Self> {
        let settings = ctx.get_settings();
        let block_size = settings.get_max_block_size()? as usize;
        let context = PerformanceContext::create(block_size, desc.clone(), function_ctx.clone());
        let basic_hash_join = BasicHashJoin::create(
            &settings,
            function_ctx.clone(),
            method,
            desc.clone(),
            state.clone(),
            0,
        )?;

        Ok(LeftSingleHashJoin {
            desc,
            basic_hash_join,
            function_ctx,
            basic_state: state,
            performance_context: context,
        })
    }
}

impl Join for LeftSingleHashJoin {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        self.basic_hash_join.add_block(data)
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        self.basic_hash_join.final_build::<false>()
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        if data.is_empty() {
            return Ok(Box::new(EmptyJoinStream));
        }

        if *self.basic_state.build_rows == 0 {
            let num_rows = data.num_rows();
            let types = self
                .desc
                .build_schema
                .fields
                .iter()
                .map(|x| x.data_type().clone())
                .collect::<Vec<_>>();
            let build_block = null_block(&types, num_rows)
                .map(|block| block.project(&self.desc.build_projection));
            let probe_block = Some(data.project(&self.desc.probe_projection));
            return Ok(Box::new(OneBlockJoinStream(Some(final_result_block(
                &self.desc,
                probe_block,
                build_block,
                num_rows,
            )))));
        }

        self.basic_hash_join.finalize_chunks();
        let probe_keys = self.desc.probe_key(&data, &self.function_ctx)?;
        let mut probe_keys = DataBlock::new(probe_keys, data.num_rows());
        let valids = match self.desc.from_correlated_subquery {
            true => None,
            false => self.desc.build_valids_by_keys(&probe_keys)?,
        };
        self.desc.remove_keys_nullable(&mut probe_keys);
        let probe_block = data.project(&self.desc.probe_projection);

        let probe_stream = with_join_hash_method!(|T| match self.basic_state.hash_table.deref() {
            HashJoinHashTable::T(table) => {
                let probe_hash_statistics = &mut self.performance_context.probe_hash_statistics;
                probe_hash_statistics.clear(probe_block.num_rows());
                let probe_data = ProbeData::new(probe_keys, valids, probe_hash_statistics);
                table.probe(probe_data)
            }
            HashJoinHashTable::NestedLoop(_) => unreachable!(),
            HashJoinHashTable::Null => Err(ErrorCode::AbortedQuery(
                "Aborted query, because the hash table is uninitialized.",
            )),
        })?;

        Ok(LeftSingleHashJoinStream::create(
            probe_block,
            self.basic_state.clone(),
            probe_stream,
            self.desc.clone(),
            &mut self.performance_context.probe_result,
            self.performance_context.filter_executor.as_mut(),
        ))
    }
}

struct LeftSingleHashJoinStream<'a> {
    desc: Arc<HashJoinDesc>,
    probe_data_block: Option<DataBlock>,
    join_state: Arc<BasicHashJoinState>,
    probe_keys_stream: Box<dyn ProbeStream + 'a>,
    probed_rows: &'a mut ProbedRows,
    filter_executor: Option<&'a mut FilterExecutor>,
}

unsafe impl<'a> Send for LeftSingleHashJoinStream<'a> {}
unsafe impl<'a> Sync for LeftSingleHashJoinStream<'a> {}

impl<'a> LeftSingleHashJoinStream<'a> {
    pub fn create(
        probe_data_block: DataBlock,
        join_state: Arc<BasicHashJoinState>,
        probe_keys_stream: Box<dyn ProbeStream + 'a>,
        desc: Arc<HashJoinDesc>,
        probed_rows: &'a mut ProbedRows,
        filter_executor: Option<&'a mut FilterExecutor>,
    ) -> Box<dyn JoinStream + 'a> {
        Box::new(LeftSingleHashJoinStream {
            desc,
            join_state,
            probed_rows,
            filter_executor,
            probe_keys_stream,
            probe_data_block: Some(probe_data_block),
        })
    }
}

impl<'a> JoinStream for LeftSingleHashJoinStream<'a> {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        let Some(probe_data_block) = self.probe_data_block.take() else {
            return Ok(None);
        };

        let num_rows = probe_data_block.num_rows();
        let mut matched = vec![None::<RowPtr>; num_rows];
        let mut matched_counts = vec![0_usize; num_rows];

        loop {
            self.probed_rows.clear();
            let max_rows = self.probed_rows.matched_probe.capacity();
            self.probe_keys_stream.advance(self.probed_rows, max_rows)?;

            if self.probed_rows.is_empty() {
                break;
            }

            if self.probed_rows.matched_probe.is_empty() {
                continue;
            }

            let probe_block = match probe_data_block.num_columns() {
                0 => None,
                _ => Some(DataBlock::take(
                    &probe_data_block,
                    self.probed_rows.matched_probe.as_slice(),
                )?),
            };
            let build_block = match self.join_state.columns.is_empty() {
                true => None,
                false => {
                    let block = DataBlock::take_column_vec(
                        self.join_state.columns.as_slice(),
                        self.join_state.column_types.as_slice(),
                        self.probed_rows.matched_build.as_slice(),
                    );
                    Some(wrap_nullable_block(&block))
                }
            };
            let result_block = final_result_block(
                &self.desc,
                probe_block,
                build_block,
                self.probed_rows.matched_build.len(),
            );

            for probe_idx in &self.probed_rows.matched_probe {
                let probe_idx = *probe_idx as usize;
                assume(probe_idx < matched.len());
                matched_counts[probe_idx] += 1;
                if matched_counts[probe_idx] > 1 {
                    return Err(ErrorCode::Internal(
                        "Scalar subquery can't return more than one row",
                    ));
                }
            }

            match self.filter_executor.as_mut() {
                None => {
                    for (probe_idx, row_ptr) in self
                        .probed_rows
                        .matched_probe
                        .iter()
                        .zip(self.probed_rows.matched_build.iter())
                    {
                        let probe_idx = *probe_idx as usize;
                        assume(probe_idx < matched.len());
                        matched[probe_idx] = Some(*row_ptr);
                    }
                }
                Some(filter_executor) => {
                    let result_count = filter_executor.select(&result_block)?;
                    for idx in filter_executor.true_selection().iter().take(result_count) {
                        let idx = *idx as usize;
                        let probe_idx = self.probed_rows.matched_probe[idx] as usize;
                        assume(probe_idx < matched.len());
                        matched[probe_idx] = Some(self.probed_rows.matched_build[idx]);
                    }
                }
            }
        }

        let mut matched_probe = Vec::new();
        let mut matched_build = Vec::new();
        let mut unmatched = Vec::new();
        for (probe_idx, row_ptr) in matched.into_iter().enumerate() {
            match row_ptr {
                Some(row_ptr) => {
                    matched_probe.push(probe_idx as u64);
                    matched_build.push(row_ptr);
                }
                None => unmatched.push(probe_idx as u64),
            }
        }

        let mut blocks = Vec::with_capacity(2);
        if !matched_probe.is_empty() {
            let probe_block = match probe_data_block.num_columns() {
                0 => None,
                _ => Some(DataBlock::take(
                    &probe_data_block,
                    matched_probe.as_slice(),
                )?),
            };
            let build_block = match self.join_state.columns.is_empty() {
                true => None,
                false => {
                    let block = DataBlock::take_column_vec(
                        self.join_state.columns.as_slice(),
                        self.join_state.column_types.as_slice(),
                        matched_build.as_slice(),
                    );
                    Some(wrap_nullable_block(&block))
                }
            };
            blocks.push(final_result_block(
                &self.desc,
                probe_block,
                build_block,
                matched_probe.len(),
            ));
        }

        if !unmatched.is_empty() {
            let probe_block = match probe_data_block.num_columns() {
                0 => None,
                _ => Some(DataBlock::take(&probe_data_block, unmatched.as_slice())?),
            };
            let build_block = null_block(&self.join_state.column_types, unmatched.len());
            blocks.push(final_result_block(
                &self.desc,
                probe_block,
                build_block,
                unmatched.len(),
            ));
        }

        match blocks.len() {
            0 => Ok(None),
            1 => Ok(blocks.pop()),
            _ => Ok(Some(DataBlock::concat(&blocks)?)),
        }
    }
}
