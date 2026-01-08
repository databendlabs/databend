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
use databend_common_catalog::table_context::TableContext;
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::FilterExecutor;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::with_join_hash_method;

use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::transforms::HashJoinHashTable;
use crate::pipelines::processors::transforms::Join;
use crate::pipelines::processors::transforms::memory::basic::BasicHashJoin;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::ProbeData;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbedRows;
use crate::pipelines::processors::transforms::new_hash_join::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::OneBlockJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::performance::PerformanceContext;
use crate::sessions::QueryContext;

pub struct AntiLeftHashJoin {
    pub(crate) basic_hash_join: BasicHashJoin,

    pub(crate) desc: Arc<HashJoinDesc>,
    pub(crate) function_ctx: FunctionContext,
    pub(crate) basic_state: Arc<BasicHashJoinState>,
    pub(crate) performance_context: PerformanceContext,
}

impl AntiLeftHashJoin {
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
            ctx,
            function_ctx.clone(),
            method,
            desc.clone(),
            state.clone(),
        )?;

        Ok(AntiLeftHashJoin {
            desc,
            basic_hash_join,
            function_ctx,
            basic_state: state,
            performance_context: context,
        })
    }
}

impl Join for AntiLeftHashJoin {
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
            let result_block = data.project(&self.desc.probe_projections);
            return Ok(Box::new(OneBlockJoinStream(Some(result_block))));
        }

        self.basic_hash_join.finalize_chunks();

        let probe_keys = self.desc.probe_key(&data, &self.function_ctx)?;

        let mut keys = DataBlock::new(probe_keys, data.num_rows());
        let valids = match self.desc.from_correlated_subquery {
            true => None,
            false => self.desc.build_valids_by_keys(&keys)?,
        };

        self.desc.remove_keys_nullable(&mut keys);
        let probe_block = data.project(&self.desc.probe_projections);

        let join_stream = with_join_hash_method!(|T| match self.basic_state.hash_table.deref() {
            HashJoinHashTable::T(table) => {
                let probe_hash_statistics = &mut self.performance_context.probe_hash_statistics;
                probe_hash_statistics.clear(probe_block.num_rows());

                let probe_data = ProbeData::new(keys, valids, probe_hash_statistics);
                table.probe(probe_data)
            }
            HashJoinHashTable::Null => Err(ErrorCode::AbortedQuery(
                "Aborted query, because the hash table is uninitialized.",
            )),
        })?;

        match &mut self.performance_context.filter_executor {
            None => Ok(LeftAntiHashJoinStream::create(
                probe_block,
                join_stream,
                &mut self.performance_context.probe_result,
            )),
            Some(filter_executor) => Ok(LeftAntiFilterHashJoinStream::create(
                probe_block,
                self.basic_state.clone(),
                join_stream,
                self.desc.clone(),
                &mut self.performance_context.probe_result,
                filter_executor,
            )),
        }
    }
}

struct LeftAntiHashJoinStream<'a> {
    probe_data_block: Option<DataBlock>,
    probe_keys_stream: Box<dyn ProbeStream + 'a>,
    probed_rows: &'a mut ProbedRows,
}

unsafe impl<'a> Send for LeftAntiHashJoinStream<'a> {}
unsafe impl<'a> Sync for LeftAntiHashJoinStream<'a> {}

impl<'a> LeftAntiHashJoinStream<'a> {
    pub fn create(
        probe_data_block: DataBlock,
        probe_keys_stream: Box<dyn ProbeStream + 'a>,
        probed_rows: &'a mut ProbedRows,
    ) -> Box<dyn JoinStream + 'a> {
        Box::new(LeftAntiHashJoinStream {
            probed_rows,
            probe_data_block: Some(probe_data_block),
            probe_keys_stream,
        })
    }
}

impl<'a> JoinStream for LeftAntiHashJoinStream<'a> {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        let Some(probe_data_block) = self.probe_data_block.take() else {
            return Ok(None);
        };

        let num_rows = probe_data_block.num_rows();
        let mut selected = vec![false; num_rows];

        loop {
            self.probed_rows.clear();
            let max_rows = self.probed_rows.matched_probe.capacity();
            self.probe_keys_stream.advance(self.probed_rows, max_rows)?;

            if self.probed_rows.is_empty() {
                let bitmap = Bitmap::from_trusted_len_iter(selected.into_iter());
                return Ok(Some(probe_data_block.filter_with_bitmap(&bitmap)?));
            }

            for idx in &self.probed_rows.unmatched {
                let idx = *idx as usize;
                assume(idx < selected.len());
                selected[idx] = true;
            }
        }
    }
}

struct LeftAntiFilterHashJoinStream<'a> {
    desc: Arc<HashJoinDesc>,
    probe_data_block: Option<DataBlock>,
    join_state: Arc<BasicHashJoinState>,
    probe_keys_stream: Box<dyn ProbeStream + 'a>,
    probed_rows: &'a mut ProbedRows,
    filter_executor: &'a mut FilterExecutor,
}

unsafe impl<'a> Send for LeftAntiFilterHashJoinStream<'a> {}
unsafe impl<'a> Sync for LeftAntiFilterHashJoinStream<'a> {}

impl<'a> LeftAntiFilterHashJoinStream<'a> {
    pub fn create(
        probe_data_block: DataBlock,
        join_state: Arc<BasicHashJoinState>,
        probe_keys_stream: Box<dyn ProbeStream + 'a>,
        desc: Arc<HashJoinDesc>,
        probed_rows: &'a mut ProbedRows,
        filter_executor: &'a mut FilterExecutor,
    ) -> Box<dyn JoinStream + 'a> {
        Box::new(LeftAntiFilterHashJoinStream {
            desc,
            join_state,
            probed_rows,
            filter_executor,
            probe_keys_stream,
            probe_data_block: Some(probe_data_block),
        })
    }
}

impl<'a> JoinStream for LeftAntiFilterHashJoinStream<'a> {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        let Some(probe_data_block) = self.probe_data_block.take() else {
            return Ok(None);
        };

        let num_rows = probe_data_block.num_rows();
        let mut selected = vec![true; num_rows];

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
                    &self.probed_rows.matched_probe,
                )?),
            };

            let build_block = match self.join_state.columns.is_empty() {
                true => None,
                false => {
                    let row_ptrs = self.probed_rows.matched_build.as_slice();
                    Some(DataBlock::take_column_vec(
                        self.join_state.columns.as_slice(),
                        self.join_state.column_types.as_slice(),
                        row_ptrs,
                        row_ptrs.len(),
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

            if !self.desc.probe_to_build.is_empty() {
                for (index, (is_probe_nullable, is_build_nullable)) in
                    self.desc.probe_to_build.iter()
                {
                    let entry = match (is_probe_nullable, is_build_nullable) {
                        (true, true) | (false, false) => result_block.get_by_offset(*index).clone(),
                        (true, false) => {
                            result_block.get_by_offset(*index).clone().remove_nullable()
                        }
                        (false, true) => {
                            let entry = result_block.get_by_offset(*index);
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
            }

            let selected_rows = self.filter_executor.select(&result_block)?;

            if selected_rows == result_block.num_rows() {
                for probe_idx in &self.probed_rows.matched_probe {
                    assume((*probe_idx as usize) < selected.len());
                    selected[*probe_idx as usize] = false;
                }
            } else if selected_rows != 0 {
                let selection = self.filter_executor.true_selection();
                for idx in selection[..selected_rows].iter() {
                    assume((*idx as usize) < self.probed_rows.matched_probe.len());
                    let idx = self.probed_rows.matched_probe[*idx as usize];
                    assume((idx as usize) < selected.len());
                    selected[idx as usize] = false;
                }
            }
        }

        let bitmap = Bitmap::from_trusted_len_iter(selected.into_iter());
        match bitmap.true_count() {
            0 => Ok(None),
            _ => Ok(Some(probe_data_block.filter_with_bitmap(&bitmap)?)),
        }
    }
}
