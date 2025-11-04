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
use databend_common_catalog::table_context::TableContext;
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::with_join_hash_method;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::FilterExecutor;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;

use crate::pipelines::processors::transforms::build_runtime_filter_packet;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbedRows;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::ProbeData;
use crate::pipelines::processors::transforms::new_hash_join::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::Join;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;
use crate::pipelines::processors::transforms::new_hash_join::memory::basic::BasicHashJoin;
use crate::pipelines::processors::transforms::new_hash_join::memory::basic_state::BasicHashJoinState;
use crate::pipelines::processors::transforms::new_hash_join::performance::PerformanceContext;
use crate::pipelines::processors::transforms::HashJoinHashTable;
use crate::pipelines::processors::transforms::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::RuntimeFiltersDesc;
use crate::pipelines::processors::HashJoinDesc;
use crate::sessions::QueryContext;

pub struct InnerHashJoin {
    pub(crate) basic_hash_join: BasicHashJoin,

    pub(crate) desc: Arc<HashJoinDesc>,
    pub(crate) function_ctx: FunctionContext,
    pub(crate) basic_state: Arc<BasicHashJoinState>,
    pub(crate) performance_context: PerformanceContext,
}

impl InnerHashJoin {
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

        Ok(InnerHashJoin {
            desc,
            basic_hash_join,
            function_ctx,
            basic_state: state,
            performance_context: context,
        })
    }
}

impl Join for InnerHashJoin {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        self.basic_hash_join.add_block(data)
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        self.basic_hash_join.final_build()
    }

    fn build_runtime_filter(&self, desc: &RuntimeFiltersDesc) -> Result<JoinRuntimeFilterPacket> {
        build_runtime_filter_packet(
            self.basic_state.chunks.deref(),
            *self.basic_state.build_rows,
            &desc.filters_desc,
            &self.function_ctx,
            desc.inlist_threshold,
            desc.bloom_threshold,
            desc.min_max_threshold,
            desc.selectivity_threshold,
        )
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        if data.is_empty() || *self.basic_state.build_rows == 0 {
            return Ok(Box::new(EmptyJoinStream));
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

        let joined_stream =
            with_join_hash_method!(|T| match self.basic_state.hash_table.deref() {
                HashJoinHashTable::T(table) => {
                    let probe_hash_statistics = &mut self.performance_context.probe_hash_statistics;
                    probe_hash_statistics.clear(probe_block.num_rows());

                    let probe_data = ProbeData::new(keys, valids, probe_hash_statistics);
                    let probe_keys_stream = table.probe_matched(probe_data)?;

                    Ok(InnerHashJoinStream::create(
                        probe_block,
                        self.basic_state.clone(),
                        probe_keys_stream,
                        self.desc.clone(),
                        &mut self.performance_context.probe_result,
                    ))
                }
                HashJoinHashTable::Null => Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the hash table is uninitialized.",
                )),
            })?;

        match &mut self.performance_context.filter_executor {
            None => Ok(joined_stream),
            Some(filter_executor) => Ok(InnerHashJoinFilterStream::create(
                joined_stream,
                filter_executor,
            )),
        }
    }
}

struct InnerHashJoinStream<'a> {
    desc: Arc<HashJoinDesc>,
    probe_data_block: DataBlock,
    join_state: Arc<BasicHashJoinState>,
    probe_keys_stream: Box<dyn ProbeStream + 'a>,
    probed_rows: &'a mut ProbedRows,
}

unsafe impl<'a> Send for InnerHashJoinStream<'a> {}
unsafe impl<'a> Sync for InnerHashJoinStream<'a> {}

impl<'a> InnerHashJoinStream<'a> {
    pub fn create(
        probe_data_block: DataBlock,
        join_state: Arc<BasicHashJoinState>,
        probe_keys_stream: Box<dyn ProbeStream + 'a>,
        desc: Arc<HashJoinDesc>,
        probed_rows: &'a mut ProbedRows,
    ) -> Box<dyn JoinStream + 'a> {
        Box::new(InnerHashJoinStream {
            desc,
            join_state,
            probed_rows,
            probe_data_block,
            probe_keys_stream,
        })
    }
}

impl<'a> JoinStream for InnerHashJoinStream<'a> {
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

            return Ok(Some(result_block));
        }
    }
}

struct InnerHashJoinFilterStream<'a> {
    inner: Box<dyn JoinStream + 'a>,
    filter_executor: &'a mut FilterExecutor,
}

impl<'a> InnerHashJoinFilterStream<'a> {
    pub fn create(
        inner: Box<dyn JoinStream + 'a>,
        filter_executor: &'a mut FilterExecutor,
    ) -> Box<dyn JoinStream + 'a> {
        Box::new(InnerHashJoinFilterStream {
            inner,
            filter_executor,
        })
    }
}

impl<'a> JoinStream for InnerHashJoinFilterStream<'a> {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        loop {
            let Some(data_block) = self.inner.next()? else {
                return Ok(None);
            };

            if data_block.is_empty() {
                continue;
            }

            let data_block = self.filter_executor.filter(data_block)?;

            if data_block.is_empty() {
                continue;
            }

            return Ok(Some(data_block));
        }
    }
}
