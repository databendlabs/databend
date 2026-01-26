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
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::FilterExecutor;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::with_join_hash_method;

use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::transforms::HashJoinHashTable;
use crate::pipelines::processors::transforms::Join;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::ProbeData;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbedRows;
use crate::pipelines::processors::transforms::new_hash_join::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::OneBlockJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::memory::basic::BasicHashJoin;
use crate::pipelines::processors::transforms::new_hash_join::performance::PerformanceContext;
use crate::pipelines::processors::transforms::wrap_true_validity;
use crate::sessions::QueryContext;

pub struct OuterLeftHashJoin {
    pub(crate) basic_hash_join: BasicHashJoin,

    pub(crate) desc: Arc<HashJoinDesc>,
    pub(crate) function_ctx: FunctionContext,
    pub(crate) basic_state: Arc<BasicHashJoinState>,
    pub(crate) performance_context: PerformanceContext,
}

impl OuterLeftHashJoin {
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

        Ok(OuterLeftHashJoin {
            desc,
            basic_hash_join,
            function_ctx,
            basic_state: state,
            performance_context: context,
        })
    }
}

impl Join for OuterLeftHashJoin {
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

            let build_block = null_block(&types, data.num_rows());
            let probe_block = Some(data.project(&self.desc.probe_projection));
            let result_block = final_result_block(&self.desc, probe_block, build_block, num_rows);
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
        let probe_block = data.project(&self.desc.probe_projection);

        let probe_stream = with_join_hash_method!(|T| match self.basic_state.hash_table.deref() {
            HashJoinHashTable::T(table) => {
                let probe_hash_statistics = &mut self.performance_context.probe_hash_statistics;
                probe_hash_statistics.clear(probe_block.num_rows());

                let probe_data = ProbeData::new(keys, valids, probe_hash_statistics);
                table.probe(probe_data)
            }
            HashJoinHashTable::NestedLoop(_) => unreachable!(),
            HashJoinHashTable::Null => Err(ErrorCode::AbortedQuery(
                "Aborted query, because the hash table is uninitialized.",
            )),
        })?;

        match self.performance_context.filter_executor.as_mut() {
            None => Ok(OuterLeftHashJoinStream::<false>::create(
                probe_block,
                self.basic_state.clone(),
                probe_stream,
                self.desc.clone(),
                &mut self.performance_context.probe_result,
                None,
            )),
            Some(filter_executor) => Ok(OuterLeftHashJoinStream::<true>::create(
                probe_block,
                self.basic_state.clone(),
                probe_stream,
                self.desc.clone(),
                &mut self.performance_context.probe_result,
                Some(filter_executor),
            )),
        }
    }
}

struct OuterLeftHashJoinStream<'a, const CONJUNCT: bool> {
    desc: Arc<HashJoinDesc>,
    probe_data_block: DataBlock,
    join_state: Arc<BasicHashJoinState>,
    probe_keys_stream: Box<dyn ProbeStream + 'a>,
    probed_rows: &'a mut ProbedRows,
    conjunct_unmatched: Vec<u8>,
    unmatched_rows: Vec<u64>,
    filter_executor: Option<&'a mut FilterExecutor>,
}

unsafe impl<'a, const CONJUNCT: bool> Send for OuterLeftHashJoinStream<'a, CONJUNCT> {}
unsafe impl<'a, const CONJUNCT: bool> Sync for OuterLeftHashJoinStream<'a, CONJUNCT> {}

impl<'a, const CONJUNCT: bool> JoinStream for OuterLeftHashJoinStream<'a, CONJUNCT> {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        loop {
            self.probed_rows.clear();
            let max_rows = self.probed_rows.matched_probe.capacity();
            self.probe_keys_stream.advance(self.probed_rows, max_rows)?;

            if !CONJUNCT && !self.probed_rows.unmatched.is_empty() {
                self.unmatched_rows
                    .extend_from_slice(&self.probed_rows.unmatched);
            }

            if self.probed_rows.is_empty() {
                if self.conjunct_unmatched.is_empty() && self.unmatched_rows.is_empty() {
                    return Ok(None);
                }

                let unmatched_row_id = match CONJUNCT {
                    true => std::mem::take(&mut self.conjunct_unmatched)
                        .into_iter()
                        .enumerate()
                        .filter(|(_, matched)| *matched == 0)
                        .map(|(row_id, _)| row_id as u64)
                        .collect::<Vec<_>>(),
                    false => std::mem::take(&mut self.unmatched_rows),
                };

                let probe_block = match self.probe_data_block.num_columns() {
                    0 => None,
                    _ => Some(DataBlock::take(
                        &self.probe_data_block,
                        unmatched_row_id.as_slice(),
                    )?),
                };

                let types = &self.join_state.column_types;
                let build_block = null_block(types, unmatched_row_id.len());

                return Ok(Some(final_result_block(
                    &self.desc,
                    probe_block,
                    build_block,
                    unmatched_row_id.len(),
                )));
            }

            if self.probed_rows.matched_probe.is_empty() {
                continue;
            }

            let probe_block = match self.probe_data_block.num_columns() {
                0 => None,
                _ => Some(DataBlock::take(
                    &self.probe_data_block,
                    self.probed_rows.matched_probe.as_slice(),
                )?),
            };

            let build_block = match self.join_state.columns.is_empty() {
                true => None,
                false => {
                    let row_ptrs = self.probed_rows.matched_build.as_slice();
                    let build_block = DataBlock::take_column_vec(
                        self.join_state.columns.as_slice(),
                        self.join_state.column_types.as_slice(),
                        row_ptrs,
                    );

                    let true_validity = Bitmap::new_constant(true, row_ptrs.len());
                    let entries = build_block
                        .columns()
                        .iter()
                        .map(|c| wrap_true_validity(c, row_ptrs.len(), &true_validity));
                    Some(DataBlock::from_iter(entries, row_ptrs.len()))
                }
            };

            let mut result_block = final_result_block(
                &self.desc,
                probe_block,
                build_block,
                self.probed_rows.matched_build.len(),
            );

            if CONJUNCT && let Some(filter_executor) = self.filter_executor.as_mut() {
                let result_count = filter_executor.select(&result_block)?;

                if result_count == 0 {
                    continue;
                }

                let true_sel = filter_executor.true_selection();

                for idx in true_sel.iter().take(result_count) {
                    let row_id = self.probed_rows.matched_probe[*idx as usize] as usize;
                    self.conjunct_unmatched[row_id] = 1;
                }

                let origin_rows = result_block.num_rows();
                result_block = filter_executor.take(result_block, origin_rows, result_count)?;
            }

            return Ok(Some(result_block));
        }
    }
}

impl<'a, const CONJUNCT: bool> OuterLeftHashJoinStream<'a, CONJUNCT> {
    pub fn create(
        probe_data_block: DataBlock,
        join_state: Arc<BasicHashJoinState>,
        probe_keys_stream: Box<dyn ProbeStream + 'a>,
        desc: Arc<HashJoinDesc>,
        probed_rows: &'a mut ProbedRows,
        filter_executor: Option<&'a mut FilterExecutor>,
    ) -> Box<dyn JoinStream + 'a> {
        let num_rows = probe_data_block.num_rows();
        let pending_unmatched = match CONJUNCT {
            true => vec![0; num_rows],
            false => Vec::new(),
        };

        let unmatched_rows = match CONJUNCT {
            true => Vec::new(),
            false => Vec::with_capacity(num_rows),
        };

        probed_rows.unmatched.reserve(num_rows);
        Box::new(OuterLeftHashJoinStream::<'a, CONJUNCT> {
            desc,
            join_state,
            probed_rows,
            probe_data_block,
            probe_keys_stream,
            filter_executor,
            unmatched_rows,
            conjunct_unmatched: pending_unmatched,
        })
    }
}

pub fn final_result_block(
    desc: &HashJoinDesc,
    probe_block: Option<DataBlock>,
    build_block: Option<DataBlock>,
    num_rows: usize,
) -> DataBlock {
    let mut result_block = match (probe_block, build_block) {
        (Some(mut probe_block), Some(build_block)) => {
            probe_block.merge_block(build_block);
            probe_block
        }
        (Some(probe_block), None) => probe_block,
        (None, Some(build_block)) => build_block,
        (None, None) => DataBlock::new(vec![], num_rows),
    };

    if !desc.probe_to_build.is_empty() {
        for (index, (is_probe_nullable, is_build_nullable)) in desc.probe_to_build.iter() {
            let entry = match (is_probe_nullable, is_build_nullable) {
                (true, true) | (false, false) => result_block.get_by_offset(*index).clone(),
                (true, false) => result_block.get_by_offset(*index).clone().remove_nullable(),
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
    result_block
}

pub fn null_block(types: &[DataType], num_rows: usize) -> Option<DataBlock> {
    match types.is_empty() {
        true => None,
        false => {
            let columns = types
                .iter()
                .map(|column_type| {
                    BlockEntry::new_const_column(
                        column_type.wrap_nullable(),
                        Scalar::Null,
                        num_rows,
                    )
                })
                .collect::<Vec<_>>();

            Some(DataBlock::new(columns, num_rows))
        }
    }
}
