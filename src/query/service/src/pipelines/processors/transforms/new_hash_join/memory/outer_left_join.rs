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
use databend_common_expression::types::DataType;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::with_join_hash_method;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_expression::Scalar;

use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbedRows;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::ProbeData;
use crate::pipelines::processors::transforms::new_hash_join::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::OneBlockJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::memory::basic::BasicHashJoin;
use crate::pipelines::processors::transforms::new_hash_join::performance::PerformanceContext;
use crate::pipelines::processors::transforms::wrap_true_validity;
use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::transforms::HashJoinHashTable;
use crate::pipelines::processors::transforms::Join;
use crate::pipelines::processors::HashJoinDesc;
use crate::sessions::QueryContext;

pub struct OuterLeftHashJoin {
    basic_hash_join: BasicHashJoin,

    desc: Arc<HashJoinDesc>,
    function_ctx: FunctionContext,
    basic_state: Arc<BasicHashJoinState>,
    performance_context: PerformanceContext,
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
            ctx,
            function_ctx.clone(),
            method,
            desc.clone(),
            state.clone(),
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
        self.basic_hash_join.final_build()
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

            let build_block = null_build_block(&types, data.num_rows());
            let probe_block = Some(data.project(&self.desc.probe_projections));
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
        let probe_block = data.project(&self.desc.probe_projections);

        let probe_stream = with_join_hash_method!(|T| match self.basic_state.hash_table.deref() {
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

        Ok(OuterLeftHashJoinStream::create(
            probe_block,
            self.basic_state.clone(),
            probe_stream,
            self.desc.clone(),
            &mut self.performance_context.probe_result,
        ))
    }
}

struct OuterLeftHashJoinStream<'a> {
    desc: Arc<HashJoinDesc>,
    probe_data_block: DataBlock,
    join_state: Arc<BasicHashJoinState>,
    probe_keys_stream: Box<dyn ProbeStream + 'a>,
    probed_rows: &'a mut ProbedRows,
    unmatched_rows: Vec<u64>,
}

unsafe impl<'a> Send for OuterLeftHashJoinStream<'a> {}
unsafe impl<'a> Sync for OuterLeftHashJoinStream<'a> {}

impl<'a> OuterLeftHashJoinStream<'a> {
    pub fn create(
        probe_data_block: DataBlock,
        join_state: Arc<BasicHashJoinState>,
        probe_keys_stream: Box<dyn ProbeStream + 'a>,
        desc: Arc<HashJoinDesc>,
        probed_rows: &'a mut ProbedRows,
    ) -> Box<dyn JoinStream + 'a> {
        Box::new(OuterLeftHashJoinStream {
            desc,
            join_state,
            probed_rows,
            probe_data_block,
            probe_keys_stream,
            unmatched_rows: vec![],
        })
    }
}

impl<'a> JoinStream for OuterLeftHashJoinStream<'a> {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        loop {
            self.probed_rows.clear();
            let max_rows = self.probed_rows.matched_probe.capacity();
            self.probe_keys_stream.advance(self.probed_rows, max_rows)?;

            if !self.probed_rows.unmatched.is_empty() {
                self.unmatched_rows
                    .extend_from_slice(&self.probed_rows.unmatched);
            }

            if self.probed_rows.is_empty() {
                if self.unmatched_rows.is_empty() {
                    return Ok(None);
                }

                let unmatched = std::mem::take(&mut self.unmatched_rows);
                let probe_block = match self.probe_data_block.num_columns() {
                    0 => None,
                    _ => Some(DataBlock::take(&self.probe_data_block, &unmatched)?),
                };

                let types = &self.join_state.column_types;
                let build_block = null_build_block(types, unmatched.len());

                return Ok(Some(final_result_block(
                    &self.desc,
                    probe_block,
                    build_block,
                    unmatched.len(),
                )));
            }

            if self.probed_rows.matched_probe.is_empty() {
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
                    let build_block1 = DataBlock::take_column_vec(
                        self.join_state.columns.as_slice(),
                        self.join_state.column_types.as_slice(),
                        row_ptrs,
                        row_ptrs.len(),
                    );

                    let true_validity = Bitmap::new_constant(true, row_ptrs.len());
                    let entries = build_block1
                        .columns()
                        .iter()
                        .map(|c| wrap_true_validity(c, row_ptrs.len(), &true_validity));
                    Some(DataBlock::from_iter(entries, row_ptrs.len()))
                }
            };

            return Ok(Some(final_result_block(
                &self.desc,
                probe_block,
                build_block,
                self.probed_rows.matched_build.len(),
            )));
        }
    }
}

fn final_result_block(
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

fn null_build_block(types: &[DataType], num_rows: usize) -> Option<DataBlock> {
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
