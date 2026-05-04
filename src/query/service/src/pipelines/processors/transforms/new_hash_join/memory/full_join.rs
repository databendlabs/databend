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
use databend_common_expression::types::DataType;
use databend_common_expression::with_join_hash_method;

use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::transforms::HashJoinHashTable;
use crate::pipelines::processors::transforms::Join;
use crate::pipelines::processors::transforms::hash_join_table::RowPtr;
use crate::pipelines::processors::transforms::memory::basic::BasicHashJoin;
use crate::pipelines::processors::transforms::memory::basic::SCAN_MAP_MATCHED;
use crate::pipelines::processors::transforms::memory::basic_state::SCAN_ROW_MATCHED;
use crate::pipelines::processors::transforms::memory::basic_state::SCAN_ROW_UNMATCHED;
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

pub struct FullHashJoin {
    pub(crate) basic_hash_join: BasicHashJoin,
    pub(crate) desc: Arc<HashJoinDesc>,
    pub(crate) function_ctx: FunctionContext,
    pub(crate) basic_state: Arc<BasicHashJoinState>,
    pub(crate) performance_context: PerformanceContext,
    pub(crate) finished: bool,
}

impl FullHashJoin {
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

        Ok(FullHashJoin {
            desc,
            basic_hash_join,
            function_ctx,
            basic_state: state,
            performance_context: context,
            finished: false,
        })
    }
}

impl Join for FullHashJoin {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        self.basic_hash_join.add_block(data)
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        self.basic_hash_join.final_build::<SCAN_MAP_MATCHED>()
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
            let probe_block = Some(wrap_nullable_block(
                &data.project(&self.desc.probe_projection),
            ));
            return Ok(Box::new(OneBlockJoinStream(Some(final_result_block(
                &self.desc,
                probe_block,
                build_block,
                num_rows,
            )))));
        }

        self.basic_hash_join.finalize_chunks();
        let nullable_block = wrap_nullable_block(&data);
        let probe_keys = self.desc.probe_key(&nullable_block, &self.function_ctx)?;
        let mut probe_keys = DataBlock::new(probe_keys, data.num_rows());
        let valids = self.desc.build_valids_by_keys(&probe_keys)?;
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

        Ok(FullHashJoinStream::create(
            probe_block,
            self.basic_state.clone(),
            probe_stream,
            self.desc.clone(),
            &mut self.performance_context.probe_result,
            self.performance_context.filter_executor.as_mut(),
        ))
    }

    fn final_probe(&mut self) -> Result<Option<Box<dyn JoinStream + '_>>> {
        self.basic_hash_join.finalize_chunks();
        if self.finished {
            return Ok(None);
        }

        self.finished = true;
        let max_rows = self
            .performance_context
            .probe_result
            .matched_probe
            .capacity();
        Ok(Some(FullHashJoinFinalStream::create(
            max_rows,
            self.desc.clone(),
            self.basic_state.clone(),
        )))
    }
}

struct FullHashJoinStream<'a> {
    desc: Arc<HashJoinDesc>,
    probe_data_block: DataBlock,
    join_state: Arc<BasicHashJoinState>,
    probe_keys_stream: Box<dyn ProbeStream + 'a>,
    probed_rows: &'a mut ProbedRows,
    filter_executor: Option<&'a mut FilterExecutor>,
    conjunct_unmatched: Vec<u8>,
    unmatched_rows: Vec<u64>,
}

unsafe impl<'a> Send for FullHashJoinStream<'a> {}
unsafe impl<'a> Sync for FullHashJoinStream<'a> {}

impl<'a> FullHashJoinStream<'a> {
    pub fn create(
        probe_data_block: DataBlock,
        join_state: Arc<BasicHashJoinState>,
        probe_keys_stream: Box<dyn ProbeStream + 'a>,
        desc: Arc<HashJoinDesc>,
        probed_rows: &'a mut ProbedRows,
        filter_executor: Option<&'a mut FilterExecutor>,
    ) -> Box<dyn JoinStream + 'a> {
        let num_rows = probe_data_block.num_rows();
        Box::new(FullHashJoinStream {
            desc,
            join_state,
            probed_rows,
            probe_data_block,
            probe_keys_stream,
            filter_executor,
            conjunct_unmatched: vec![0; num_rows],
            unmatched_rows: Vec::with_capacity(num_rows),
        })
    }

    fn unmatched_block(&mut self) -> Result<Option<DataBlock>> {
        let unmatched_row_id = if self.filter_executor.is_some() {
            std::mem::take(&mut self.conjunct_unmatched)
                .into_iter()
                .enumerate()
                .filter(|(_, matched)| *matched == 0)
                .map(|(row_id, _)| row_id as u64)
                .collect::<Vec<_>>()
        } else {
            std::mem::take(&mut self.unmatched_rows)
        };

        if unmatched_row_id.is_empty() {
            return Ok(None);
        }

        let probe_block = match self.probe_data_block.num_columns() {
            0 => None,
            _ => Some(wrap_nullable_block(&DataBlock::take(
                &self.probe_data_block,
                unmatched_row_id.as_slice(),
            )?)),
        };
        let build_block = null_block(&self.join_state.column_types, unmatched_row_id.len());
        Ok(Some(final_result_block(
            &self.desc,
            probe_block,
            build_block,
            unmatched_row_id.len(),
        )))
    }
}

impl<'a> JoinStream for FullHashJoinStream<'a> {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        loop {
            self.probed_rows.clear();
            let max_rows = self.probed_rows.matched_probe.capacity();
            self.probe_keys_stream.advance(self.probed_rows, max_rows)?;

            if self.filter_executor.is_none() && !self.probed_rows.unmatched.is_empty() {
                self.unmatched_rows
                    .extend_from_slice(&self.probed_rows.unmatched);
            }

            if self.probed_rows.is_empty() {
                return self.unmatched_block();
            }

            if self.probed_rows.matched_probe.is_empty() {
                continue;
            }

            let probe_block = match self.probe_data_block.num_columns() {
                0 => None,
                _ => Some(wrap_nullable_block(&DataBlock::take(
                    &self.probe_data_block,
                    self.probed_rows.matched_probe.as_slice(),
                )?)),
            };
            let build_block = match self.join_state.columns.is_empty() {
                true => None,
                false => Some(wrap_nullable_block(&DataBlock::take_column_vec(
                    self.join_state.columns.as_slice(),
                    self.join_state.column_types.as_slice(),
                    self.probed_rows.matched_build.as_slice(),
                ))),
            };
            let result_block = final_result_block(
                &self.desc,
                probe_block,
                build_block,
                self.probed_rows.matched_build.len(),
            );

            let Some(filter_executor) = self.filter_executor.as_mut() else {
                for row_ptr in &self.probed_rows.matched_build {
                    self.join_state.scan_map.as_mut()[row_ptr.chunk_index as usize]
                        [row_ptr.row_index as usize] = SCAN_ROW_MATCHED;
                }
                return Ok(Some(result_block));
            };

            let result_count = filter_executor.select(&result_block)?;
            if result_count == 0 {
                continue;
            }

            for idx in filter_executor.true_selection().iter().take(result_count) {
                let idx = *idx as usize;
                let probe_idx = self.probed_rows.matched_probe[idx] as usize;
                assume(probe_idx < self.conjunct_unmatched.len());
                self.conjunct_unmatched[probe_idx] = 1;

                let row_ptr = self.probed_rows.matched_build[idx];
                self.join_state.scan_map.as_mut()[row_ptr.chunk_index as usize]
                    [row_ptr.row_index as usize] = SCAN_ROW_MATCHED;
            }

            let num_rows = result_block.num_rows();
            return Ok(Some(filter_executor.take(
                result_block,
                num_rows,
                result_count,
            )?));
        }
    }
}

struct FullHashJoinFinalStream<'a> {
    max_rows: usize,
    desc: Arc<HashJoinDesc>,
    join_state: Arc<BasicHashJoinState>,
    scan_idx: Vec<RowPtr>,
    scan_progress: Option<(usize, usize)>,
    types: Vec<DataType>,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> JoinStream for FullHashJoinFinalStream<'a> {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        while let Some((chunk_idx, row_idx)) = self.scan_progress.take() {
            let scan_map = &self.join_state.scan_map[chunk_idx];
            let remain_rows = self.max_rows - self.scan_idx.len();
            let remain_rows = std::cmp::min(remain_rows, scan_map.len() - row_idx);

            for idx in (row_idx..scan_map.len()).take(remain_rows) {
                assume(idx < scan_map.len());
                assume(self.scan_idx.len() < self.scan_idx.capacity());
                if scan_map[idx] == SCAN_ROW_UNMATCHED {
                    self.scan_idx.push(RowPtr {
                        chunk_index: chunk_idx as u32,
                        row_index: idx as u32,
                    });
                }
            }

            let new_row_idx = row_idx + remain_rows;
            self.scan_progress = match new_row_idx >= scan_map.len() {
                true => self.join_state.steal_scan_chunk_index(),
                false => Some((chunk_idx, new_row_idx)),
            };

            if self.scan_idx.len() >= self.max_rows {
                break;
            }
        }

        if self.scan_idx.is_empty() {
            return Ok(None);
        }

        let num_rows = self.scan_idx.len();
        let probe_block = null_block(&self.types, num_rows);
        let build_block = match self.join_state.columns.is_empty() {
            true => None,
            false => Some(wrap_nullable_block(&DataBlock::take_column_vec(
                self.join_state.columns.as_slice(),
                self.join_state.column_types.as_slice(),
                self.scan_idx.as_slice(),
            ))),
        };
        self.scan_idx.clear();
        Ok(Some(final_result_block(
            &self.desc,
            probe_block,
            build_block,
            num_rows,
        )))
    }
}

impl<'a> FullHashJoinFinalStream<'a> {
    pub fn create(
        max_rows: usize,
        desc: Arc<HashJoinDesc>,
        join_state: Arc<BasicHashJoinState>,
    ) -> Box<dyn JoinStream + 'a> {
        let scan_progress = join_state.steal_scan_chunk_index();
        let mut types = vec![];
        for (i, field) in desc.probe_schema.fields().iter().enumerate() {
            if desc.probe_projection.contains(&i) {
                types.push(field.data_type().clone());
            }
        }

        Box::new(FullHashJoinFinalStream {
            max_rows,
            desc,
            join_state,
            scan_progress,
            types,
            scan_idx: Vec::with_capacity(max_rows),
            _marker: Default::default(),
        })
    }
}
