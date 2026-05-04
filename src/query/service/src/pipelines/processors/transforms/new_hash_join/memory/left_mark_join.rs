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
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::with_join_hash_method;

use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::transforms::HashJoinHashTable;
use crate::pipelines::processors::transforms::Join;
use crate::pipelines::processors::transforms::hash_join_table::RowPtr;
use crate::pipelines::processors::transforms::memory::basic::BasicHashJoin;
use crate::pipelines::processors::transforms::memory::basic_state::SCAN_ROW_MARK_NULL;
use crate::pipelines::processors::transforms::memory::basic_state::SCAN_ROW_MATCHED;
use crate::pipelines::processors::transforms::memory::basic_state::SCAN_ROW_UNMATCHED;
use crate::pipelines::processors::transforms::memory::left_join::final_result_block;
use crate::pipelines::processors::transforms::memory::right_mark_join::create_marker_block;
use crate::pipelines::processors::transforms::memory::right_mark_join::init_markers;
use crate::pipelines::processors::transforms::memory::right_mark_join::nullable_filter;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::ProbeData;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbedRows;
use crate::pipelines::processors::transforms::new_hash_join::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;
use crate::pipelines::processors::transforms::new_hash_join::performance::PerformanceContext;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;

pub struct LeftMarkHashJoin {
    pub(crate) basic_hash_join: BasicHashJoin,
    pub(crate) desc: Arc<HashJoinDesc>,
    pub(crate) function_ctx: FunctionContext,
    pub(crate) basic_state: Arc<BasicHashJoinState>,
    pub(crate) performance_context: PerformanceContext,
    pub(crate) finished: bool,
}

struct BuildMarkerState {
    chunk_index: usize,
    keys: Vec<BlockEntry>,
    validity: Option<Bitmap>,
}

impl LeftMarkHashJoin {
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

        Ok(LeftMarkHashJoin {
            desc,
            basic_hash_join,
            function_ctx,
            basic_state: state,
            performance_context: context,
            finished: false,
        })
    }

    fn next_build_marker_state(&self) -> Result<Option<BuildMarkerState>> {
        let Some(chunk_index) = self.basic_state.build_queue.front().copied() else {
            return Ok(None);
        };
        let chunk = &self.basic_state.chunks[chunk_index];
        let keys = self.desc.build_key(chunk, &self.function_ctx)?;
        if keys.is_empty() {
            return Ok(Some(BuildMarkerState {
                chunk_index,
                keys,
                validity: None,
            }));
        }
        let keys_block = DataBlock::new(keys.clone(), chunk.num_rows());
        let validity = self.desc.build_valids_by_keys(&keys_block)?;
        Ok(Some(BuildMarkerState {
            chunk_index,
            keys,
            validity,
        }))
    }
}

impl Join for LeftMarkHashJoin {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        self.basic_hash_join.add_block(data)
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        let build_marker_state = self.next_build_marker_state()?;
        let progress = self.basic_hash_join.final_build::<true>()?;
        if progress.is_some() {
            if let Some(build_marker_state) = build_marker_state {
                let scan_map =
                    &mut self.basic_state.scan_map.as_mut()[build_marker_state.chunk_index];
                match build_marker_state.validity {
                    Some(validity) => {
                        let valid_count = validity.true_count();
                        let mut invalid_count = 0;
                        for is_valid in validity.iter() {
                            if !is_valid {
                                let scan_idx = valid_count + invalid_count;
                                invalid_count += 1;
                                if scan_map[scan_idx] == SCAN_ROW_UNMATCHED {
                                    scan_map[scan_idx] = SCAN_ROW_MARK_NULL;
                                }
                            }
                        }
                    }
                    None => init_markers(
                        ProjectedBlock::from(build_marker_state.keys.as_slice()),
                        scan_map.len(),
                        scan_map,
                    ),
                }
            }
        }
        Ok(progress)
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        if data.is_empty() || *self.basic_state.build_rows == 0 {
            return Ok(Box::new(EmptyJoinStream));
        }

        self.basic_hash_join.finalize_chunks();
        let probe_keys = self.desc.probe_key(&data, &self.function_ctx)?;
        let mut probe_keys = DataBlock::new(probe_keys, data.num_rows());
        let valids = self.desc.build_valids_by_keys(&probe_keys)?;
        if valids
            .as_ref()
            .is_some_and(|valids| valids.null_count() > 0)
        {
            let mut has_null = self.desc.marker_join_desc.has_null.write();
            *has_null = true;
        }
        if self.desc.from_correlated_subquery {
            let mut has_null = self.desc.marker_join_desc.has_null.write();
            *has_null = false;
        }
        self.desc.remove_keys_nullable(&mut probe_keys);

        let probe_stream = with_join_hash_method!(|T| match self.basic_state.hash_table.deref() {
            HashJoinHashTable::T(table) => {
                let probe_hash_statistics = &mut self.performance_context.probe_hash_statistics;
                probe_hash_statistics.clear(data.num_rows());
                let probe_data = ProbeData::new(probe_keys, valids, probe_hash_statistics);
                table.probe_matched(probe_data)
            }
            HashJoinHashTable::NestedLoop(_) => unreachable!(),
            HashJoinHashTable::Null => Err(ErrorCode::AbortedQuery(
                "Aborted query, because the hash table is uninitialized.",
            )),
        })?;

        Ok(LeftMarkHashJoinStream::create(
            data.project(&self.desc.probe_projection),
            self.basic_state.clone(),
            probe_stream,
            self.desc.clone(),
            self.function_ctx.clone(),
            &mut self.performance_context.probe_result,
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
        Ok(Some(LeftMarkHashJoinFinalStream::create(
            max_rows,
            self.desc.clone(),
            self.basic_state.clone(),
        )))
    }
}

struct LeftMarkHashJoinStream<'a> {
    desc: Arc<HashJoinDesc>,
    function_ctx: FunctionContext,
    probe_data_block: DataBlock,
    join_state: Arc<BasicHashJoinState>,
    probe_keys_stream: Box<dyn ProbeStream + 'a>,
    probed_rows: &'a mut ProbedRows,
}

unsafe impl<'a> Send for LeftMarkHashJoinStream<'a> {}
unsafe impl<'a> Sync for LeftMarkHashJoinStream<'a> {}

impl<'a> LeftMarkHashJoinStream<'a> {
    pub fn create(
        probe_data_block: DataBlock,
        join_state: Arc<BasicHashJoinState>,
        probe_keys_stream: Box<dyn ProbeStream + 'a>,
        desc: Arc<HashJoinDesc>,
        function_ctx: FunctionContext,
        probed_rows: &'a mut ProbedRows,
    ) -> Box<dyn JoinStream + 'a> {
        Box::new(LeftMarkHashJoinStream {
            desc,
            function_ctx,
            join_state,
            probed_rows,
            probe_data_block,
            probe_keys_stream,
        })
    }
}

impl<'a> JoinStream for LeftMarkHashJoinStream<'a> {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        loop {
            self.probed_rows.clear();
            let max_rows = self.probed_rows.matched_probe.capacity();
            self.probe_keys_stream.advance(self.probed_rows, max_rows)?;

            if self.probed_rows.is_empty() {
                return Ok(None);
            }
            if self.probed_rows.matched_probe.is_empty() {
                continue;
            }

            match self.desc.other_predicate.as_ref() {
                None => {
                    for row_ptr in &self.probed_rows.matched_build {
                        self.mark_true(*row_ptr);
                    }
                }
                Some(predicate) => {
                    let probe_block = match self.probe_data_block.num_columns() {
                        0 => None,
                        _ => Some(DataBlock::take(
                            &self.probe_data_block,
                            self.probed_rows.matched_probe.as_slice(),
                        )?),
                    };
                    let build_block = match self.join_state.columns.is_empty() {
                        true => None,
                        false => Some(DataBlock::take_column_vec(
                            self.join_state.columns.as_slice(),
                            self.join_state.column_types.as_slice(),
                            self.probed_rows.matched_build.as_slice(),
                        )),
                    };
                    let result_block = final_result_block(
                        &self.desc,
                        probe_block,
                        build_block,
                        self.probed_rows.matched_build.len(),
                    );
                    let filter = nullable_filter(&result_block, predicate, &self.function_ctx)?;
                    for (idx, row_ptr) in self.probed_rows.matched_build.iter().enumerate() {
                        if unsafe { !filter.validity.get_bit_unchecked(idx) } {
                            self.mark_null(*row_ptr);
                        } else if unsafe { filter.column.get_bit_unchecked(idx) } {
                            self.mark_true(*row_ptr);
                        }
                    }
                }
            }
        }
    }
}

impl LeftMarkHashJoinStream<'_> {
    fn mark_true(&self, row_ptr: RowPtr) {
        self.join_state.scan_map.as_mut()[row_ptr.chunk_index as usize]
            [row_ptr.row_index as usize] = SCAN_ROW_MATCHED;
    }

    fn mark_null(&self, row_ptr: RowPtr) {
        let state = &mut self.join_state.scan_map.as_mut()[row_ptr.chunk_index as usize]
            [row_ptr.row_index as usize];
        if *state == SCAN_ROW_UNMATCHED {
            *state = SCAN_ROW_MARK_NULL;
        }
    }
}

struct LeftMarkHashJoinFinalStream<'a> {
    max_rows: usize,
    desc: Arc<HashJoinDesc>,
    join_state: Arc<BasicHashJoinState>,
    scan_idx: Vec<RowPtr>,
    markers: Vec<u8>,
    scan_progress: Option<(usize, usize)>,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> JoinStream for LeftMarkHashJoinFinalStream<'a> {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        while let Some((chunk_idx, row_idx)) = self.scan_progress.take() {
            let scan_map = &self.join_state.scan_map[chunk_idx];
            let remain_rows = self.max_rows - self.scan_idx.len();
            let remain_rows = std::cmp::min(remain_rows, scan_map.len() - row_idx);

            for idx in (row_idx..scan_map.len()).take(remain_rows) {
                assume(idx < scan_map.len());
                assume(self.scan_idx.len() < self.scan_idx.capacity());
                self.scan_idx.push(RowPtr {
                    chunk_index: chunk_idx as u32,
                    row_index: idx as u32,
                });
                self.markers.push(scan_map[idx]);
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
        let has_null = *self.desc.marker_join_desc.has_null.read();
        let build_block = match self.join_state.columns.is_empty() {
            true => Some(DataBlock::new(vec![], num_rows)),
            false => Some(DataBlock::take_column_vec(
                self.join_state.columns.as_slice(),
                self.join_state.column_types.as_slice(),
                self.scan_idx.as_slice(),
            )),
        };
        let marker_block = Some(create_marker_block(has_null, &self.markers));
        self.scan_idx.clear();
        self.markers.clear();
        Ok(Some(final_result_block(
            &self.desc,
            build_block,
            marker_block,
            num_rows,
        )))
    }
}

impl<'a> LeftMarkHashJoinFinalStream<'a> {
    pub fn create(
        max_rows: usize,
        desc: Arc<HashJoinDesc>,
        join_state: Arc<BasicHashJoinState>,
    ) -> Box<dyn JoinStream + 'a> {
        let scan_progress = join_state.steal_scan_chunk_index();
        Box::new(LeftMarkHashJoinFinalStream {
            max_rows,
            desc,
            join_state,
            scan_progress,
            scan_idx: Vec::with_capacity(max_rows),
            markers: Vec::with_capacity(max_rows),
            _marker: Default::default(),
        })
    }
}
