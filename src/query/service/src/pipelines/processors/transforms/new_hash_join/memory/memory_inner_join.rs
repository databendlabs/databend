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
use std::sync::PoisonError;

use databend_common_base::base::ProgressValues;
use databend_common_catalog::table_context::TableContext;
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::with_join_hash_method;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FilterExecutor;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_expression::HashMethodSerializer;
use databend_common_expression::HashMethodSingleBinary;
use databend_common_hashtable::BinaryHashJoinHashMap;
use databend_common_hashtable::HashJoinHashMap;
use ethnum::U256;

use crate::pipelines::processors::transforms::new_hash_join::common::SquashBlocks;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbedRows;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::ProbeData;
use crate::pipelines::processors::transforms::new_hash_join::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::Join;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;
use crate::pipelines::processors::transforms::new_hash_join::memory::memory_state::HashJoinMemoryState;
use crate::pipelines::processors::transforms::new_hash_join::performance::PerformanceContext;
use crate::pipelines::processors::transforms::FixedKeyHashJoinHashTable;
use crate::pipelines::processors::transforms::HashJoinHashTable;
use crate::pipelines::processors::transforms::SerializerHashJoinHashTable;
use crate::pipelines::processors::transforms::SingleBinaryHashJoinHashTable;
use crate::pipelines::processors::HashJoinDesc;
use crate::sessions::QueryContext;

pub struct MemoryInnerJoin {
    desc: Arc<HashJoinDesc>,
    squash_block: SquashBlocks,

    method: HashMethodKind,
    function_ctx: FunctionContext,
    state: Arc<HashJoinMemoryState>,

    performance_context: PerformanceContext,
}

impl MemoryInnerJoin {
    pub fn create(
        ctx: &QueryContext,
        function_ctx: FunctionContext,
        method: HashMethodKind,
        desc: Arc<HashJoinDesc>,
        state: Arc<HashJoinMemoryState>,
    ) -> Result<Self> {
        let settings = ctx.get_settings();
        let block_size = settings.get_max_block_size()? as usize;
        let block_bytes = settings.get_max_block_size()? as usize;

        let context = PerformanceContext::create(block_size, desc.clone(), function_ctx.clone());
        Ok(MemoryInnerJoin {
            desc,
            state,
            method,
            function_ctx,
            squash_block: SquashBlocks::new(block_size, block_bytes),
            performance_context: context,
        })
    }

    fn init_columns_vec(&mut self) {
        if self.desc.build_projection.is_empty() || !self.state.columns.is_empty() {
            return;
        }

        let locked = self.state.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);

        if self.state.chunks.is_empty() || !self.state.columns.is_empty() {
            return;
        }

        if let Some(block) = self.state.chunks.first() {
            for offset in 0..self.desc.build_projection.len() {
                let column_type = self.state.column_types.as_mut();
                column_type.push(block.get_by_offset(offset).data_type());
            }
        }

        for offset in 0..self.desc.build_projection.len() {
            let full_columns = self
                .state
                .chunks
                .iter()
                .map(|block| block.get_by_offset(offset).to_column())
                .collect::<Vec<_>>();

            let columns = self.state.columns.as_mut();
            columns.push(Column::take_downcast_column_vec(&full_columns));
        }
    }

    fn init_memory_hash_table(&mut self) {
        if !matches!(self.state.hash_table.deref(), HashJoinHashTable::Null) {
            return;
        }

        let locked = self.state.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);

        if matches!(self.state.hash_table.deref(), HashJoinHashTable::Null) {
            let build_num_rows = *self.state.build_rows.deref();
            *self.state.hash_table.as_mut() = match self.method.clone() {
                HashMethodKind::Serializer(_) => {
                    HashJoinHashTable::Serializer(SerializerHashJoinHashTable::new(
                        BinaryHashJoinHashMap::with_build_row_num(build_num_rows),
                        HashMethodSerializer::default(),
                    ))
                }
                HashMethodKind::SingleBinary(_) => {
                    HashJoinHashTable::SingleBinary(SingleBinaryHashJoinHashTable::new(
                        BinaryHashJoinHashMap::with_build_row_num(build_num_rows),
                        HashMethodSingleBinary::default(),
                    ))
                }
                HashMethodKind::KeysU8(hash_method) => {
                    HashJoinHashTable::KeysU8(FixedKeyHashJoinHashTable::new(
                        HashJoinHashMap::<u8>::with_build_row_num(build_num_rows),
                        hash_method,
                    ))
                }
                HashMethodKind::KeysU16(hash_method) => {
                    HashJoinHashTable::KeysU16(FixedKeyHashJoinHashTable::new(
                        HashJoinHashMap::<u16>::with_build_row_num(build_num_rows),
                        hash_method,
                    ))
                }
                HashMethodKind::KeysU32(hash_method) => {
                    HashJoinHashTable::KeysU32(FixedKeyHashJoinHashTable::new(
                        HashJoinHashMap::<u32>::with_build_row_num(build_num_rows),
                        hash_method,
                    ))
                }
                HashMethodKind::KeysU64(hash_method) => {
                    HashJoinHashTable::KeysU64(FixedKeyHashJoinHashTable::new(
                        HashJoinHashMap::<u64>::with_build_row_num(build_num_rows),
                        hash_method,
                    ))
                }
                HashMethodKind::KeysU128(hash_method) => {
                    HashJoinHashTable::KeysU128(FixedKeyHashJoinHashTable::new(
                        HashJoinHashMap::<u128>::with_build_row_num(build_num_rows),
                        hash_method,
                    ))
                }
                HashMethodKind::KeysU256(hash_method) => {
                    HashJoinHashTable::KeysU256(FixedKeyHashJoinHashTable::new(
                        HashJoinHashMap::<U256>::with_build_row_num(build_num_rows),
                        hash_method,
                    ))
                }
            }
        }
    }

    fn build_hash_table(&self, keys: DataBlock, chunk_idx: usize) -> Result<()> {
        let mut arena = Vec::with_capacity(0);

        match self.state.hash_table.deref() {
            HashJoinHashTable::Null => (),
            HashJoinHashTable::Serializer(v) => v.insert(keys, chunk_idx, &mut arena)?,
            HashJoinHashTable::SingleBinary(v) => v.insert(keys, chunk_idx, &mut arena)?,
            HashJoinHashTable::KeysU8(v) => v.insert(keys, chunk_idx, &mut arena)?,
            HashJoinHashTable::KeysU16(v) => v.insert(keys, chunk_idx, &mut arena)?,
            HashJoinHashTable::KeysU32(v) => v.insert(keys, chunk_idx, &mut arena)?,
            HashJoinHashTable::KeysU64(v) => v.insert(keys, chunk_idx, &mut arena)?,
            HashJoinHashTable::KeysU128(v) => v.insert(keys, chunk_idx, &mut arena)?,
            HashJoinHashTable::KeysU256(v) => v.insert(keys, chunk_idx, &mut arena)?,
        };

        if arena.capacity() != 0 {
            let locked = self.state.mutex.lock();
            let _locked = locked.unwrap_or_else(PoisonError::into_inner);
            self.state.arenas.as_mut().push(arena);
        }

        Ok(())
    }

    fn steal_chunk_index(&self) -> Option<usize> {
        let locked = self.state.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);
        self.state.build_queue.as_mut().pop_front()
    }
}

impl Join for MemoryInnerJoin {
    fn add_block(&mut self, mut data: Option<DataBlock>) -> Result<()> {
        let mut squashed_block = match data.take() {
            None => self.squash_block.finalize()?,
            Some(data_block) => self.squash_block.add_block(data_block)?,
        };

        if let Some(squashed_block) = squashed_block.take() {
            let locked = self.state.mutex.lock();
            let _locked = locked.unwrap_or_else(PoisonError::into_inner);

            *self.state.build_rows.as_mut() += squashed_block.num_rows();
            let chunk_index = self.state.chunks.len();
            self.state.chunks.as_mut().push(squashed_block);
            self.state.build_queue.as_mut().push_back(chunk_index);
        }

        Ok(())
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        self.init_memory_hash_table();

        let Some(chunk_index) = self.steal_chunk_index() else {
            return Ok(None);
        };

        let mut chunk_block = DataBlock::empty();

        // take storage block
        {
            let chunks = self.state.chunks.as_mut();
            std::mem::swap(&mut chunks[chunk_index], &mut chunk_block);
        }

        let keys_entries = self.desc.build_key(&chunk_block, &self.function_ctx)?;
        let mut keys_block = DataBlock::new(keys_entries, chunk_block.num_rows());

        chunk_block = chunk_block.project(&self.desc.build_projection);
        if let Some(bitmap) = self.desc.build_valids_by_keys(&keys_block)? {
            keys_block = keys_block.filter_with_bitmap(&bitmap)?;

            if bitmap.null_count() != bitmap.len() {
                chunk_block = chunk_block.filter_with_bitmap(&bitmap)?;
            }
        }

        self.desc.remove_keys_nullable(&mut keys_block);

        let num_rows = chunk_block.num_rows();
        let num_bytes = chunk_block.memory_size();

        // restore storage block
        {
            let chunks = self.state.chunks.as_mut();
            std::mem::swap(&mut chunks[chunk_index], &mut chunk_block);
        }

        self.build_hash_table(keys_block, chunk_index)?;

        Ok(Some(ProgressValues {
            rows: num_rows,
            bytes: num_bytes,
        }))
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        if data.is_empty() {
            return Ok(Box::new(EmptyJoinStream));
        }

        self.init_columns_vec();
        let probe_keys = self.desc.probe_key(&data, &self.function_ctx)?;

        let mut keys = DataBlock::new(probe_keys, data.num_rows());
        let valids = match self.desc.from_correlated_subquery {
            true => None,
            false => self.desc.build_valids_by_keys(&keys)?,
        };

        self.desc.remove_keys_nullable(&mut keys);
        let probe_block = data.project(&self.desc.probe_projections);

        let joined_stream: Box<dyn JoinStream + '_> =
            with_join_hash_method!(|T| match self.state.hash_table.deref() {
                HashJoinHashTable::T(table) => {
                    let probe_hash_statistics = &mut self.performance_context.probe_hash_statistics;
                    probe_hash_statistics.clear();

                    let probe_data = ProbeData::new(keys, valids, probe_hash_statistics);
                    let probe_keys_stream = table.probe_matched(probe_data)?;

                    Ok(MemoryInnerJoinStream::create(
                        probe_block,
                        self.state.clone(),
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
            Some(filter_executor) => Ok(FilterJoinStream::create(joined_stream, filter_executor)),
        }
    }

    fn final_probe(&mut self) -> Result<Box<dyn JoinStream>> {
        Ok(Box::new(EmptyJoinStream))
    }
}

struct MemoryInnerJoinStream<'a> {
    desc: Arc<HashJoinDesc>,
    probe_data_block: DataBlock,
    join_state: Arc<HashJoinMemoryState>,
    probe_keys_stream: Box<dyn ProbeStream + 'a>,
    probed_rows: &'a mut ProbedRows,
}

unsafe impl<'a> Send for MemoryInnerJoinStream<'a> {}
unsafe impl<'a> Sync for MemoryInnerJoinStream<'a> {}

impl<'a> MemoryInnerJoinStream<'a> {
    pub fn create(
        probe_data_block: DataBlock,
        join_state: Arc<HashJoinMemoryState>,
        probe_keys_stream: Box<dyn ProbeStream + 'a>,
        desc: Arc<HashJoinDesc>,
        probed_rows: &'a mut ProbedRows,
    ) -> Box<dyn JoinStream + 'a> {
        Box::new(MemoryInnerJoinStream {
            desc,
            join_state,
            probed_rows,
            probe_data_block,
            probe_keys_stream,
        })
    }
}

impl<'a> JoinStream for MemoryInnerJoinStream<'a> {
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

pub struct FilterJoinStream<'a> {
    inner: Box<dyn JoinStream + 'a>,
    filter_executor: &'a mut FilterExecutor,
}

impl<'a> FilterJoinStream<'a> {
    pub fn create(
        inner: Box<dyn JoinStream + 'a>,
        filter_executor: &'a mut FilterExecutor,
    ) -> Box<dyn JoinStream + 'a> {
        Box::new(FilterJoinStream {
            inner,
            filter_executor,
        })
    }
}

impl<'a> JoinStream for FilterJoinStream<'a> {
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
