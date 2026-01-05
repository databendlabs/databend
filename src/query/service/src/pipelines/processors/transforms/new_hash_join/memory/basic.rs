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
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_expression::HashMethodSerializer;
use databend_common_expression::HashMethodSingleBinary;
use databend_common_hashtable::BinaryHashJoinHashMap;
use databend_common_hashtable::HashJoinHashMap;
use databend_common_sql::plans::JoinType;
use ethnum::U256;

use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::transforms::FixedKeyHashJoinHashTable;
use crate::pipelines::processors::transforms::HashJoinHashTable;
use crate::pipelines::processors::transforms::SerializerHashJoinHashTable;
use crate::pipelines::processors::transforms::SingleBinaryHashJoinHashTable;
use crate::pipelines::processors::transforms::UniqueFixedKeyHashJoinHashTable;
use crate::pipelines::processors::transforms::UniqueSerializerHashJoinHashTable;
use crate::pipelines::processors::transforms::UniqueSingleBinaryHashJoinHashTable;
use crate::pipelines::processors::transforms::new_hash_join::common::SquashBlocks;
use crate::sessions::QueryContext;

pub struct BasicHashJoin {
    pub(crate) desc: Arc<HashJoinDesc>,
    pub(crate) squash_block: SquashBlocks,

    pub(crate) method: HashMethodKind,
    pub(crate) function_ctx: FunctionContext,
    pub(crate) state: Arc<BasicHashJoinState>,
}

impl BasicHashJoin {
    pub fn create(
        ctx: &QueryContext,
        function_ctx: FunctionContext,
        method: HashMethodKind,
        desc: Arc<HashJoinDesc>,
        state: Arc<BasicHashJoinState>,
    ) -> Result<Self> {
        let settings = ctx.get_settings();
        let block_size = settings.get_max_block_size()? as usize;
        let block_bytes = settings.get_max_block_size()? as usize;

        Ok(BasicHashJoin {
            desc,
            state,
            method,
            function_ctx,
            squash_block: SquashBlocks::new(block_size, block_bytes),
        })
    }
    pub(crate) fn add_block(&mut self, mut data: Option<DataBlock>) -> Result<()> {
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

    pub(crate) fn final_build(&mut self) -> Result<Option<ProgressValues>> {
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
}

impl BasicHashJoin {
    fn steal_chunk_index(&self) -> Option<usize> {
        let locked = self.state.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);
        self.state.build_queue.as_mut().pop_front()
    }

    pub(crate) fn finalize_chunks(&mut self) {
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

        let mut columns = Vec::with_capacity(self.desc.build_projection.len());
        for offset in 0..self.desc.build_projection.len() {
            let full_columns = self
                .state
                .chunks
                .iter()
                .map(|block| block.get_by_offset(offset).to_column())
                .collect::<Vec<_>>();

            columns.push(Column::take_downcast_column_vec(&full_columns));
        }

        std::mem::swap(&mut columns, self.state.columns.as_mut());
    }

    fn init_memory_hash_table(&mut self) {
        if !matches!(self.state.hash_table.deref(), HashJoinHashTable::Null) {
            return;
        }
        let unique_entry = matches!(self.desc.join_type, JoinType::InnerAny | JoinType::LeftAny);

        let locked = self.state.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);

        if matches!(self.state.hash_table.deref(), HashJoinHashTable::Null) {
            let build_num_rows = *self.state.build_rows.deref();
            *self.state.hash_table.as_mut() = match (self.method.clone(), unique_entry) {
                (HashMethodKind::Serializer(_), false) => {
                    HashJoinHashTable::Serializer(SerializerHashJoinHashTable::new(
                        BinaryHashJoinHashMap::with_build_row_num(build_num_rows),
                        HashMethodSerializer::default(),
                    ))
                }
                (HashMethodKind::Serializer(_), true) => {
                    HashJoinHashTable::UniqueSerializer(UniqueSerializerHashJoinHashTable::new(
                        BinaryHashJoinHashMap::with_build_row_num(build_num_rows),
                        HashMethodSerializer::default(),
                    ))
                }
                (HashMethodKind::SingleBinary(_), false) => {
                    HashJoinHashTable::SingleBinary(SingleBinaryHashJoinHashTable::new(
                        BinaryHashJoinHashMap::with_build_row_num(build_num_rows),
                        HashMethodSingleBinary::default(),
                    ))
                }
                (HashMethodKind::SingleBinary(_), true) => {
                    HashJoinHashTable::UniqueSingleBinary(UniqueSingleBinaryHashJoinHashTable::new(
                        BinaryHashJoinHashMap::with_build_row_num(build_num_rows),
                        HashMethodSingleBinary::default(),
                    ))
                }
                (HashMethodKind::KeysU8(hash_method), false) => {
                    HashJoinHashTable::KeysU8(FixedKeyHashJoinHashTable::new(
                        HashJoinHashMap::<u8>::with_build_row_num(build_num_rows),
                        hash_method,
                    ))
                }
                (HashMethodKind::KeysU8(hash_method), true) => {
                    HashJoinHashTable::UniqueKeysU8(UniqueFixedKeyHashJoinHashTable::new(
                        HashJoinHashMap::<u8, true>::with_build_row_num(build_num_rows),
                        hash_method,
                    ))
                }
                (HashMethodKind::KeysU16(hash_method), false) => {
                    HashJoinHashTable::KeysU16(FixedKeyHashJoinHashTable::new(
                        HashJoinHashMap::<u16>::with_build_row_num(build_num_rows),
                        hash_method,
                    ))
                }
                (HashMethodKind::KeysU16(hash_method), true) => {
                    HashJoinHashTable::UniqueKeysU16(UniqueFixedKeyHashJoinHashTable::new(
                        HashJoinHashMap::<u16, true>::with_build_row_num(build_num_rows),
                        hash_method,
                    ))
                }
                (HashMethodKind::KeysU32(hash_method), false) => {
                    HashJoinHashTable::KeysU32(FixedKeyHashJoinHashTable::new(
                        HashJoinHashMap::<u32>::with_build_row_num(build_num_rows),
                        hash_method,
                    ))
                }
                (HashMethodKind::KeysU32(hash_method), true) => {
                    HashJoinHashTable::UniqueKeysU32(UniqueFixedKeyHashJoinHashTable::new(
                        HashJoinHashMap::<u32, true>::with_build_row_num(build_num_rows),
                        hash_method,
                    ))
                }
                (HashMethodKind::KeysU64(hash_method), false) => {
                    HashJoinHashTable::KeysU64(FixedKeyHashJoinHashTable::new(
                        HashJoinHashMap::<u64>::with_build_row_num(build_num_rows),
                        hash_method,
                    ))
                }
                (HashMethodKind::KeysU64(hash_method), true) => {
                    HashJoinHashTable::UniqueKeysU64(UniqueFixedKeyHashJoinHashTable::new(
                        HashJoinHashMap::<u64, true>::with_build_row_num(build_num_rows),
                        hash_method,
                    ))
                }
                (HashMethodKind::KeysU128(hash_method), false) => {
                    HashJoinHashTable::KeysU128(FixedKeyHashJoinHashTable::new(
                        HashJoinHashMap::<u128>::with_build_row_num(build_num_rows),
                        hash_method,
                    ))
                }
                (HashMethodKind::KeysU128(hash_method), true) => {
                    HashJoinHashTable::UniqueKeysU128(UniqueFixedKeyHashJoinHashTable::new(
                        HashJoinHashMap::<u128, true>::with_build_row_num(build_num_rows),
                        hash_method,
                    ))
                }
                (HashMethodKind::KeysU256(hash_method), false) => {
                    HashJoinHashTable::KeysU256(FixedKeyHashJoinHashTable::new(
                        HashJoinHashMap::<U256>::with_build_row_num(build_num_rows),
                        hash_method,
                    ))
                }
                (HashMethodKind::KeysU256(hash_method), true) => {
                    HashJoinHashTable::UniqueKeysU256(UniqueFixedKeyHashJoinHashTable::new(
                        HashJoinHashMap::<U256, true>::with_build_row_num(build_num_rows),
                        hash_method,
                    ))
                }
            };
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
            HashJoinHashTable::UniqueSerializer(v) => v.insert(keys, chunk_idx, &mut arena)?,
            HashJoinHashTable::UniqueSingleBinary(v) => v.insert(keys, chunk_idx, &mut arena)?,
            HashJoinHashTable::UniqueKeysU8(v) => v.insert(keys, chunk_idx, &mut arena)?,
            HashJoinHashTable::UniqueKeysU16(v) => v.insert(keys, chunk_idx, &mut arena)?,
            HashJoinHashTable::UniqueKeysU32(v) => v.insert(keys, chunk_idx, &mut arena)?,
            HashJoinHashTable::UniqueKeysU64(v) => v.insert(keys, chunk_idx, &mut arena)?,
            HashJoinHashTable::UniqueKeysU128(v) => v.insert(keys, chunk_idx, &mut arena)?,
            HashJoinHashTable::UniqueKeysU256(v) => v.insert(keys, chunk_idx, &mut arena)?,
        };

        if arena.capacity() != 0 {
            let locked = self.state.mutex.lock();
            let _locked = locked.unwrap_or_else(PoisonError::into_inner);
            self.state.arenas.as_mut().push(arena);
        }

        Ok(())
    }
}
