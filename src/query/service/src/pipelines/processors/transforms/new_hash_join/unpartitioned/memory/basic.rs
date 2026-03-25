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
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_expression::HashMethodSerializer;
use databend_common_expression::HashMethodSingleBinary;
use databend_common_settings::Settings;
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
use crate::pipelines::processors::transforms::hash_join_table::BinaryHashJoinHashMap;
use crate::pipelines::processors::transforms::hash_join_table::HashJoinHashMap;
use crate::pipelines::processors::transforms::new_hash_join::common::SquashBlocks;

pub struct BasicHashJoin {
    pub(crate) desc: Arc<HashJoinDesc>,
    pub(crate) squash_block: SquashBlocks,

    pub(crate) method: HashMethodKind,
    pub(crate) function_ctx: FunctionContext,
    pub(crate) state: Arc<BasicHashJoinState>,
    nested_loop_join_threshold: usize,
}

impl BasicHashJoin {
    pub fn create(
        settings: &Settings,
        function_ctx: FunctionContext,
        method: HashMethodKind,
        desc: Arc<HashJoinDesc>,
        state: Arc<BasicHashJoinState>,
        nested_loop_join_threshold: usize,
    ) -> Result<Self> {
        let squash_block = SquashBlocks::new(
            settings.get_max_block_size()? as _,
            settings.get_max_block_bytes()? as _,
        );

        Ok(BasicHashJoin {
            desc,
            state,
            method,
            function_ctx,
            squash_block,
            nested_loop_join_threshold,
        })
    }

    pub(crate) fn add_block(&mut self, mut data: Option<DataBlock>) -> Result<()> {
        let mut squashed_block = match data.take() {
            None => self.squash_block.finalize()?,
            Some(data_block) => self.squash_block.add_block(data_block)?,
        };

        if let Some(squashed_block) = squashed_block.take() {
            self.state.push_chunk(squashed_block);
        }

        Ok(())
    }

    pub(crate) fn final_build<const SCAN_MAP: bool>(&mut self) -> Result<Option<ProgressValues>> {
        match self.state.hash_table.deref() {
            HashJoinHashTable::Null => match self.init_memory_hash_table() {
                Some(true) => return Ok(Some(self.build_nested_loop())),
                Some(false) => return Ok(None),
                None => {}
            },
            HashJoinHashTable::NestedLoop(_) => return Ok(None),
            _ => {}
        }

        let Some(chunk_index) = self.state.steal_chunk_index() else {
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
            if bitmap.true_count() != bitmap.len() {
                keys_block = keys_block.filter_with_bitmap(&bitmap)?;

                chunk_block = match SCAN_MAP {
                    true => {
                        let null_keys = chunk_block.clone().filter_with_bitmap(&(!(&bitmap)))?;
                        DataBlock::concat(&[chunk_block.filter_with_bitmap(&bitmap)?, null_keys])?
                    }
                    false => chunk_block.filter_with_bitmap(&bitmap)?,
                };
            }
        }

        self.desc.remove_keys_nullable(&mut keys_block);

        let num_rows = chunk_block.num_rows();
        let num_bytes = chunk_block.memory_size();

        // restore storage block
        {
            let chunks = self.state.chunks.as_mut();

            if SCAN_MAP {
                let mut scan_map = vec![0; chunk_block.num_rows()];
                let scan_maps = self.state.scan_map.as_mut();
                std::mem::swap(&mut scan_maps[chunk_index], &mut scan_map);
            }

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
    pub(crate) fn finalize_chunks(&mut self) {
        if self.desc.build_projection.is_empty() || !self.state.columns.is_empty() {
            return;
        }

        let locked = self.state.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);

        debug_assert!(!matches!(
            self.state.hash_table.deref(),
            HashJoinHashTable::NestedLoop(_)
        ));

        if !self.state.columns.is_empty() {
            return;
        }
        if let Some(block) = self.state.chunks.first() {
            *self.state.column_types.as_mut() = (0..self.desc.build_projection.len())
                .map(|offset| block.get_by_offset(offset).data_type())
                .collect();
        } else {
            return;
        };

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

        *self.state.columns.as_mut() = columns;
    }

    fn init_memory_hash_table(&mut self) -> Option<bool> {
        let locked = self.state.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);

        match self.state.hash_table.deref() {
            HashJoinHashTable::Null => {}
            HashJoinHashTable::NestedLoop(_) => return Some(false),
            _ => return None,
        }

        let build_num_rows = *self.state.build_rows.deref();
        if build_num_rows < self.nested_loop_join_threshold {
            *self.state.hash_table.as_mut() = HashJoinHashTable::NestedLoop(vec![]);
            return Some(true);
        }

        let unique_entry = matches!(self.desc.join_type, JoinType::InnerAny | JoinType::LeftAny)
            || (matches!(self.desc.join_type, JoinType::LeftSemi | JoinType::LeftAnti)
                && self.desc.other_predicate.is_none());

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
        None
    }

    fn build_hash_table(&self, keys: DataBlock, chunk_idx: usize) -> Result<()> {
        let mut arena = Vec::with_capacity(0);

        match self.state.hash_table.deref() {
            HashJoinHashTable::Null => (),
            HashJoinHashTable::NestedLoop(_) => unreachable!(),
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

    fn build_nested_loop(&self) -> ProgressValues {
        let mut progress = ProgressValues::default();
        let mut plain = vec![];
        while let Some(chunk_index) = self.state.steal_chunk_index() {
            let chunk_block = &self.state.chunks[chunk_index];
            progress.rows += chunk_block.num_rows();
            progress.bytes += chunk_block.memory_size();
            plain.push(chunk_block.clone());
        }
        debug_assert!(matches!(
            *self.state.hash_table,
            HashJoinHashTable::NestedLoop(_)
        ));
        *self.state.hash_table.as_mut() = HashJoinHashTable::NestedLoop(plain);
        progress
    }
}
