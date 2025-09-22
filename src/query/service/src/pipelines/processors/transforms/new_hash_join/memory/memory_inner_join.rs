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
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;
use databend_common_expression::arrow::and_validities;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::HashMethodKind;
use databend_common_expression::HashMethodSerializer;
use databend_common_expression::HashMethodSingleBinary;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_hashtable::BinaryHashJoinHashMap;
use databend_common_hashtable::HashJoinHashMap;
use ethnum::U256;

use crate::pipelines::processors::transforms::new_hash_join::common::SquashBlocks;
use crate::pipelines::processors::transforms::new_hash_join::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::Join;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;
use crate::pipelines::processors::transforms::new_hash_join::memory::memory_state::HashJoinMemoryState;
use crate::pipelines::processors::transforms::FixedKeyHashJoinHashTable;
use crate::pipelines::processors::transforms::HashJoinHashTable;
use crate::pipelines::processors::transforms::SerializerHashJoinHashTable;
use crate::pipelines::processors::transforms::SingleBinaryHashJoinHashTable;
use crate::pipelines::processors::HashJoinDesc;

pub struct MemoryInnerJoin {
    desc: Arc<HashJoinDesc>,
    squash_block: SquashBlocks,

    method: HashMethodKind,
    state: Arc<HashJoinMemoryState>,
}

impl MemoryInnerJoin {
    pub fn create() -> Box<dyn Join> {
        unimplemented!()
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
                    HashJoinHashTable::Serializer(SerializerHashJoinHashTable {
                        hash_table: BinaryHashJoinHashMap::with_build_row_num(build_num_rows),
                        hash_method: HashMethodSerializer::default(),
                    })
                }
                HashMethodKind::SingleBinary(_) => {
                    HashJoinHashTable::SingleBinary(SingleBinaryHashJoinHashTable {
                        hash_table: BinaryHashJoinHashMap::with_build_row_num(build_num_rows),
                        hash_method: HashMethodSingleBinary::default(),
                    })
                }
                HashMethodKind::KeysU8(hash_method) => {
                    HashJoinHashTable::KeysU8(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u8>::with_build_row_num(build_num_rows),
                        hash_method,
                    })
                }
                HashMethodKind::KeysU16(hash_method) => {
                    HashJoinHashTable::KeysU16(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u16>::with_build_row_num(build_num_rows),
                        hash_method,
                    })
                }
                HashMethodKind::KeysU32(hash_method) => {
                    HashJoinHashTable::KeysU32(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u32>::with_build_row_num(build_num_rows),
                        hash_method,
                    })
                }
                HashMethodKind::KeysU64(hash_method) => {
                    HashJoinHashTable::KeysU64(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u64>::with_build_row_num(build_num_rows),
                        hash_method,
                    })
                }
                HashMethodKind::KeysU128(hash_method) => {
                    HashJoinHashTable::KeysU128(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u128>::with_build_row_num(build_num_rows),
                        hash_method,
                    })
                }
                HashMethodKind::KeysU256(hash_method) => {
                    HashJoinHashTable::KeysU256(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<U256>::with_build_row_num(build_num_rows),
                        hash_method,
                    })
                }
            }
        }
    }

    fn build_hash_table(&self, data_block: &DataBlock) -> Result<()> {
        match self.state.hash_table.deref() {
            HashJoinHashTable::Null => {}
            HashJoinHashTable::Serializer(_) => {}
            HashJoinHashTable::SingleBinary(_) => {}
            HashJoinHashTable::KeysU8(v) => {}
            HashJoinHashTable::KeysU16(v) => {}
            HashJoinHashTable::KeysU32(v) => {}
            HashJoinHashTable::KeysU64(v) => {}
            HashJoinHashTable::KeysU128(v) => {}
            HashJoinHashTable::KeysU256(v) => {}
        }
        // self.state.h
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

        let data_block = &self.state.chunks[chunk_index];

        let num_rows = data_block.num_rows();
        let num_bytes = data_block.memory_size();

        let progress_values = ProgressValues {
            rows: num_rows,
            bytes: num_bytes,
        };

        Ok(Some(progress_values))
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream>> {
        todo!()
    }

    fn final_probe(&mut self) -> Result<Box<dyn JoinStream>> {
        Ok(Box::new(EmptyJoinStream))
    }
}

// pub fn build_join_keys(block: DataBlock, desc: &HashJoinDesc) -> Result<DataBlock> {
//     let build_keys = &desc.build_keys;
//
//     let evaluator = Evaluator::new(&block, &desc.func_ctx, &BUILTIN_FUNCTIONS);
//     let keys_entries: Vec<BlockEntry> = build_keys
//         .iter()
//         .map(|expr| {
//             Ok(evaluator
//                 .run(expr)?
//                 .convert_to_full_column(expr.data_type(), block.num_rows())
//                 .into())
//         })
//         .collect::<Result<_>>()?;
//
//     // projection data blocks
//     let column_nums = block.num_columns();
//     let mut block_entries = Vec::with_capacity(desc.build_projections.len());
//
//     for index in 0..column_nums {
//         if !desc.build_projections.contains(&index) {
//             continue;
//         }
//
//         block_entries.push(block.get_by_offset(index).clone());
//     }
//
//     let mut projected_block = DataBlock::new(block_entries, block.num_rows());
//     // After computing complex join key expressions, we discard unnecessary columns as soon as possible to expect the release of memory.
//     drop(block);
//
//     let is_null_equal = &desc.is_null_equal;
//     let mut valids = None;
//
//     for (entry, null_equals) in keys_entries.iter().zip(is_null_equal.iter()) {
//         if !null_equals {
//             let (is_all_null, column_valids) = entry.as_column().unwrap().validity();
//
//             if is_all_null {
//                 valids = Some(Bitmap::new_constant(false, projected_block.num_rows()));
//                 break;
//             }
//
//             valids = and_validities(valids, column_valids.cloned());
//
//             if let Some(bitmap) = valids.as_ref() {
//                 if bitmap.null_count() == bitmap.len() {
//                     break;
//                 }
//
//                 if bitmap.null_count() == 0 {
//                     valids = None;
//                 }
//             }
//         }
//     }
//
//     for (entry, is_null) in keys_entries.into_iter().zip(is_null_equal.iter()) {
//         projected_block.add_entry(match !is_null && entry.data_type().is_nullable() {
//             true => entry.remove_nullable(),
//             false => entry,
//         });
//     }
//
//     if let Some(bitmap) = valids {
//         if bitmap.null_count() != bitmap.len() {
//             return projected_block.filter_with_bitmap(&bitmap);
//         }
//     }
//
//     Ok(projected_block)
// }
