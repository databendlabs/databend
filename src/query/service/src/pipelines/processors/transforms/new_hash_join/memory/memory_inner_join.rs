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
use databend_common_expression::{with_join_hash_method, BlockEntry, HashMethod};
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_expression::HashMethodSerializer;
use databend_common_expression::HashMethodSingleBinary;
use databend_common_expression::ProjectedBlock;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_hashtable::{BinaryHashJoinHashMap, HashJoinHashtableLike};
use databend_common_hashtable::HashJoinHashMap;
use databend_common_sql::ColumnSet;
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
    build_projection: ColumnSet,
    probe_projections: ColumnSet,
    function_ctx: FunctionContext,
    num_matched: usize,
    processed_probe_rows: usize,
}

impl MemoryInnerJoin {
    pub fn create() -> Self {
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

    fn build_hash_table(&self, keys: Vec<BlockEntry>, chunk_idx: usize) -> Result<()> {
        let mut arena = Vec::new();
        let keys = ProjectedBlock::from(&keys);

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

        if !arena.is_empty() {
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
            std::mem::swap(&mut chunks[chunk_block], &mut chunk_block);
        }

        let mut keys_entries = self.desc.build_key(&chunk_block, &self.function_ctx)?;
        chunk_block = chunk_block.project(&self.build_projection);

        if let Some(bitmap) = self.desc.build_valids_by_keys(&mut keys_entries)? {
            if bitmap.null_count() != bitmap.len() {
                chunk_block = chunk_block.filter_with_bitmap(&bitmap)?;
            }
        }

        let num_rows = chunk_block.num_rows();
        let num_bytes = chunk_block.memory_size();

        // restore storage block
        {
            let chunks = self.state.chunks.as_mut();
            std::mem::swap(&mut chunks[chunk_block], &mut chunk_block);
        }

        self.build_hash_table(keys_entries, chunk_index)?;

        Ok(Some(ProgressValues {
            rows: num_rows,
            bytes: num_bytes,
        }))
    }

    fn probe_block(&mut self, mut data: DataBlock) -> Result<Box<dyn JoinStream>> {
        let mut probe_keys = self.desc.probe_key(&data, &self.function_ctx)?;

        let valids = match self.desc.from_correlated_subquery {
            true => None,
            false => self.desc.build_valids_by_keys(&mut probe_keys)?,
        };

        // Adaptive early filtering.
        // Thanks to the **adaptive** execution strategy of early filtering, we don't experience a performance decrease
        // when all keys have matches. This allows us to achieve the same performance as before.
        self.processed_probe_rows += match valids.as_ref() {
            None => data.num_rows(),
            Some(valids) => valids.len() - valids.null_count(),
        };

        // We use the information from the probed data to predict the matching state of this probe.
        let enable_early_filtering =
            (self.num_matched as f64) / (self.processed_probe_rows as f64) < 0.8;

        let probe_key = ProjectedBlock::from(&probe_keys);
        let mut hashes = Vec::with_capacity(data.num_rows());
        // let mut selection = Vec::with_capacity(data.num_rows());
        with_join_hash_method!(|T| match self.state.hash_table.deref() {
            HashJoinHashTable::T(table) => {
                // Build `keys` and get the hashes of `keys`.
                let keys_state = table.hash_method.build_keys_state(probe_key, data.num_rows())?;
                table.hash_method.build_keys_hashes(&keys_state, &mut hashes);
                let keys = table.hash_method.build_keys_accessor(keys_state.clone())?;

                // probe_state.process_state = Some(ProcessState {
                //     input,
                //     probe_has_null,
                //     keys_state,
                //     next_idx: 0,
                // });

                match enable_early_filtering {
                    true => table.hash_table.early_filtering_matched_probe(
                            &mut hashes,
                            valids,
                            &mut selection,
                        ),
                    false => table.hash_table.probe(&mut hashes, valids)
                }

                // Perform a round of hash table probe.
                // probe_state.probe_with_selection = prefer_early_filtering;
                probe_state.selection_count = if !Self::need_unmatched_selection(
                    &self.hash_join_state.hash_join_desc.join_type,
                    probe_state.with_conjunction,
                ) {
                    if prefer_early_filtering {
                        // Early filtering, use selection to get better performance.
                        table.hash_table.early_filtering_matched_probe(
                            &mut probe_state.hashes,
                            valids,
                            &mut probe_state.selection,
                        )
                    } else {
                        // If don't do early filtering, don't use selection.
                        table.hash_table.probe(&mut probe_state.hashes, valids)
                    }
                } else {
                    if prefer_early_filtering {
                        // Early filtering, use matched selection and unmatched selection to get better performance.
                        let unmatched_selection =
                            probe_state.probe_unmatched_indexes.as_mut().unwrap();
                        let (matched_count, unmatched_count) =
                            table.hash_table.early_filtering_probe(
                                &mut probe_state.hashes,
                                valids,
                                &mut probe_state.selection,
                                unmatched_selection,
                            );
                        probe_state.probe_unmatched_indexes_count = unmatched_count;
                        matched_count
                    } else {
                        // If don't do early filtering, don't use selection.
                        table.hash_table.probe(&mut probe_state.hashes, valids)
                    }
                };
                probe_state.num_keys_hash_matched += probe_state.selection_count as u64;

                // Continue to probe hash table and process data blocks.
                self.result_blocks(probe_state, keys, &table.hash_table)
            }
            HashJoinHashTable::Null => Err(ErrorCode::AbortedQuery(
                "Aborted query, because the hash table is uninitialized.",
            )),
        })
        // let keys_state = self.method.build_keys_state(probe_keys, input_num_rows)?;
        table
            .hash_method
            .build_keys_hashes(&keys_state, &mut probe_state.hashes);
        let keys = table.hash_method.build_keys_accessor(keys_state.clone())?;
        data = data.project(&self.probe_projections)?;

        todo!()
    }

    fn final_probe(&mut self) -> Result<Box<dyn JoinStream>> {
        Ok(Box::new(EmptyJoinStream))
    }
}

struct MemoryInnerJoinStream {}
