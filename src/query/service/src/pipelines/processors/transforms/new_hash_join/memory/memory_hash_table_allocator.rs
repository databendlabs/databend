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

use databend_common_hashtable::BinaryHashJoinHashMap;
use databend_common_hashtable::HashJoinHashMap;
use ethnum::U256;

use crate::pipelines::processors::transforms::new_hash_join::memory::memory_hash_join::MemoryHashJoinState;
use crate::pipelines::processors::transforms::new_hash_join::ITryCompleteStream;
use crate::pipelines::processors::transforms::new_hash_join::Progress;
use crate::pipelines::processors::transforms::new_hash_join::TryCompleteFuture;
use crate::pipelines::processors::transforms::new_hash_join::TryCompleteStream;
use crate::pipelines::processors::transforms::FixedKeyHashJoinHashTable;
use crate::pipelines::processors::transforms::HashJoinHashTable;
use crate::pipelines::processors::transforms::SerializerHashJoinHashTable;
use crate::pipelines::processors::transforms::SingleBinaryHashJoinHashTable;

pub struct MemoryHashTableAllocator {
    state: Arc<MemoryHashJoinState>,
}

impl MemoryHashTableAllocator {
    pub fn create(state: Arc<MemoryHashJoinState>) -> TryCompleteStream<Progress> {
        Box::new(MemoryHashTableAllocator { state })
    }
}

impl ITryCompleteStream<Progress> for MemoryHashTableAllocator {
    fn next_try_complete(
        &mut self,
    ) -> databend_common_exception::Result<Option<TryCompleteFuture<Progress>>> {
        let _guard = self.state.mutex.lock();
        let rows = *self.state.build_rows.deref();

        *self.state.hash_table.as_mut() = match std::mem::take(self.state.hash_table.as_mut()) {
            HashJoinHashTable::Null => HashJoinHashTable::Null,
            HashJoinHashTable::Serializer(v) => {
                HashJoinHashTable::Serializer(SerializerHashJoinHashTable {
                    hash_method: v.hash_method,
                    hash_table: BinaryHashJoinHashMap::with_build_row_num(rows),
                })
            }
            HashJoinHashTable::SingleBinary(v) => {
                HashJoinHashTable::SingleBinary(SingleBinaryHashJoinHashTable {
                    hash_method: v.hash_method,
                    hash_table: BinaryHashJoinHashMap::with_build_row_num(rows),
                })
            }
            HashJoinHashTable::KeysU8(v) => HashJoinHashTable::KeysU8(FixedKeyHashJoinHashTable {
                hash_method: v.hash_method,
                hash_table: HashJoinHashMap::<u8>::with_build_row_num(rows),
            }),
            HashJoinHashTable::KeysU16(v) => {
                HashJoinHashTable::KeysU16(FixedKeyHashJoinHashTable {
                    hash_method: v.hash_method,
                    hash_table: HashJoinHashMap::<u16>::with_build_row_num(rows),
                })
            }
            HashJoinHashTable::KeysU32(v) => {
                HashJoinHashTable::KeysU32(FixedKeyHashJoinHashTable {
                    hash_method: v.hash_method,
                    hash_table: HashJoinHashMap::<u32>::with_build_row_num(rows),
                })
            }
            HashJoinHashTable::KeysU64(v) => {
                HashJoinHashTable::KeysU64(FixedKeyHashJoinHashTable {
                    hash_method: v.hash_method,
                    hash_table: HashJoinHashMap::<u64>::with_build_row_num(rows),
                })
            }
            HashJoinHashTable::KeysU128(v) => {
                HashJoinHashTable::KeysU128(FixedKeyHashJoinHashTable {
                    hash_method: v.hash_method,
                    hash_table: HashJoinHashMap::<u128>::with_build_row_num(rows),
                })
            }
            HashJoinHashTable::KeysU256(v) => {
                HashJoinHashTable::KeysU256(FixedKeyHashJoinHashTable {
                    hash_method: v.hash_method,
                    hash_table: HashJoinHashMap::<U256>::with_build_row_num(rows),
                })
            }
        };

        Ok(None)
    }
}
