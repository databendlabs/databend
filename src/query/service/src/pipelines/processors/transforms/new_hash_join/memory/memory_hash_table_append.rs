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

use databend_common_expression::ProjectedBlock;

use crate::pipelines::processors::transforms::new_hash_join::memory::memory_hash_join::MemoryHashJoinState;
use crate::pipelines::processors::transforms::new_hash_join::memory::memory_hash_table::JoinHashTable;
use crate::pipelines::processors::transforms::new_hash_join::ITryCompleteFuture;
use crate::pipelines::processors::transforms::new_hash_join::ITryCompleteStream;
use crate::pipelines::processors::transforms::new_hash_join::Progress;
use crate::pipelines::processors::transforms::new_hash_join::TryCompleteFuture;
use crate::pipelines::processors::transforms::new_hash_join::TryCompleteStream;
use crate::pipelines::processors::transforms::HashJoinHashTable;

pub struct HashTableAppendScheduler {
    state: Arc<MemoryHashJoinState>,
}

impl HashTableAppendScheduler {
    pub fn create(state: Arc<MemoryHashJoinState>) -> TryCompleteStream<Progress> {
        Box::new(HashTableAppendScheduler { state })
    }

    fn steal_task(&self) -> Option<usize> {
        let _guard = self.state.mutex.lock();
        self.state.working_queue.as_mut().pop_front()
    }
}

impl ITryCompleteStream<Progress> for HashTableAppendScheduler {
    fn next_try_complete(
        &mut self,
    ) -> databend_common_exception::Result<Option<TryCompleteFuture<Progress>>> {
        let Some(chunk_num) = self.steal_task() else {
            return Ok(None);
        };

        Ok(Some(HashTableAppendBlock::create(
            chunk_num,
            self.state.clone(),
        )))
    }
}

pub struct HashTableAppendBlock {
    chunk_num: usize,
    state: Arc<MemoryHashJoinState>,
}

impl HashTableAppendBlock {
    pub fn create(num: usize, state: Arc<MemoryHashJoinState>) -> TryCompleteFuture<Progress> {
        TryCompleteFuture::new(HashTableAppendBlock {
            chunk_num: num,
            state,
        })
    }
}

impl ITryCompleteFuture<Progress> for HashTableAppendBlock {
    fn try_complete(&mut self) -> databend_common_exception::Result<Option<Progress>> {
        let block = &mut self.state.chunks.as_mut()[self.chunk_num];

        let key_num = self.state.params.build_keys.len();
        let mut group_keys = Vec::with_capacity(key_num);

        let block_column_num = block.num_columns() - key_num;
        while block.num_columns() > block_column_num {
            group_keys.push(block.remove_column(block_column_num));
        }

        let mut arena = Vec::new();
        let keys = ProjectedBlock::from(&group_keys);

        match self.state.hash_table.deref() {
            HashJoinHashTable::Null => Ok(()),
            HashJoinHashTable::KeysU8(v) => v.insert(keys, self.chunk_num, &mut arena),
            HashJoinHashTable::KeysU16(v) => v.insert(keys, self.chunk_num, &mut arena),
            HashJoinHashTable::KeysU32(v) => v.insert(keys, self.chunk_num, &mut arena),
            HashJoinHashTable::KeysU64(v) => v.insert(keys, self.chunk_num, &mut arena),
            HashJoinHashTable::KeysU128(v) => v.insert(keys, self.chunk_num, &mut arena),
            HashJoinHashTable::KeysU256(v) => v.insert(keys, self.chunk_num, &mut arena),
            HashJoinHashTable::Serializer(v) => v.insert(keys, self.chunk_num, &mut arena),
            HashJoinHashTable::SingleBinary(v) => v.insert(keys, self.chunk_num, &mut arena),
        }?;

        {
            let _guard = self.state.mutex.lock();
            self.state.arenas.as_mut().push(arena);
        }

        Ok(Some(Progress {
            total_rows: *self.state.build_rows.deref(),
            total_bytes: 0,
            progressed_rows: 0,
            progressed_bytes: 0,
        }))
    }
}

impl MemoryHashJoinState {
    pub fn init_working_queue(&self) {
        let working_queue = self.working_queue.as_mut();
        working_queue.extend(0..self.chunks.len());
    }
}
