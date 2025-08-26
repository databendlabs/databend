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

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;

use crate::pipelines::processors::transforms::new_hash_join::compact_blocks;
use crate::pipelines::processors::transforms::new_hash_join::memory::memory_hash_table_allocator::MemoryHashTableAllocator;
use crate::pipelines::processors::transforms::new_hash_join::memory::memory_hash_table_append::HashTableAppendScheduler;
use crate::pipelines::processors::transforms::new_hash_join::CStyleCell;
use crate::pipelines::processors::transforms::new_hash_join::FlattenTryCompleteStream;
use crate::pipelines::processors::transforms::new_hash_join::IgnorePanicMutex;
use crate::pipelines::processors::transforms::new_hash_join::Join;
use crate::pipelines::processors::transforms::new_hash_join::JoinParams;
use crate::pipelines::processors::transforms::new_hash_join::JoinSettings;
use crate::pipelines::processors::transforms::new_hash_join::NoneTryCompleteStream;
use crate::pipelines::processors::transforms::new_hash_join::ProbeData;
use crate::pipelines::processors::transforms::new_hash_join::Progress;
use crate::pipelines::processors::transforms::new_hash_join::TryCompleteStream;
use crate::pipelines::processors::transforms::HashJoinHashTable;

pub struct MemoryHashJoin {
    state: Arc<MemoryHashJoinState>,
}

impl Join for MemoryHashJoin {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn add_block(&self, block: DataBlock) -> Result<TryCompleteStream<Progress>> {
        if self.state.settings.check_threshold(&block) {
            let _guard = self.state.mutex.lock();

            *self.state.build_rows.as_mut() += block.num_rows();
            self.state.chunks.as_mut().push(block);
            return Ok(NoneTryCompleteStream::create());
        }

        // compact for small blocks
        let _guard = self.state.mutex.lock();
        *self.state.build_rows.as_mut() += block.num_rows();
        self.state.small_chunks.as_mut().push_back(block);
        Ok(NoneTryCompleteStream::create())
    }

    fn finish_build(&self) -> Result<TryCompleteStream<Progress>> {
        // The compact process may still OOM, but the probability is very low.
        let _guard = self.state.mutex.lock();

        // 1. Compact undersized blocks into uniformly-sized blocks.
        self.state.compact_small_blocks()?;

        // 2. Initialize task queue for parallel building.
        self.state.init_working_queue();

        // Large memory operations need to be performed under the protection of a stream.
        Ok(FlattenTryCompleteStream::create(vec![
            // 3. init hash table with fixed memory size
            MemoryHashTableAllocator::create(self.state.clone()),
            // 4. Populate the hashtable with the collected data chunks.
            HashTableAppendScheduler::create(self.state.clone()),
        ]))
    }

    fn probe(&self, _block: DataBlock) -> Result<TryCompleteStream<ProbeData>> {
        todo!()
    }

    fn finish_probe(&self) -> Result<TryCompleteStream<ProbeData>> {
        Ok(NoneTryCompleteStream::create())
    }
}

pub struct MemoryHashJoinState {
    pub params: JoinParams,
    pub settings: JoinSettings,

    pub mutex: IgnorePanicMutex<()>,
    pub build_rows: CStyleCell<usize>,
    pub chunks: CStyleCell<Vec<DataBlock>>,
    pub small_chunks: CStyleCell<VecDeque<DataBlock>>,

    pub working_queue: CStyleCell<VecDeque<usize>>,

    pub hash_table: CStyleCell<HashJoinHashTable>,
    pub arenas: CStyleCell<Vec<Vec<u8>>>,
}

impl MemoryHashJoinState {
    pub fn compact_small_blocks(&self) -> Result<()> {
        if self.small_chunks.is_empty() {
            return Ok(());
        }

        let small_blocks = std::mem::take(self.small_chunks.as_mut());
        let compacted_blocks = compact_blocks(
            small_blocks,
            self.settings.max_block_rows,
            self.settings.max_block_bytes,
        )?;

        let chunks = self.chunks.as_mut();
        chunks.extend(compacted_blocks);
        Ok(())
    }
}
