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
use std::cell::SyncUnsafeCell;
use std::collections::VecDeque;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnVec;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::pipelines::processors::transforms::new_hash_join::compact_blocks;
use crate::pipelines::processors::transforms::new_hash_join::memory_hash_table::GlobalMemoryHashTable;
use crate::pipelines::processors::transforms::new_hash_join::memory_hash_table::JoinHashTable;
use crate::pipelines::processors::transforms::new_hash_join::CStyleCell;
use crate::pipelines::processors::transforms::new_hash_join::ITryCompleteFuture;
use crate::pipelines::processors::transforms::new_hash_join::ITryCompleteStream;
use crate::pipelines::processors::transforms::new_hash_join::IgnorePanicMutex;
use crate::pipelines::processors::transforms::new_hash_join::Join;
use crate::pipelines::processors::transforms::new_hash_join::JoinParams;
use crate::pipelines::processors::transforms::new_hash_join::JoinSettings;
use crate::pipelines::processors::transforms::new_hash_join::NoneTryCompleteStream;
use crate::pipelines::processors::transforms::new_hash_join::ProbeData;
use crate::pipelines::processors::transforms::new_hash_join::Progress;
use crate::pipelines::processors::transforms::new_hash_join::SequenceStream;
use crate::pipelines::processors::transforms::new_hash_join::TryCompleteFuture;
use crate::pipelines::processors::transforms::new_hash_join::TryCompleteStream;

pub struct MemoryHashJoin<Table: JoinHashTable> {
    state: Arc<MemoryHashJoinState>,

    table: Arc<GlobalMemoryHashTable<Table>>,
}

impl<Table: JoinHashTable> Join for MemoryHashJoin<Table> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn add_block(&self, block: DataBlock) -> Result<TryCompleteStream<Progress>> {
        if self.state.settings.check_threshold(&block) {
            let _guard = self.state.mutex.lock();

            self.state.build_rows.as_mut() += block.num_rows();
            self.state.chunks.as_mut().push(block);
            return Ok(NoneTryCompleteStream::create());
        }

        // compact for small blocks
        let _guard = self.state.mutex.lock();
        self.state.build_rows.as_mut() += block.num_rows();
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
        Ok(SequenceStream::create(vec![
            // 3. init hash table with fixed memory size
            ReserverHashTableEntities::create(self.state.clone()),
            // 4. Populate the hashtable with the collected data chunks.
            BuildHashTableEntities::create(self.state.clone()),
        ]))
    }

    fn probe(&self, block: DataBlock) -> Result<TryCompleteStream<ProbeData>> {
        todo!()
    }

    fn finish_probe(&self) -> Result<TryCompleteStream<ProbeData>> {
        todo!()
    }
}

struct MemoryHashJoinState {
    settings: JoinSettings,

    mutex: IgnorePanicMutex<()>,
    build_rows: CStyleCell<usize>,
    chunks: CStyleCell<Vec<DataBlock>>,
    small_chunks: CStyleCell<VecDeque<DataBlock>>,

    working_queue: CStyleCell<VecDeque<usize>>,
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
        chunks.extend(compacted_blocks.into_iter());
        Ok(())
    }

    pub fn init_working_queue(&self) {
        let working_queue = self.working_queue.as_mut();
        working_queue.extend(0..self.chunks.len());
    }
}

struct ReserverHashTableEntities {
    state: Arc<MemoryHashJoinState>,
}

impl ReserverHashTableEntities {
    pub fn create(state: Arc<MemoryHashJoinState>) -> TryCompleteStream<Progress> {
        Box::new(ReserverHashTableEntities { state })
    }
}

impl ITryCompleteStream<Progress> for ReserverHashTableEntities {
    fn next_try_complete(&mut self) -> Result<Option<TryCompleteFuture<Progress>>> {
        todo!()
    }
}

struct BuildHashTableEntities {
    state: Arc<MemoryHashJoinState>,
}

impl BuildHashTableEntities {
    pub fn create(state: Arc<MemoryHashJoinState>) -> TryCompleteStream<Progress> {
        Box::new(BuildHashTableEntities { state })
    }
}
