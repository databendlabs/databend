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
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::pipelines::processors::transforms::new_hash_join::compact_blocks;
use crate::pipelines::processors::transforms::new_hash_join::memory_hash_table::GlobalMemoryHashTable;
use crate::pipelines::processors::transforms::new_hash_join::memory_hash_table::JoinHashTable;
use crate::pipelines::processors::transforms::new_hash_join::ITryCompleteFuture;
use crate::pipelines::processors::transforms::new_hash_join::ITryCompleteStream;
use crate::pipelines::processors::transforms::new_hash_join::Join;
use crate::pipelines::processors::transforms::new_hash_join::JoinSettings;
use crate::pipelines::processors::transforms::new_hash_join::NoneTryCompleteStream;
use crate::pipelines::processors::transforms::new_hash_join::ProbeData;
use crate::pipelines::processors::transforms::new_hash_join::Progress;
use crate::pipelines::processors::transforms::new_hash_join::TryCompleteFuture;
use crate::pipelines::processors::transforms::new_hash_join::TryCompleteStream;
use crate::pipelines::processors::HashJoinDesc;

pub struct MemoryHashJoin<Table: JoinHashTable> {
    settings: JoinSettings,

    table: Arc<GlobalMemoryHashTable<Table>>,
    partial_blocks: Mutex<VecDeque<DataBlock>>,
    build_blocks: Arc<Mutex<VecDeque<DataBlock>>>,
}

impl<Table: JoinHashTable> Join for MemoryHashJoin<Table> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn add_block(&self, block: DataBlock) -> Result<TryCompleteStream<Progress>> {
        if block.num_rows() >= self.settings.max_block_rows
            || block.memory_size() >= self.settings.max_block_bytes
        {
            let locked_res = self.build_blocks.lock();
            let mut build_blocks = locked_res.unwrap_or_else(PoisonError::into_inner);
            build_blocks.push_back(block);
            return Ok(NoneTryCompleteStream::create());
        }

        // compact for small blocks
        let lock_res = self.partial_blocks.lock();
        let partial_blocks = lock_res.unwrap_or_else(PoisonError::into_inner);
        partial_blocks.push_back(block);

        Ok(NoneTryCompleteStream::create())
    }

    fn finish_build(&self) -> Result<TryCompleteStream<Progress>> {
        // The compact process may still OOM, but the probability is very low.
        let lock_res = self.partial_blocks.lock();
        let mut lock_guard = lock_res.unwrap_or_else(PoisonError::into_inner);

        if !lock_guard.is_empty() {
            // Get partial blocks for compacting
            let partial_blocks = std::mem::take(lock_guard.deref_mut());
            let compacted_blocks = compact_blocks(
                partial_blocks,
                self.settings.max_block_rows,
                self.settings.max_block_bytes,
            )?;

            let build_lock_res = self.build_blocks.lock();
            let mut build_blocks_guard = build_lock_res.unwrap_or_else(PoisonError::into_inner);
            build_blocks_guard.extend(compacted_blocks);
        }

        assert!(lock_guard.is_empty());

        Ok(NoneTryCompleteStream::create())
    }

    fn probe(&self, block: DataBlock) -> Result<TryCompleteStream<ProbeData>> {
        todo!()
    }

    fn finished_probe(&self) -> Result<TryCompleteStream<ProbeData>> {
        todo!()
    }
}

struct BuildTryCompleteStream {
    build_blocks: Arc<Mutex<VecDeque<DataBlock>>>,
}

impl BuildTryCompleteStream {
    pub fn create() -> TryCompleteStream<Progress> {
        Box::new(BuildTryCompleteStream {})
    }
}

impl ITryCompleteStream<Progress> for BuildTryCompleteStream {
    fn next_try_complete(&self) -> Result<Option<TryCompleteFuture<Progress>>> {
        let lock_res = self.build_blocks.lock();
        let build_blocks = lock_res.unwrap_or_else(PoisonError::into_inner);

        match build_blocks.pop_front() {
            None => Ok(None),
            Some(data_block) => Ok(Some(BuildTryCompleteFuture::create(data_block))),
        }
    }
}

struct BuildTryCompleteFuture {
    data_block: DataBlock,
    hash_join_desc: HashJoinDesc,
}

impl BuildTryCompleteFuture {
    pub fn create(block: DataBlock) -> TryCompleteFuture<Progress> {
        Box::pin(BuildTryCompleteFuture {})
    }
}

impl ITryCompleteFuture<Progress> for BuildTryCompleteFuture {
    fn try_complete(&self) -> Result<Option<Progress>> {
        // TODO:
        let evaluator = Evaluator::new(&self.data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);

        let build_keys = &self.hash_join_desc.build_keys;
        let mut keys_entries: Vec<BlockEntry> = build_keys
            .iter()
            .map(|expr| {
                Ok(evaluator
                    .run(expr)?
                    .convert_to_full_column(expr.data_type(), self.data_block.num_rows())
                    .into())
            })
            .collect::<Result<_>>()?;

        let column_nums = self.data_block.num_columns();
        let mut block_entries = Vec::with_capacity(self.build_projections.len());

        todo!()
    }
}
