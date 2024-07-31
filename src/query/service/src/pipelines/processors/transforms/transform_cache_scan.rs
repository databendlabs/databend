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

use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;

use crate::pipelines::processors::HashJoinState;
use crate::sessions::QueryContext;

#[derive(Clone)]
pub enum CacheSourceState {
    HashJoinCacheState(HashJoinCacheState),
}

impl CacheSourceState {
    fn next_data_block(&mut self) -> Option<DataBlock> {
        match self {
            CacheSourceState::HashJoinCacheState(state) => state.next_data_block(),
        }
    }
}

#[derive(Clone)]
pub struct HashJoinCacheState {
    initialized: bool,
    column_indexes: Vec<usize>,
    columns: Vec<Vec<BlockEntry>>,
    num_rows: Vec<usize>,
    num_cache_blocks: usize,
    hash_join_state: Arc<HashJoinState>,
    output_buffer: VecDeque<DataBlock>,
    max_block_size: usize,
}

impl HashJoinCacheState {
    pub fn new(
        column_indexes: Vec<usize>,
        hash_join_state: Arc<HashJoinState>,
        max_block_size: usize,
    ) -> Self {
        Self {
            initialized: false,
            column_indexes,
            columns: Vec::new(),
            num_rows: Vec::new(),
            num_cache_blocks: 0,
            hash_join_state,
            output_buffer: VecDeque::new(),
            max_block_size,
        }
    }
    fn next_data_block(&mut self) -> Option<DataBlock> {
        if !self.initialized {
            self.num_cache_blocks = self.hash_join_state.num_build_chunks();
            for column_index in self.column_indexes.iter() {
                let column = self.hash_join_state.get_cached_columns(*column_index);
                self.columns.push(column);
            }
            self.num_rows = self.hash_join_state.get_cached_num_rows();
            self.initialized = true;
        }

        if let Some(data_block) = self.output_buffer.pop_front() {
            return Some(data_block);
        }

        let next_cache_block_index = self.hash_join_state.next_cache_block_index();
        if next_cache_block_index >= self.num_cache_blocks {
            // Release memory.
            self.columns.clear();
            return None;
        }

        let block_entries = (0..self.columns.len())
            .map(|idx| self.columns[idx][next_cache_block_index].clone())
            .collect::<Vec<BlockEntry>>();
        let num_rows = self.num_rows[next_cache_block_index];
        let data_block = DataBlock::new(block_entries, num_rows);

        for data_block in data_block.split_by_rows_no_tail(self.max_block_size) {
            self.output_buffer.push_back(data_block);
        }

        self.output_buffer.pop_front()
    }
}

pub struct TransformCacheScan {
    cache_source_state: CacheSourceState,
}

impl TransformCacheScan {
    pub fn create(
        ctx: Arc<QueryContext>,
        output_port: Arc<OutputPort>,
        cache_source_state: CacheSourceState,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output_port, TransformCacheScan {
            cache_source_state,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for TransformCacheScan {
    const NAME: &'static str = "TransformCacheScan";

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        let data_block = self.cache_source_state.next_data_block();
        Ok(data_block)
    }
}
