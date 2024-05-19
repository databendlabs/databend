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
use crate::sql::plans::CacheSource;

enum CacheSourceState {
    HashJoinCacheState(HashJoinCacheState),
}

impl CacheSourceState {
    fn next_data_block(&mut self) -> Option<DataBlock> {
        match self {
            CacheSourceState::HashJoinCacheState(state) => state.next_data_block(),
        }
    }
}

struct HashJoinCacheState {
    initilized: bool,
    column_indexes: Vec<usize>,
    columns: Vec<Vec<BlockEntry>>,
    num_rows: Vec<usize>,
    num_cache_blocks: usize,
    hash_join_state: Arc<HashJoinState>,
}

impl HashJoinCacheState {
    fn next_data_block(&mut self) -> Option<DataBlock> {
        if !self.initilized {
            self.num_cache_blocks = self.hash_join_state.num_build_chunks();
            for column_index in self.column_indexes.iter() {
                let column = self.hash_join_state.build_cache_columns(*column_index);
                self.columns.push(column);
            }
            self.num_rows = self.hash_join_state.build_cache_num_rows();
            self.initilized = true;
        }

        let next_cache_block_index = self.hash_join_state.next_cache_block_index();
        if next_cache_block_index >= self.num_cache_blocks {
            // Release the memory.
            self.columns.clear();
            return None;
        }

        let block_entries = (0..self.columns.len())
            .map(|idx| self.columns[idx][next_cache_block_index].clone())
            .collect::<Vec<BlockEntry>>();
        let num_rows = self.num_rows[next_cache_block_index];

        Some(DataBlock::new(block_entries, num_rows))
    }
}

pub struct TransformCacheScan {
    cache_source_state: CacheSourceState,
}

impl TransformCacheScan {
    pub fn create(
        ctx: Arc<QueryContext>,
        output_port: Arc<OutputPort>,
        cache_source: CacheSource,
        state: Arc<HashJoinState>,
    ) -> Result<ProcessorPtr> {
        let cache_source_state = match cache_source {
            CacheSource::HashJoinBuild((_, column_indexes)) => {
                CacheSourceState::HashJoinCacheState(HashJoinCacheState {
                    initilized: false,
                    column_indexes,
                    columns: Vec::new(),
                    num_rows: Vec::new(),
                    num_cache_blocks: 0,
                    hash_join_state: state.clone(),
                })
            }
        };
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
