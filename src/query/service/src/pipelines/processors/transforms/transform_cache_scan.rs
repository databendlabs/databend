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
use crate::sql::plans::CacheSource;

pub struct TransformCacheScan {
    cache_source: CacheSource,
    state: Arc<HashJoinState>,
    initilized: bool,
    output_pos: usize,
    cache_size: usize,
    columns: Vec<Vec<(BlockEntry, usize)>>,
    output_buffer: VecDeque<DataBlock>,
}

impl TransformCacheScan {
    pub fn create(
        ctx: Arc<QueryContext>,
        output_port: Arc<OutputPort>,
        cache_source: CacheSource,
        state: Arc<HashJoinState>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output_port, TransformCacheScan {
            cache_source,
            state,
            initilized: false,
            output_pos: 0,
            cache_size: 0,
            columns: Vec::new(),
            output_buffer: VecDeque::new(),
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for TransformCacheScan {
    const NAME: &'static str = "TransformCacheScan";

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if !self.initilized {
            match &self.cache_source {
                CacheSource::HashJoinBuild((_, column_indexes)) => {
                    for column_index in column_indexes {
                        let column = self.state.read_build_cache(*column_index);
                        self.columns.push(column);
                    }
                    self.output_pos = 0;
                    self.cache_size = self.state.num_build_chunks();
                }
            }
            self.initilized = true;
        }

        if self.output_pos >= self.cache_size {
            return Ok(None);
        }

        if !self.output_buffer.is_empty() {
            return Ok(self.output_buffer.pop_front());
        }

        let block_entries = (0..self.columns.len())
            .map(|idx| self.columns[idx][self.output_pos].clone().0)
            .collect::<Vec<BlockEntry>>();
        let num_rows = self.columns[0][self.output_pos].1;
        self.output_pos += 1;
        let data_block = DataBlock::new(block_entries, num_rows);
        Ok(Some(data_block))
    }
}
