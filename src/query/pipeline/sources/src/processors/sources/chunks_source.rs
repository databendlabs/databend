// Copyright 2022 Datafuse Labs.
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

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::Chunk;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use parking_lot::Mutex;

use crate::processors::sources::SyncSource;
use crate::processors::sources::SyncSourcer;

pub struct ChunksSource {
    chunks: Arc<Mutex<VecDeque<Chunk<String>>>>,
}

impl ChunksSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        chunks: Arc<Mutex<VecDeque<Chunk<String>>>>,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx, output, ChunksSource { chunks })
    }
}

impl SyncSource for ChunksSource {
    const NAME: &'static str = "ChunksSource";

    fn generate(&mut self) -> Result<Option<Chunk>> {
        let mut chunks_guard = self.chunks.lock();
        match chunks_guard.pop_front() {
            None => Ok(None),
            Some(chunk) => {
                todo!("expression");
                // Ok(Some(chunk))
            }
        }
    }
}
