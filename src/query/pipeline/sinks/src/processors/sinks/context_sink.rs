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

use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_exception::Result;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;

use crate::processors::sinks::Sink;
use crate::processors::sinks::Sinker;

pub struct ContextSink {
    ctx: Arc<dyn TableContext>,
}

impl ContextSink {
    pub fn create(input: Arc<InputPort>, ctx: Arc<dyn TableContext>) -> ProcessorPtr {
        Sinker::create(input, ContextSink { ctx })
    }
}

impl Sink for ContextSink {
    const NAME: &'static str = "ContextSink";

    fn consume(&mut self, block: DataBlock) -> Result<()> {
        self.ctx.push_precommit_block(block);
        Ok(())
    }
}
