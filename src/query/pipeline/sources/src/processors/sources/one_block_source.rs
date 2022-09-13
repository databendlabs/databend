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

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_exception::Result;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::{Event, ProcessorPtr};
use parking_lot::Mutex;
use common_pipeline_core::processors::Processor;

use crate::processors::sources::SyncSource;
use crate::processors::sources::SyncSourcer;

pub struct OneBlockSource {
    output: Arc<OutputPort>,
    data_block: Option<DataBlock>,
}

impl OneBlockSource {
    pub fn create(output: Arc<OutputPort>, data_block: DataBlock) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(OneBlockSource {
            output,
            data_block: Some(data_block),
        })))
    }
}

#[async_trait::async_trait]
impl Processor for OneBlockSource {
    fn name(&self) -> &'static str {
        "BlockSource"
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.data_block.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        self.output.finish();
        Ok(Event::Finished)
    }
}
