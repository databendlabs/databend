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
use std::sync::Arc;

use common_exception::Result;
use common_expression::Chunk;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;

pub struct OneChunkSource {
    output: Arc<OutputPort>,
    chunk: Option<Chunk>,
}

impl OneChunkSource {
    pub fn create(output: Arc<OutputPort>, chunk: Chunk) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(OneChunkSource {
            output,
            chunk: Some(chunk),
        })))
    }
}

#[async_trait::async_trait]
impl Processor for OneChunkSource {
    fn name(&self) -> String {
        "ChunkSource".to_string()
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

        if let Some(chunk) = self.chunk.take() {
            todo!("expression");
            // self.output.push_data(Ok(chunk));
            return Ok(Event::NeedConsume);
        }

        self.output.finish();
        Ok(Event::Finished)
    }
}
