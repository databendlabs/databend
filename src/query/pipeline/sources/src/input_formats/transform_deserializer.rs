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
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use log::trace;

use crate::input_formats::input_pipeline::BlockBuilderTrait;
use crate::input_formats::input_pipeline::InputFormatPipe;
use crate::input_formats::InputContext;

struct DeserializeProcessor<I: InputFormatPipe> {
    pub block_builder: I::BlockBuilder,
    pub input_buffer: Option<I::RowBatch>,
    pub output_buffer: VecDeque<DataBlock>,
}

impl<I: InputFormatPipe> DeserializeProcessor<I> {
    pub(crate) fn create(ctx: Arc<InputContext>) -> Result<Self> {
        Ok(Self {
            block_builder: I::try_create_block_builder(&ctx)?,
            input_buffer: Default::default(),
            output_buffer: Default::default(),
        })
    }

    fn process(&mut self) -> Result<()> {
        let blocks = self.block_builder.deserialize(self.input_buffer.take())?;
        for b in blocks.into_iter() {
            if !b.is_empty() {
                self.output_buffer.push_back(b)
            }
        }
        Ok(())
    }
}

pub struct DeserializeTransformer<I: InputFormatPipe> {
    processor: DeserializeProcessor<I>,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    flushing: bool,
}

impl<I: InputFormatPipe> DeserializeTransformer<I> {
    pub(crate) fn create(
        ctx: Arc<InputContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        let processor = DeserializeProcessor::create(ctx)?;
        Ok(ProcessorPtr::create(Box::new(Self {
            processor,
            input,
            output,
            flushing: false,
        })))
    }
}

#[async_trait::async_trait]
impl<I: InputFormatPipe> Processor for DeserializeTransformer<I> {
    fn name(&self) -> String {
        "DeserializeTransformer".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            Ok(Event::Finished)
        } else if !self.output.can_push() {
            self.input.set_not_need_data();
            Ok(Event::NeedConsume)
        } else {
            match self.processor.output_buffer.pop_front() {
                Some(data_block) => {
                    trace!("DeserializeTransformer push rows {}", data_block.num_rows());
                    self.output.push_data(Ok(data_block));
                    Ok(Event::NeedConsume)
                }
                None => {
                    if self.processor.input_buffer.is_some() {
                        Ok(Event::Sync)
                    } else if self.input.has_data() {
                        let block = self.input.pull_data().unwrap()?;
                        let block_meta = block.get_owned_meta().unwrap();
                        self.processor.input_buffer = I::RowBatch::downcast_from(block_meta);
                        Ok(Event::Sync)
                    } else if self.input.is_finished() {
                        if self.flushing {
                            self.output.finish();
                            Ok(Event::Finished)
                        } else {
                            self.flushing = true;
                            Ok(Event::Sync)
                        }
                    } else {
                        self.input.set_need_data();
                        Ok(Event::NeedData)
                    }
                }
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        self.processor.process()
    }
}
