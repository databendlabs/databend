//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_exception::Result;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use crossbeam_channel::TryRecvError;

use crate::processors::sources::input_formats::input_context::InputContext;
use crate::processors::sources::input_formats::input_pipeline::BlockBuilderTrait;
use crate::processors::sources::input_formats::input_pipeline::InputFormatPipe;

struct DeserializeProcessor<I: InputFormatPipe> {
    pub block_builder: I::BlockBuilder,
    pub input_buffer: Option<I::RowBatch>,
    pub output_buffer: VecDeque<DataBlock>,
}

impl<I: InputFormatPipe> DeserializeProcessor<I> {
    pub(crate) fn create(ctx: Arc<InputContext>) -> Result<Self> {
        Ok(Self {
            block_builder: I::BlockBuilder::create(ctx),
            input_buffer: Default::default(),
            output_buffer: Default::default(),
        })
    }

    fn process(&mut self) -> Result<()> {
        let blocks = self.block_builder.deserialize(self.input_buffer.take())?;
        for b in blocks.into_iter() {
            self.output_buffer.push_back(b)
        }
        Ok(())
    }
}

pub struct DeserializeTransformer<I: InputFormatPipe> {
    processor: DeserializeProcessor<I>,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    rx: crossbeam_channel::Receiver<I::RowBatch>,
    flushing: bool,
}

impl<I: InputFormatPipe> DeserializeTransformer<I> {
    pub(crate) fn create(
        ctx: Arc<InputContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        rx: crossbeam_channel::Receiver<I::RowBatch>,
    ) -> Result<ProcessorPtr> {
        let processor = DeserializeProcessor::create(ctx)?;
        Ok(ProcessorPtr::create(Box::new(Self {
            processor,
            input,
            output,
            rx,
            flushing: false,
        })))
    }
}

#[async_trait::async_trait]
impl<I: InputFormatPipe> Processor for DeserializeTransformer<I> {
    fn name(&self) -> &'static str {
        "DeserializeTransformer"
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
                    tracing::info!("DeserializeTransformer push rows {}", data_block.num_rows());
                    self.output.push_data(Ok(data_block));
                    Ok(Event::NeedConsume)
                }
                None => {
                    if self.processor.input_buffer.is_some() {
                        Ok(Event::Sync)
                    } else {
                        if self.input.has_data() {
                            self.input.pull_data();
                            match self.rx.try_recv() {
                                Ok(read_batch) => {
                                    self.processor.input_buffer = Some(read_batch);
                                    return Ok(Event::Sync);
                                }
                                Err(TryRecvError::Disconnected) => {
                                    tracing::warn!("DeserializeTransformer rx disconnected");
                                    self.input.finish();
                                    self.flushing = true;
                                    return Ok(Event::Finished);
                                }
                                Err(TryRecvError::Empty) => {
                                    // do nothing
                                }
                            }
                        }
                        //  !has_data() or try_recv return Empty
                        if self.input.is_finished() {
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
    }

    fn process(&mut self) -> Result<()> {
        self.processor.process()
    }
}
