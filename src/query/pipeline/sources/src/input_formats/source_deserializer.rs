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

use common_base::base::ProgressValues;
use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;

use crate::input_formats::input_pipeline::BlockBuilderTrait;
use crate::input_formats::input_pipeline::InputFormatPipe;
use crate::input_formats::input_pipeline::RowBatchTrait;
use crate::input_formats::InputContext;

pub struct DeserializeSource<I: InputFormatPipe> {
    #[allow(unused)]
    ctx: Arc<InputContext>,
    output: Arc<OutputPort>,

    block_builder: I::BlockBuilder,
    input_rx: async_channel::Receiver<Result<I::RowBatch>>,
    input_buffer: Option<I::RowBatch>,
    input_finished: bool,
    output_buffer: VecDeque<DataBlock>,
}

impl<I: InputFormatPipe> DeserializeSource<I> {
    #[allow(unused)]
    pub(crate) fn create(
        ctx: Arc<InputContext>,
        output: Arc<OutputPort>,
        rx: async_channel::Receiver<Result<I::RowBatch>>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(Self {
            ctx: ctx.clone(),
            block_builder: I::try_create_block_builder(&ctx)?,
            output,
            input_rx: rx,
            input_buffer: Default::default(),
            input_finished: false,
            output_buffer: Default::default(),
        })))
    }
}

#[async_trait::async_trait]
impl<I: InputFormatPipe> Processor for DeserializeSource<I> {
    fn name(&self) -> String {
        "Deserializer".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input_buffer = None;
            self.input_finished = true;
            Ok(Event::Finished)
        } else if !self.output.can_push() {
            Ok(Event::NeedConsume)
        } else {
            match self.output_buffer.pop_front() {
                Some(data_block) => {
                    tracing::info!("DeserializeSource push rows {}", data_block.num_rows());
                    self.output.push_data(Ok(data_block));
                    Ok(Event::NeedConsume)
                }
                None => {
                    if self.input_buffer.is_some() {
                        Ok(Event::Sync)
                    } else if self.input_finished {
                        self.output.finish();
                        Ok(Event::Finished)
                    } else {
                        Ok(Event::Async)
                    }
                }
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        if self.input_finished {
            assert!(self.input_buffer.is_none());
        }
        if let Some(row_batch) = &self.input_buffer {
            let process_values = ProgressValues {
                rows: row_batch.rows(),
                bytes: row_batch.size(),
            };
            self.ctx.scan_progress.incr(&process_values)
        }
        let blocks = self.block_builder.deserialize(self.input_buffer.take())?;
        for b in blocks.into_iter() {
            self.output_buffer.push_back(b)
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        assert!(self.input_buffer.is_none() && !self.input_finished);
        match self.input_rx.recv().await {
            Ok(row_batch) => {
                self.input_buffer = Some(row_batch?);
            }
            Err(_) => {
                self.input_finished = true;
            }
        }
        Ok(())
    }
}
