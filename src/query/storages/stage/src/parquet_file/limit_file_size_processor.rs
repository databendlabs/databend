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
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;

use super::block_batch::BlockBatch;

pub(super) struct LimitFileSizeProcessor {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    threshold: usize,

    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,

    // since we only output one BlockBatch each time, the remaining blocks is kept here.
    // remember to flush it when input is finished
    blocks: Vec<DataBlock>,
}

impl LimitFileSizeProcessor {
    pub(super) fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        threshold: usize,
    ) -> Result<ProcessorPtr> {
        let p = Self {
            input,
            output,
            threshold,
            input_data: None,
            output_data: None,
            blocks: Vec::new(),
        };
        Ok(ProcessorPtr::create(Box::new(p)))
    }
}

impl Processor for LimitFileSizeProcessor {
    fn name(&self) -> String {
        String::from("ResizeProcessor")
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
            match self.output_data.take() {
                Some(data) => {
                    self.output.push_data(Ok(data));
                    Ok(Event::NeedConsume)
                }
                None => {
                    if self.input_data.is_some() {
                        Ok(Event::Sync)
                    } else if self.input.has_data() {
                        self.input_data = Some(self.input.pull_data().unwrap()?);
                        Ok(Event::Sync)
                    } else if self.input.is_finished() {
                        if self.blocks.is_empty() {
                            self.output.finish();
                            Ok(Event::Finished)
                        } else {
                            // flush the remaining blocks
                            let blocks = std::mem::take(&mut self.blocks);
                            self.output.push_data(Ok(BlockBatch::create_block(blocks)));
                            Ok(Event::NeedConsume)
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
        assert!(self.input_data.is_some());
        assert!(self.output_data.is_none());
        // slicing has overhead, we do not do it for now.
        let block = self.input_data.take().unwrap();
        let mut blocks = std::mem::take(&mut self.blocks);

        blocks.push(block);
        let mut break_point = blocks.len();
        let mut size = 0;
        for (i, b) in blocks.iter().enumerate() {
            size += b.memory_size();
            if size > self.threshold {
                break_point = i;
                break;
            }
        }
        if break_point == blocks.len() {
            self.blocks = blocks;
        } else {
            let remain = blocks.split_off(break_point + 1);
            self.output_data = Some(BlockBatch::create_block(blocks));
            self.blocks = remain;
        }
        Ok(())
    }
}
