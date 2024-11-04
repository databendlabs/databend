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
use std::mem;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;

use crate::append::row_based_file::buffers::FileOutputBuffer;
use crate::append::row_based_file::buffers::FileOutputBuffers;

pub(super) struct LimitFileSizeProcessor {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    threshold: usize,
    flushing: bool,
    buffered_size: usize,

    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,
    buffers: Vec<FileOutputBuffer>,
}

impl LimitFileSizeProcessor {
    pub(super) fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        max_file_size: usize,
    ) -> Result<ProcessorPtr> {
        let p = Self {
            input,
            output,
            threshold: max_file_size,
            input_data: None,
            output_data: None,
            buffers: Vec::new(),
            flushing: false,
            buffered_size: 0,
        };
        Ok(ProcessorPtr::create(Box::new(p)))
    }
}

impl Processor for LimitFileSizeProcessor {
    fn name(&self) -> String {
        String::from("LimitFileSizeProcessor")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> databend_common_exception::Result<Event> {
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
                    // backwards
                    if self.buffered_size > self.threshold || self.input_data.is_some() {
                        Ok(Event::Sync)
                    } else if self.input.has_data() {
                        self.input_data = Some(self.input.pull_data().unwrap()?);
                        Ok(Event::Sync)
                    } else if self.input.is_finished() {
                        if self.buffers.is_empty() {
                            assert_eq!(self.buffered_size, 0);
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
        assert!(self.output_data.is_none());
        assert!(self.input_data.is_some() || self.flushing || self.buffered_size > self.threshold);

        if self.buffered_size <= self.threshold {
            if let Some(block) = self.input_data.take() {
                let block_meta = block.get_owned_meta().unwrap();
                let buffers = FileOutputBuffers::downcast_from(block_meta).unwrap();
                let buffers = buffers.buffers;
                self.buffered_size += buffers.iter().map(|b| b.buffer.len()).sum::<usize>();
                self.buffers.extend(buffers);
            }
        }

        let mut size = 0;
        for i in 0..self.buffers.len() {
            size += self.buffers[i].buffer.len();
            if size > self.threshold {
                let mut buffers = mem::take(&mut self.buffers);
                self.buffers = buffers.split_off(i + 1);
                self.buffered_size = self.buffers.iter().map(|b| b.buffer.len()).sum::<usize>();
                self.output_data = Some(FileOutputBuffers::create_block(buffers));
                return Ok(());
            }
        }
        if self.flushing {
            assert!(self.input_data.is_none());
            let buffers = mem::take(&mut self.buffers);
            self.output
                .push_data(Ok(FileOutputBuffers::create_block(buffers)));
            self.buffered_size = 0;
        }
        Ok(())
    }
}
