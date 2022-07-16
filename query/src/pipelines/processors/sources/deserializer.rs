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

use common_datablocks::DataBlock;
use common_exception::Result;
use common_formats::InputFormat;

use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;

pub struct Deserializer {
    input_format: Arc<dyn InputFormat>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    input_data: Option<Vec<u8>>,
    output_data: VecDeque<DataBlock>,
}

impl Deserializer {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        input_format: Arc<dyn InputFormat>,
    ) -> ProcessorPtr {
        ProcessorPtr::create(Box::new(Deserializer {
            input_format,
            input,
            output,
            input_data: None,
            output_data: Default::default(),
        }))
    }
}

#[async_trait::async_trait]
impl Processor for Deserializer {
    fn name(&self) -> &'static str {
        "Deserializer"
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        match self.output.is_finished() {
            true => self.finish_input(),
            false if !self.output.can_push() => self.not_need_data(),
            false => match self.output_data.pop_front() {
                None if self.input_data.is_some() => Ok(Event::Sync),
                None => self.pull_data(),
                Some(data_block) => {
                    self.output.push_data(Ok(data_block));
                    Ok(Event::NeedConsume)
                }
            },
        }
    }

    fn process(&mut self) -> Result<()> {
        if let Some(buf) = self.input_data.take() {
            let mut state = self.input_format.create_state();
            self.input_format.set_buf(buf, &mut state);
            let blocks = self.input_format.deserialize_data(&mut state)?;
            self.output_data = blocks.into();
        }

        Ok(())
    }
}

impl Deserializer {
    fn pull_data(&mut self) -> Result<Event> {
        if self.input.has_data() {
            self.input_data = Some(self.input.pull_buffer().unwrap()?);
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn not_need_data(&mut self) -> Result<Event> {
        self.input.set_not_need_data();
        Ok(Event::NeedConsume)
    }

    fn finish_input(&mut self) -> Result<Event> {
        self.input.finish();
        Ok(Event::Finished)
    }
}
