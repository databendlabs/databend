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

use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;

pub trait ExchangeSorting: Send + Sync + 'static {
    fn block_number(&self, data_block: &DataBlock) -> Result<isize>;
}

// N input one output
pub struct TransformExchangeSorting {
    inputs: Vec<Arc<InputPort>>,
    output: Arc<OutputPort>,
    sorting: Arc<dyn ExchangeSorting>,

    buffer_len: usize,
    buffer: Vec<Option<(isize, DataBlock)>>,
}

impl TransformExchangeSorting {
    pub fn create(inputs: usize, sorting: Arc<dyn ExchangeSorting>) -> TransformExchangeSorting {
        let output = OutputPort::create();
        let mut buffer = Vec::with_capacity(inputs);
        let mut inputs_port = Vec::with_capacity(inputs);

        for _ in 0..inputs {
            buffer.push(None);
            inputs_port.push(InputPort::create());
        }

        TransformExchangeSorting {
            output,
            sorting,
            buffer,
            buffer_len: 0,
            inputs: inputs_port,
        }
    }

    pub fn get_output(&self) -> Arc<OutputPort> {
        self.output.clone()
    }
    pub fn get_inputs(&self) -> Vec<Arc<InputPort>> {
        self.inputs.clone()
    }
}

#[async_trait::async_trait]
impl Processor for TransformExchangeSorting {
    fn name(&self) -> String {
        String::from("TransformExchangeSorting")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            for input in &self.inputs {
                input.finish();
            }

            return Ok(Event::Finished);
        }

        let mut unready_inputs = false;
        let mut all_inputs_finished = true;
        for (index, input) in self.inputs.iter().enumerate() {
            if input.is_finished() {
                continue;
            }

            all_inputs_finished = false;
            if self.buffer[index].is_none() {
                if input.has_data() {
                    let data_block = input.pull_data().unwrap()?;
                    let block_number = self.sorting.block_number(&data_block)?;
                    self.buffer[index] = Some((block_number, data_block));
                    self.buffer_len += 1;
                    input.set_need_data();
                    continue;
                }

                unready_inputs = true;
            }

            input.set_need_data();
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if all_inputs_finished && self.buffer_len == 0 {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if !unready_inputs {
            let mut min_index = 0;
            let mut min_value = isize::MAX;
            for (index, buffer) in self.buffer.iter().enumerate() {
                if let Some((block_number, _)) = buffer {
                    if *block_number < min_value {
                        min_index = index;
                        min_value = *block_number;
                    }
                }
            }

            if let Some((_, block)) = self.buffer[min_index].take() {
                self.buffer_len -= 1;
                self.output.push_data(Ok(block));
                return Ok(Event::NeedConsume);
            }
        }

        Ok(Event::NeedData)
    }
}
