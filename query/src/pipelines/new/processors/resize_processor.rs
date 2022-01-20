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

use common_exception::Result;

use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::Event;
use crate::pipelines::new::processors::Processor;

pub struct ResizeProcessor {
    inputs: Vec<Arc<InputPort>>,
    outputs: Vec<Arc<OutputPort>>,

    cur_input_index: usize,
    cur_output_index: usize,
}

impl ResizeProcessor {
    pub fn create(inputs: usize, outputs: usize) -> Self {
        let mut inputs_port = Vec::with_capacity(inputs);
        let mut outputs_port = Vec::with_capacity(outputs);

        for _index in 0..inputs {
            inputs_port.push(InputPort::create());
        }

        for _index in 0..outputs {
            outputs_port.push(OutputPort::create());
        }

        ResizeProcessor {
            inputs: inputs_port,
            outputs: outputs_port,
            cur_input_index: 0,
            cur_output_index: 0,
        }
    }

    pub fn get_inputs(&self) -> &[Arc<InputPort>] {
        &self.inputs
    }

    pub fn get_outputs(&self) -> &[Arc<OutputPort>] {
        &self.outputs
    }

    fn get_current_input(&mut self) -> Option<Arc<InputPort>> {
        let mut finished = true;
        let mut index = self.cur_input_index;

        loop {
            let input = &self.inputs[index];

            if !input.is_finished() {
                finished = false;

                if input.has_data() {
                    self.cur_input_index = index;
                    return Some(input.clone());
                }
            }

            index += 1;
            if index == self.inputs.len() {
                index = 0;
            }

            if index == self.cur_input_index {
                self.cur_input_index = index;
                return match finished {
                    true => Some(input.clone()),
                    false => None,
                };
            }
        }
    }

    fn get_current_output(&mut self) -> Option<Arc<OutputPort>> {
        let mut finished = true;
        let mut index = self.cur_output_index;

        loop {
            let output = &self.outputs[index];

            if !output.is_finished() {
                finished = false;

                if output.can_push() {
                    self.cur_output_index = index;
                    return Some(output.clone());
                }
            }

            index += 1;
            if index == self.outputs.len() {
                index = 0;
            }

            if index == self.cur_output_index {
                self.cur_output_index = index;
                return match finished {
                    true => Some(output.clone()),
                    false => None,
                };
            }
        }
    }

    fn finish_inputs(&mut self) {
        for input in &self.inputs {
            input.finish();
        }
    }

    fn inputs_need_data(&mut self) {
        for input in &self.inputs {
            input.set_need_data();
        }
    }

    fn inputs_not_need_data(&mut self) {
        for input in &self.inputs {
            input.set_not_need_data();
        }
    }

    fn finish_outputs(&mut self) {
        for output in &self.outputs {
            output.finish();
        }
    }
}

#[async_trait::async_trait]
impl Processor for ResizeProcessor {
    fn name(&self) -> &'static str {
        "Resize"
    }

    fn event(&mut self) -> Result<Event> {
        if let Some(cur_output) = self.get_current_output() {
            if cur_output.is_finished() {
                self.finish_inputs();
                return Ok(Event::Finished);
            }

            if let Some(cur_input) = self.get_current_input() {
                if cur_input.is_finished() {
                    self.finish_outputs();
                    return Ok(Event::Finished);
                }

                cur_output.push_data(cur_input.pull_data().unwrap());
                return Ok(Event::NeedConsume);
            }

            self.inputs_need_data();
            return Ok(Event::NeedData);
        }

        self.inputs_not_need_data();
        Ok(Event::NeedConsume)
    }
}
