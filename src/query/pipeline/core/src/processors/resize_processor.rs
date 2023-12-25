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

use crate::pipe::PipeItem;
use crate::processors::Event;
use crate::processors::EventCause;
use crate::processors::InputPort;
use crate::processors::OutputPort;
use crate::processors::Processor;
use crate::processors::ProcessorPtr;

#[derive(PartialEq)]
enum PortStatus {
    Idle,
    HasData,
    NeedData,
    Finished,
}

struct PortWithStatus<Port> {
    pub status: PortStatus,
    pub port: Arc<Port>,
}

pub struct ResizeProcessor {
    initialized: bool,

    finished_inputs: usize,
    finished_outputs: usize,

    waiting_inputs: VecDeque<usize>,
    waiting_outputs: VecDeque<usize>,

    inputs: Vec<PortWithStatus<InputPort>>,
    outputs: Vec<PortWithStatus<OutputPort>>,
}

#[async_trait::async_trait]
impl Processor for ResizeProcessor {
    fn name(&self) -> String {
        String::from("Resize")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    #[allow(clippy::collapsible_if)]
    fn event_with_cause(&mut self, cause: EventCause) -> Result<Event> {
        if let EventCause::Output(output_index) = &cause {
            let output = &mut self.outputs[*output_index];

            if output.port.is_finished() {
                if output.status != PortStatus::Finished {
                    self.finished_outputs += 1;
                    output.status = PortStatus::Finished;
                }
            } else if output.port.can_push() {
                if output.status != PortStatus::NeedData {
                    output.status = PortStatus::NeedData;
                    self.waiting_outputs.push_back(*output_index);
                }
            }
        }

        if !self.initialized && !self.waiting_outputs.is_empty() {
            self.initialized = true;
            for input in &self.inputs {
                input.port.set_need_data();
            }
        }

        if self.finished_outputs == self.outputs.len() {
            for input in &self.inputs {
                input.port.finish();
            }

            return Ok(Event::Finished);
        }

        if let EventCause::Input(input_index) = &cause {
            let input = &mut self.inputs[*input_index];

            if input.port.is_finished() {
                if input.status != PortStatus::Finished {
                    self.finished_inputs += 1;
                    input.status = PortStatus::Finished;
                }
            } else if input.port.has_data() {
                if input.status != PortStatus::HasData {
                    input.status = PortStatus::HasData;
                    self.waiting_inputs.push_back(*input_index);
                }
            }
        }

        while !self.waiting_outputs.is_empty() && !self.waiting_inputs.is_empty() {
            let output_index = self.waiting_outputs.pop_front().unwrap();

            // Port is finished when waiting.
            if self.outputs[output_index].port.is_finished() {
                if self.outputs[output_index].status != PortStatus::Finished {
                    self.finished_outputs += 1;
                    self.outputs[output_index].status = PortStatus::Finished;
                }

                continue;
            }

            let input_index = self.waiting_inputs.pop_front().unwrap();

            self.outputs[output_index]
                .port
                .push_data(self.inputs[input_index].port.pull_data().unwrap());
            self.inputs[input_index].status = PortStatus::Idle;
            self.outputs[output_index].status = PortStatus::Idle;

            if self.inputs[input_index].port.is_finished() {
                if self.inputs[input_index].status != PortStatus::Finished {
                    self.finished_inputs += 1;
                    self.inputs[input_index].status = PortStatus::Finished;
                }

                continue;
            }

            self.inputs[input_index].port.set_need_data();
        }

        if self.finished_outputs == self.outputs.len() {
            for input in &self.inputs {
                input.port.finish();
            }

            return Ok(Event::Finished);
        }

        if self.finished_inputs == self.inputs.len() {
            for output in &self.outputs {
                output.port.finish();
            }

            return Ok(Event::Finished);
        }

        match self.waiting_outputs.is_empty() {
            true => Ok(Event::NeedConsume),
            false => Ok(Event::NeedData),
        }
    }
}

impl ResizeProcessor {
    pub fn create(inputs: usize, outputs: usize) -> Self {
        let mut inputs_port = Vec::with_capacity(inputs);
        let mut outputs_port = Vec::with_capacity(outputs);

        for _index in 0..inputs {
            inputs_port.push(PortWithStatus {
                status: PortStatus::Idle,
                port: InputPort::create(),
            });
        }

        for _index in 0..outputs {
            outputs_port.push(PortWithStatus {
                status: PortStatus::Idle,
                port: OutputPort::create(),
            });
        }

        ResizeProcessor {
            initialized: false,
            finished_inputs: 0,
            finished_outputs: 0,
            inputs: inputs_port,
            outputs: outputs_port,
            waiting_inputs: VecDeque::with_capacity(inputs),
            waiting_outputs: VecDeque::with_capacity(outputs),
        }
    }

    pub fn get_inputs(&self) -> Vec<Arc<InputPort>> {
        self.inputs.iter().map(|x| x.port.clone()).collect()
    }

    pub fn get_outputs(&self) -> Vec<Arc<OutputPort>> {
        self.outputs.iter().map(|x| x.port.clone()).collect()
    }
}

pub fn create_resize_item(inputs: usize, outputs: usize) -> PipeItem {
    let resize = ResizeProcessor::create(inputs, outputs);
    let inputs = resize.get_inputs();
    let outputs = resize.get_outputs();
    PipeItem::create(ProcessorPtr::create(Box::new(resize)), inputs, outputs)
}
