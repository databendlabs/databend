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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::core::port::InputPort;
use crate::core::port::OutputPort;
use crate::core::processor::Event;
use crate::core::processor::EventCause;
use crate::core::processor::Processor;

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

pub struct SequenceGroupProcessor {
    // groups:Vec<ResizeProcessor>,
    initialized: bool,

    finished_inputs: usize,
    finished_outputs: usize,

    ignore_output: bool,
    waiting_inputs: VecDeque<usize>,
    waiting_outputs: VecDeque<usize>,

    inputs: Vec<PortWithStatus<InputPort>>,
    outputs: Vec<PortWithStatus<OutputPort>>,

    input_offset: usize,
    input_groups_port: VecDeque<(bool, Vec<PortWithStatus<InputPort>>)>,
}

#[async_trait::async_trait]
impl Processor for SequenceGroupProcessor {
    fn name(&self) -> String {
        String::from("SequenceGroup")
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

            for (_, inputs) in &self.input_groups_port {
                for input in inputs {
                    input.port.finish();
                }
            }

            return Ok(Event::Finished);
        }

        if let EventCause::Input(input_index) = &cause {
            let input_index = input_index - self.input_offset;
            let input = &mut self.inputs[input_index];

            if input.port.is_finished() {
                if input.status != PortStatus::Finished {
                    self.finished_inputs += 1;
                    input.status = PortStatus::Finished;
                }
            } else if input.port.has_data() {
                if self.ignore_output {
                    let _ignore = input.port.pull_data().ok_or_else(|| {
                        databend_common_exception::ErrorCode::Internal(
                            "Failed to pull data from input port when ignoring output",
                        )
                    })??;
                    input.status = PortStatus::Idle;

                    if input.port.is_finished() {
                        if input.status != PortStatus::Finished {
                            self.finished_inputs += 1;
                            input.status = PortStatus::Finished;
                        }
                    } else {
                        input.port.set_need_data();
                    }
                } else if input.status != PortStatus::HasData {
                    input.status = PortStatus::HasData;
                    self.waiting_inputs.push_back(input_index);
                }
            }
        }

        while !self.waiting_outputs.is_empty() && !self.waiting_inputs.is_empty() {
            let output_index = self.waiting_outputs.pop_front().ok_or_else(|| {
                databend_common_exception::ErrorCode::Internal(
                    "Waiting outputs queue should not be empty",
                )
            })?;

            // Port is finished when waiting.
            if self.outputs[output_index].port.is_finished() {
                if self.outputs[output_index].status != PortStatus::Finished {
                    self.finished_outputs += 1;
                    self.outputs[output_index].status = PortStatus::Finished;
                }

                continue;
            }

            let input_index = self.waiting_inputs.pop_front().ok_or_else(|| {
                databend_common_exception::ErrorCode::Internal(
                    "Waiting inputs queue should not be empty",
                )
            })?;

            let data = self.inputs[input_index].port.pull_data().ok_or_else(|| {
                databend_common_exception::ErrorCode::Internal(
                    "Failed to pull data from input port",
                )
            })?;
            self.outputs[output_index].port.push_data(data);
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

            for (_, inputs) in &self.input_groups_port {
                for input in inputs {
                    input.port.finish();
                }
            }

            return Ok(Event::Finished);
        }

        if self.finished_inputs == self.inputs.len() {
            let Some((ignore_output, group_inputs_port)) = self.input_groups_port.pop_front()
            else {
                for output in &self.outputs {
                    output.port.finish();
                }

                return Ok(Event::Finished);
            };

            self.input_offset += self.inputs.len();

            self.finished_inputs = 0;
            self.waiting_inputs.clear();
            self.inputs = group_inputs_port;
            self.ignore_output = ignore_output;

            for input in &self.inputs {
                input.port.set_need_data();
            }

            return Ok(Event::NeedData);
        }

        match self.waiting_outputs.is_empty() {
            true => Ok(Event::NeedConsume),
            false => Ok(Event::NeedData),
        }
    }
}

impl SequenceGroupProcessor {
    pub fn create(input_groups: Vec<(usize, bool)>, outputs: usize) -> Result<Self> {
        let mut input_groups_port = VecDeque::with_capacity(input_groups.len());
        let mut outputs_port = Vec::with_capacity(outputs);

        for (inputs, ignore_result) in input_groups {
            let mut group_inputs_port = Vec::with_capacity(inputs);
            for _index in 0..inputs {
                group_inputs_port.push(PortWithStatus {
                    status: PortStatus::Idle,
                    port: InputPort::create(),
                });
            }

            input_groups_port.push_back((ignore_result, group_inputs_port));
        }

        for _index in 0..outputs {
            outputs_port.push(PortWithStatus {
                status: PortStatus::Idle,
                port: OutputPort::create(),
            });
        }

        let Some((ignore_output, inputs_port)) = input_groups_port.pop_front() else {
            return Err(ErrorCode::Internal(""));
        };

        let inputs = inputs_port.len();

        Ok(SequenceGroupProcessor {
            ignore_output,
            input_groups_port,
            initialized: false,
            input_offset: 0,
            finished_inputs: 0,
            finished_outputs: 0,
            inputs: inputs_port,
            outputs: outputs_port,
            waiting_inputs: VecDeque::with_capacity(inputs),
            waiting_outputs: VecDeque::with_capacity(outputs),
        })
    }

    pub fn get_inputs(&self) -> Vec<Arc<InputPort>> {
        let mut inputs = vec![];

        for input in &self.inputs {
            inputs.push(input.port.clone());
        }

        for (_, groups) in &self.input_groups_port {
            for group in groups {
                inputs.push(group.port.clone());
            }
        }

        inputs
    }

    pub fn get_outputs(&self) -> Vec<Arc<OutputPort>> {
        self.outputs.iter().map(|x| x.port.clone()).collect()
    }
}
