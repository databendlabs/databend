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
use databend_common_expression::DataBlock;

use super::EventCause;
use crate::processors::Event;
use crate::processors::InputPort;
use crate::processors::OutputPort;
use crate::processors::PortStatus;
use crate::processors::PortWithStatus;
use crate::processors::Processor;

pub trait Router {
    fn route(&self, block: &DataBlock) -> Vec<Option<DataBlock>>;
}

pub(crate) enum PortStatus {
    Idle,
    HasData,
    NeedData,
    Finished,
}

pub(crate) struct PortWithStatus<Port> {
    pub status: PortStatus,
    pub port: Arc<Port>,
}

pub struct RouterProcessor<R: Router> {
    initialized: bool,
    finished_inputs: usize,
    finished_outputs: usize,

    inputs: Vec<PortWithStatus<InputPort>>,
    outputs: Vec<PortWithStatus<OutputPort>>,
    blocks: Vec<VecDeque<DataBlock>>,
    to_process_block: Option<DataBlock>,

    waiting_inputs: VecDeque<usize>,
    waiting_outputs: VecDeque<usize>,
    router: R,
}

impl RouterProcessor {
    pub fn create(inputs: usize, outputs: usize, router: R) -> Self {
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

        RouterProcessor {
            initialized: false,
            finished_inputs: 0,
            finished_outputs: 0,

            inputs: inputs_port,
            outputs: outputs_port,
            blocks: vec![VecDeque::new(), outputs],
            to_process_block: None,
            router,
            waiting_inputs: VecDeque::with_capacity(inputs),
            waiting_outputs: VecDeque::with_capacity(outputs),
        }
    }
}

#[async_trait::async_trait]
impl Processor for RouterProcessor {
    fn name(&self) -> String {
        "Router".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event_with_cause(&mut self, cause: EventCause) -> Result<Event> {
        if let EventCause::Output(output_index) = &cause {
            let output = &mut self.outputs[*output_index];

            if output.is_finished() {
                if output.status != PortStatus::Finished {
                    self.finished_outputs += 1;
                    output.status = PortStatus::Finished;
                }
            } else if output.can_push() {
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

        if let Some(blocks) = &self.blocks {
            for (i, block) in blocks.iter_mut().enumerate() {
                if block.is_some() && self.outputs[i].can_push() {
                    self.outputs[i].push_data(Ok(block.take()));
                    self.inputs[i].set_need_data();
                } else if !self.inputs[i].is_finished() {
                    self.inputs[i].set_need_data();
                }
            }

            let cnt = blocks.iter().filter(|x| x.is_some()).count();
            if cnt == 0 {
                self.blocks = None;
            }
        } else {
            // try to pull data from input and process them

            // 1. if any input has data, pull data from input and process them
            // pick from random input
            for input in &self.inputs {
                if input.has_data() {
                    self.to_process_block = Some(input.pull_data()?);
                    return Ok(Event::Process);
                }
            }
        }

        if self.blocks.is_none() {
            let mut blocks = vec![None; self.outputs.len()];
            for input in &self.inputs {
                if input.is_finished() {
                    for block in blocks.iter_mut() {
                        *block = Some(DataBlock::empty());
                    }
                    break;
                }
                if input.has_data() {
                    let block = input.pull_data()?;
                    for (i, block) in self.router.route(&block).iter().enumerate() {
                        if let Some(block) = block {
                            blocks[i] = Some(block.clone());
                        }
                    }
                }
            }
            self.blocks = Some(blocks);
        }

        let mut finished = true;
        for (input, output) in self.channel.iter() {
            if output.is_finished() || input.is_finished() {
                input.finish();
                output.finish();
                continue;
            }
            finished = false;
            input.set_need_data();
            if output.can_push() && input.has_data() {
                output.push_data(input.pull_data().unwrap());
            }
        }
        if finished {
            return Ok(Event::Finished);
        }
        Ok(Event::NeedData)
    }

    // Synchronous work.
    fn process(&mut self) -> Result<()> {
        Err(ErrorCode::Unimplemented("Unimplemented process."))
    }
}
