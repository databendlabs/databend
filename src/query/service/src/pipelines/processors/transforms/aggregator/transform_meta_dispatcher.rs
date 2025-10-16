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
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::EventCause;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use log::info;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::SharedRestoreState;

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

pub struct TransformMetaDispatcher {
    initialized: bool,
    input: PortWithStatus<InputPort>,
    outputs: Vec<PortWithStatus<OutputPort>>,
    finished_outputs: usize,
    waiting_outputs: VecDeque<usize>,
    queue: VecDeque<AggregateMeta>,
    shared_state: Arc<SharedRestoreState>,
}

impl TransformMetaDispatcher {
    pub fn create(
        output_num: usize,
        shared_state: Arc<SharedRestoreState>,
    ) -> Result<ProcessorPtr> {
        let input = PortWithStatus {
            status: PortStatus::Idle,
            port: InputPort::create(),
        };
        let mut outputs_port = Vec::with_capacity(output_num);
        for _index in 0..output_num {
            outputs_port.push(PortWithStatus {
                status: PortStatus::Idle,
                port: OutputPort::create(),
            });
        }
        Ok(ProcessorPtr::create(Box::new(TransformMetaDispatcher {
            input,
            outputs,
            queue: VecDeque::new(),
            shared_state,
            initialized: false,
            finished_outputs: 0,
            waiting_outputs: VecDeque::new(),
        })))
    }
}

impl Processor for TransformMetaDispatcher {
    fn name(&self) -> String {
        String::from("TransformMetaDispatcher")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

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
            self.input.port.set_need_data()
        }

        if self.finished_outputs == self.outputs.len() {
            self.input.port.finish();
            return Ok(Event::Finished);
        }

        let mut input_has_data = false;

        if let EventCause::Input(_input_index) = &cause {
            let input = &mut self.input;

            if input.port.is_finished() {
                for output in &self.outputs {
                    output.port.finish();
                }
                return Ok(Event::Finished);
            } else if input.port.has_data() {
                if input.status != PortStatus::NeedData {
                    input_has_data = true;
                    input.status = PortStatus::NeedData;
                }
            }
        }

        while input_has_data && !self.waiting_outputs.is_empty() {
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

            let data = self.input.port.pull_data().ok_or_else(|| {
                databend_common_exception::ErrorCode::Internal(
                    "Failed to pull data from input port",
                )
            })?;

            self.outputs[output_index].port.push_data(data);
            self.input.status = PortStatus::Idle;
            self.outputs[output_index].status = PortStatus::Idle;

            if self.input.port.is_finished() {
                for output in &self.outputs {
                    output.port.finish();
                }
                return Ok(Event::Finished);
            }

            self.input.port.set_need_data();
        }

        if self.finished_outputs == self.outputs.len() {
            self.input.port.finish();
            return Ok(Event::Finished);
        }

        match self.waiting_outputs.is_empty() {
            true => Ok(Event::NeedConsume),
            false => Ok(Event::NeedData),
        }
    }
}
