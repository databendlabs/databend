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
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::EventCause;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;

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
    wait_counts: Vec<usize>,
}

impl TransformMetaDispatcher {
    pub fn create(
        output_num: usize,
        shared_state: Arc<SharedRestoreState>,
    ) -> Result<TransformMetaDispatcher> {
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
        Ok(TransformMetaDispatcher {
            input,
            outputs: outputs_port,
            queue: VecDeque::new(),
            shared_state,
            initialized: false,
            finished_outputs: 0,
            waiting_outputs: VecDeque::new(),
            wait_counts: vec![0; output_num],
        })
    }

    pub fn input_port(&self) -> Arc<InputPort> {
        self.input.port.clone()
    }

    pub fn output_ports(&self) -> Vec<Arc<OutputPort>> {
        self.outputs.iter().map(|x| x.port.clone()).collect()
    }

    // Extract Partitioned meta into internal queue
    fn extract_partitioned(&mut self, meta: AggregateMeta) {
        if let AggregateMeta::Partitioned { data, .. } = meta {
            for item in data {
                self.queue.push_back(item);
            }
            // Reset wait counts when we extract new data
            for count in &mut self.wait_counts {
                *count = 0;
            }
        }
    }

    // Check if all output ports have received two Wait messages
    fn all_ports_received_two_waits(&self) -> bool {
        self.wait_counts.iter().all(|&count| count == 2)
    }

    fn process_events(&mut self) -> Result<Event> {
        // Check if all outputs are finished
        let all_outputs_finished = self
            .outputs
            .iter()
            .all(|output| output.status == PortStatus::Finished);

        if all_outputs_finished {
            self.input.port.finish();
            return Ok(Event::Finished);
        }

        // Check if input is finished and queue is empty
        if self.input.status == PortStatus::Finished && self.queue.is_empty() {
            // Finish all output ports
            for output in &mut self.outputs {
                if output.status != PortStatus::Finished {
                    output.port.finish();
                    output.status = PortStatus::Finished;
                }
            }
            return Ok(Event::Finished);
        }

        // Check if any output needs data
        for output in &self.outputs {
            if output.port.is_need_data() && output.status != PortStatus::Finished {
                return Ok(Event::NeedConsume);
            }
        }

        // Check if we need to pull from input
        if self.input.status == PortStatus::NeedData {
            self.input.port.set_need_data();
            return Ok(Event::NeedData);
        }

        Ok(Event::NeedConsume)
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
        // Handle initialization
        if !self.initialized {
            self.initialized = true;
            self.input.status = PortStatus::NeedData;
            return Ok(Event::NeedData);
        }

        // Handle output port events
        if let EventCause::Output(output_index) = cause {
            let output = &self.outputs[output_index];

            // Skip if this output is already finished
            if output.status == PortStatus::Finished {
                return self.process_events();
            }

            // Check if output port needs data
            if output.port.is_need_data() {
                if !self.queue.is_empty() {
                    // Have data in queue, dispatch it
                    let meta = self.queue.pop_front().unwrap();
                    let data_block = DataBlock::empty_with_meta(Box::new(meta));
                    output.port.push_data(Ok(data_block));
                    return self.process_events();
                } else {
                    // Queue is empty, send Wait
                    let meta = AggregateMeta::Wait;
                    let data_block = DataBlock::empty_with_meta(Box::new(meta));
                    output.port.push_data(Ok(data_block));

                    // Increment wait count for this port
                    self.wait_counts[output_index] += 1;

                    // Check if all ports have received two waits
                    if self.all_ports_received_two_waits() && self.input.status == PortStatus::Idle
                    {
                        // Time to pull from upstream
                        self.input.status = PortStatus::NeedData;
                    }

                    return self.process_events();
                }
            }
        }

        // Handle input port events
        if let EventCause::Input(_) = cause {
            if self.input.port.has_data() {
                // Pull data from input
                let mut data_block = self.input.port.pull_data().unwrap()?;
                self.input.status = PortStatus::Idle;

                // Extract meta from data block
                if let Some(block_meta) = data_block
                    .get_meta()
                    .and_then(AggregateMeta::downcast_ref_from)
                {
                    if matches!(block_meta, AggregateMeta::Partitioned { .. }) {
                        // Take ownership of meta and extract it
                        let meta = data_block.take_meta().unwrap();
                        let aggregate_meta = AggregateMeta::downcast_from(meta).unwrap();
                        self.extract_partitioned(aggregate_meta);
                        return self.process_events();
                    }
                }
            }

            if self.input.port.is_finished() {
                self.input.status = PortStatus::Finished;
                return self.process_events();
            }
        }

        self.process_events()
    }
}
