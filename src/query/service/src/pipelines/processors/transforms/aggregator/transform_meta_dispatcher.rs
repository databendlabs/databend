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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::EventCause;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
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
    data_ready: bool,
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
            data_ready: false,
        })
    }

    pub fn input_port(&self) -> Arc<InputPort> {
        self.input.port.clone()
    }

    pub fn output_ports(&self) -> Vec<Arc<OutputPort>> {
        self.outputs.iter().map(|x| x.port.clone()).collect()
    }

    // Extract Partitioned aggregate meta and push all items into the waiting queue
    fn extract_partitioned(&mut self, mut data_block: DataBlock) -> Result<()> {
        let Some(meta) = data_block
            .take_meta()
            .and_then(AggregateMeta::downcast_from)
        else {
            return Err(ErrorCode::Internal(
                "TransformMetaDispatcher expects AggregateMeta",
            ));
        };
        if let AggregateMeta::Partitioned { data, .. } = meta {
            for item in data {
                self.queue.push_back(item);
            }
        }
        Ok(())
    }

    fn bucket_finished(&self) -> bool {
        self.shared_state.bucket_finished.load(Ordering::SeqCst) == self.outputs.len()
    }

    fn debug_event(&mut self, cause: EventCause) -> Result<Event> {
        info!(
            "dispatcher event cause: {:?}, queue {}",
            cause,
            self.queue.len()
        );
        if let EventCause::Output(output_index) = &cause {
            let output = &mut self.outputs[*output_index];

            if output.port.is_finished() {
                if output.status != PortStatus::Finished {
                    self.finished_outputs += 1;
                    output.status = PortStatus::Finished;
                }
            } else if output.port.can_push() {
                if output.status != PortStatus::NeedData {
                    info!("dispatcher output {} can push", output_index);
                    output.status = PortStatus::NeedData;
                    self.waiting_outputs.push_back(*output_index);
                }
            }
        }

        if !self.initialized && !self.waiting_outputs.is_empty() {
            self.initialized = true;
            self.input.port.set_need_data();
        }

        if self.finished_outputs == self.outputs.len() {
            self.input.port.finish();
            return Ok(Event::Finished);
        }

        if self.input.port.is_finished() {
            info!("flag input finished");
            if self.input.status != PortStatus::Finished {
                self.input.status = PortStatus::Finished;
            }
        } else if self.input.port.has_data() {
            if self.input.status != PortStatus::HasData {
                info!("dispatcher input get data, set ready to true");
                self.data_ready = true;
                self.input.status = PortStatus::HasData;
                let data_block = self.input.port.pull_data().unwrap()?;
                debug_assert!(self.queue.is_empty());
                self.extract_partitioned(data_block)?;

                if self.input.port.is_finished() {
                    info!("flag input finished after pull data");
                    if self.input.status != PortStatus::Finished {
                        self.input.status = PortStatus::Finished;
                        // todo: wo may not need this?
                    }
                } else {
                    self.input.port.set_need_data();
                }
            }
        }

        if self.bucket_finished() {
            info!("this bucket finished, set data ready to false");
            self.input.status = PortStatus::Idle;
            // we cannot begin next round until all aggregate work finished
            self.shared_state.bucket_finished.store(0, Ordering::SeqCst);
            self.data_ready = false;
            if self.input.port.is_finished() {
                info!("finish all output port");
                for output in &self.outputs {
                    output.port.finish();
                }
                return Ok(Event::Finished);
            }
        }

        info!("{:?} {:?}", self.waiting_outputs, self.data_ready);

        while !self.waiting_outputs.is_empty() && self.data_ready {
            let output_index = self
                .waiting_outputs
                .pop_front()
                .ok_or_else(|| ErrorCode::Internal("Waiting outputs queue should not be empty"))?;

            if let Some(meta) = self.queue.pop_front() {
                let output = &mut self.outputs[output_index];
                output
                    .port
                    .push_data(Ok(DataBlock::empty_with_meta(Box::new(meta))));
                output.status = PortStatus::Idle;
                info!("send meta to output {}", output_index);
                continue;
            }
            info!(
                "aggregate finished: {}",
                self.shared_state.bucket_finished.load(Ordering::SeqCst)
            );

            info!("send wait to output {}", output_index);
            let output = &mut self.outputs[output_index];
            output
                .port
                .push_data(Ok(DataBlock::empty_with_meta(Box::new(
                    AggregateMeta::Wait,
                ))));
            output.status = PortStatus::Idle;
        }

        match self.waiting_outputs.is_empty() {
            true => Ok(Event::NeedConsume),
            false => Ok(Event::NeedData),
        }
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
        let event = self.debug_event(cause)?;
        info!("TransformMetaDispatcher return event: {:?}", event);
        Ok(event)
    }
}
