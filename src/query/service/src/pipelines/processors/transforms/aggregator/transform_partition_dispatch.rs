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
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::EventCause;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;

#[derive(PartialEq)]
enum PortStatus {
    Idle,
    NeedData,
    Finished,
}

struct PortWithStatus<Port> {
    pub status: PortStatus,
    pub port: Arc<Port>,
}

pub struct TransformPartitionDispatch {
    initialized: bool,

    finished_outputs: usize,
    waiting_outputs: VecDeque<usize>,
    waiting_outputs_2: VecDeque<usize>,

    sync_final_partition: bool,
    sent_final_partition: Vec<bool>,
    synchronized_final_partition: Vec<bool>,

    current_data: Option<DataBlock>,

    input: Arc<InputPort>,
    outputs: Vec<PortWithStatus<OutputPort>>,
}

impl TransformPartitionDispatch {
    pub fn create(outputs: usize) -> TransformPartitionDispatch {
        let mut outputs_port = Vec::with_capacity(outputs);

        for _index in 0..outputs {
            outputs_port.push(PortWithStatus {
                status: PortStatus::Idle,
                port: OutputPort::create(),
            });
        }

        TransformPartitionDispatch {
            initialized: false,
            finished_outputs: 0,
            outputs: outputs_port,
            input: InputPort::create(),
            waiting_outputs: VecDeque::with_capacity(outputs),
            waiting_outputs_2: VecDeque::with_capacity(outputs),
            current_data: None,
            sync_final_partition: false,
            sent_final_partition: vec![false; outputs],
            synchronized_final_partition: vec![false; outputs],
        }
    }

    pub fn get_inputs(&self) -> Vec<Arc<InputPort>> {
        vec![self.input.clone()]
    }

    pub fn get_outputs(&self) -> Vec<Arc<OutputPort>> {
        self.outputs.iter().map(|x| x.port.clone()).collect()
    }

    fn unpark_block(mut data_block: DataBlock) -> Result<(AggregateMeta, DataBlock)> {
        let Some(meta) = data_block.take_meta() else {
            return Err(ErrorCode::Internal(
                "Internal, TransformPartitionBucket only recv DataBlock with meta.",
            ));
        };

        let Some(meta) = AggregateMeta::downcast_from(meta) else {
            return Err(ErrorCode::Internal(
                "Internal, TransformPartitionBucket only recv AggregateMeta".to_string(),
            ));
        };

        Ok((meta, data_block))
    }
}

impl Processor for TransformPartitionDispatch {
    fn name(&self) -> String {
        String::from("TransformPartitionDispatch")
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
                if self.sync_final_partition {
                    if self.sent_final_partition[*output_index] {
                        self.waiting_outputs_2.push_back(*output_index);
                        self.synchronized_final_partition[*output_index] = true;
                    } else {
                        self.sent_final_partition[*output_index] = true;
                        output.port.push_data(Ok(DataBlock::empty_with_meta(
                            AggregateMeta::create_final(None),
                        )));
                    }
                } else if output.status != PortStatus::NeedData {
                    output.status = PortStatus::NeedData;
                    self.waiting_outputs.push_back(*output_index);
                }
            }
        }

        if !self.initialized && !self.waiting_outputs.is_empty() {
            self.initialized = true;
            self.input.set_need_data();
        }

        if self.finished_outputs == self.outputs.len() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if let EventCause::Input(_) = &cause {
            if !self.sync_final_partition && self.input.has_data() && self.current_data.is_none() {
                let data_block = self.input.pull_data().unwrap()?;
                let (meta, data_block) = Self::unpark_block(data_block)?;

                match meta {
                    AggregateMeta::FinalPartition(_) => {
                        self.sync_final_partition = true;
                        self.input.set_not_need_data();
                    }
                    meta => {
                        self.input.set_need_data();
                        self.current_data = Some(data_block.add_meta(Some(Box::new(meta)))?);
                    }
                };
            }
        }

        while self.sync_final_partition {
            while let Some(output_index) = self.waiting_outputs.pop_front() {
                if self.outputs[output_index].port.is_finished() {
                    self.synchronized_final_partition[output_index] = true;

                    if self.outputs[output_index].status != PortStatus::Finished {
                        self.finished_outputs += 1;
                        self.outputs[output_index].status = PortStatus::Finished;
                    }
                }

                self.outputs[output_index]
                    .port
                    .push_data(Ok(DataBlock::empty_with_meta(AggregateMeta::create_final(
                        None,
                    ))));
                self.sent_final_partition[output_index] = true;
                self.outputs[output_index].status = PortStatus::Idle;
            }

            for (idx, synchronized) in self.synchronized_final_partition.iter().enumerate() {
                if !synchronized && !self.outputs[idx].port.is_finished() {
                    return Ok(Event::NeedConsume);
                }
            }

            self.sync_final_partition = false;
            self.sent_final_partition = vec![false; self.sent_final_partition.len()];
            self.synchronized_final_partition = vec![false; self.sent_final_partition.len()];
            std::mem::swap(&mut self.waiting_outputs, &mut self.waiting_outputs_2);

            if self.input.has_data() {
                let data_block = self.input.pull_data().unwrap()?;
                let (meta, data_block) = Self::unpark_block(data_block)?;

                match meta {
                    AggregateMeta::FinalPartition(_) => {
                        self.sync_final_partition = true;
                        self.input.set_not_need_data();
                        continue;
                    }
                    meta => {
                        self.current_data = Some(data_block.add_meta(Some(Box::new(meta)))?);
                    }
                };
            }

            self.input.set_need_data();
            break;
        }

        while !self.waiting_outputs.is_empty() && self.current_data.is_some() {
            let output_index = self.waiting_outputs.pop_front().unwrap();

            // Port is finished when waiting.
            if self.outputs[output_index].port.is_finished() {
                if self.outputs[output_index].status != PortStatus::Finished {
                    self.finished_outputs += 1;
                    self.outputs[output_index].status = PortStatus::Finished;
                }

                continue;
            }

            if let Some(data_block) = self.current_data.take() {
                self.outputs[output_index].port.push_data(Ok(data_block));
                self.outputs[output_index].status = PortStatus::Idle;
                self.input.set_need_data();
            }
        }

        if self.finished_outputs == self.outputs.len() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if self.input.is_finished() && self.current_data.is_none() {
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
