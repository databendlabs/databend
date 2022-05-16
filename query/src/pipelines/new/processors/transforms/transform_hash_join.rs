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

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::Result;

use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::Event;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::transforms::hash_join::HashJoinState;
use crate::pipelines::new::processors::Processor;
use crate::pipelines::new::processors::Sink;
use crate::sessions::QueryContext;

pub struct SinkBuildHashTable {
    join_state: Arc<dyn HashJoinState>,
}

impl SinkBuildHashTable {
    pub fn try_create(join_state: Arc<dyn HashJoinState>) -> Result<Self> {
        join_state.attach()?;
        Ok(Self { join_state })
    }
}

impl Sink for SinkBuildHashTable {
    const NAME: &'static str = "BuildHashTable";

    fn on_finish(&mut self) -> Result<()> {
        self.join_state.detach()
    }

    fn consume(&mut self, data_block: DataBlock) -> Result<()> {
        self.join_state.build(data_block)
    }
}

enum HashJoinStep {
    Build,
    Probe,
    Finished,
}

pub struct TransformHashJoinProbe {
    input_data: Option<DataBlock>,
    output_data_blocks: Vec<DataBlock>,

    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    step: HashJoinStep,
    join_state: Arc<dyn HashJoinState>,
}

impl TransformHashJoinProbe {
    pub fn create(
        _ctx: Arc<QueryContext>,
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        join_state: Arc<dyn HashJoinState>,
        _output_schema: DataSchemaRef,
    ) -> ProcessorPtr {
        ProcessorPtr::create(Box::new(TransformHashJoinProbe {
            input_data: None,
            output_data_blocks: vec![],
            input_port,
            output_port,
            step: HashJoinStep::Build,
            join_state,
        }))
    }

    fn probe(&mut self, block: &DataBlock) -> Result<()> {
        self.output_data_blocks
            .append(&mut self.join_state.probe(block)?);
        Ok(())
    }
}

impl Processor for TransformHashJoinProbe {
    fn name(&self) -> &'static str {
        static NAME: &str = "TransformHashJoin";
        NAME
    }

    fn event(&mut self) -> Result<Event> {
        match self.step {
            HashJoinStep::Build => {
                if self.join_state.is_finished()? {
                    self.step = HashJoinStep::Probe;
                    Ok(Event::Sync)
                } else {
                    // Idle till build finished
                    Ok(Event::NeedData)
                }
            }
            HashJoinStep::Probe => {
                if self.output_port.is_finished() {
                    self.input_port.finish();
                    return Ok(Event::Finished);
                }

                if !self.output_port.can_push() {
                    return Ok(Event::NeedConsume);
                }

                if !self.output_data_blocks.is_empty() {
                    self.output_port
                        .push_data(Ok(self.output_data_blocks.remove(0)));
                    return Ok(Event::NeedConsume);
                }

                if self.input_data.is_some() {
                    return Ok(Event::Sync);
                }

                if self.input_port.is_finished() {
                    self.output_port.finish();
                    self.step = HashJoinStep::Finished;
                    return Ok(Event::Finished);
                }

                if let Some(data) = self.input_port.pull_data() {
                    self.input_data = Some(data?);
                    return Ok(Event::Sync);
                }

                self.input_port.set_need_data();
                Ok(Event::NeedData)
            }
            HashJoinStep::Finished => Ok(Event::Finished),
        }
    }

    fn process(&mut self) -> Result<()> {
        match self.step {
            HashJoinStep::Finished => Ok(()),
            HashJoinStep::Build => Ok(()),
            HashJoinStep::Probe => {
                if let Some(data) = self.input_data.take() {
                    self.probe(&data)?;
                }
                Ok(())
            }
        }
    }
}
