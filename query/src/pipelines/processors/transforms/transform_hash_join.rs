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

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::Result;

use super::hash_join::ProbeState;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::transforms::hash_join::HashJoinState;
use crate::pipelines::processors::Processor;
use crate::pipelines::processors::Sink;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

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
}

pub struct TransformHashJoinProbe {
    input_data: Option<DataBlock>,
    output_data_blocks: VecDeque<DataBlock>,

    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    step: HashJoinStep,
    join_state: Arc<dyn HashJoinState>,
    probe_state: ProbeState,
}

impl TransformHashJoinProbe {
    pub fn create(
        ctx: Arc<QueryContext>,
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        join_state: Arc<dyn HashJoinState>,
        _output_schema: DataSchemaRef,
    ) -> ProcessorPtr {
        let default_block_size = ctx.get_settings().get_max_block_size().unwrap_or(102400);
        ProcessorPtr::create(Box::new(TransformHashJoinProbe {
            input_data: None,
            output_data_blocks: VecDeque::new(),
            input_port,
            output_port,
            step: HashJoinStep::Build,
            join_state,
            probe_state: ProbeState::with_capacity(default_block_size as usize),
        }))
    }

    fn probe(&mut self, block: &DataBlock) -> Result<()> {
        self.probe_state.clear();
        self.output_data_blocks
            .extend(self.join_state.probe(block, &mut self.probe_state)?);
        Ok(())
    }
}

#[async_trait::async_trait]
impl Processor for TransformHashJoinProbe {
    fn name(&self) -> &'static str {
        "HashJoin"
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        match self.step {
            HashJoinStep::Build => Ok(Event::Async),
            HashJoinStep::Probe => {
                if self.output_port.is_finished() {
                    self.input_port.finish();
                    return Ok(Event::Finished);
                }

                if !self.output_port.can_push() {
                    self.input_port.set_not_need_data();
                    return Ok(Event::NeedConsume);
                }

                if !self.output_data_blocks.is_empty() {
                    let data = self.output_data_blocks.pop_front().unwrap();
                    self.output_port.push_data(Ok(data));
                    return Ok(Event::NeedConsume);
                }

                if self.input_data.is_some() {
                    return Ok(Event::Sync);
                }

                if self.input_port.has_data() {
                    let data = self.input_port.pull_data().unwrap()?;
                    self.input_data = Some(data);
                    return Ok(Event::Sync);
                }

                if self.input_port.is_finished() {
                    self.output_port.finish();
                    return Ok(Event::Finished);
                }

                self.input_port.set_need_data();
                Ok(Event::NeedData)
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        match self.step {
            HashJoinStep::Build => Ok(()),
            HashJoinStep::Probe => {
                if let Some(data) = self.input_data.take() {
                    self.probe(&data)?;
                }
                Ok(())
            }
        }
    }

    async fn async_process(&mut self) -> Result<()> {
        if let HashJoinStep::Build = &self.step {
            self.join_state.wait_finish().await?;
            self.step = HashJoinStep::Probe;
        }

        Ok(())
    }
}
