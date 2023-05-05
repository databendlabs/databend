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

use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;

use super::hash_join::ProbeState;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::transforms::hash_join::HashJoinState;
use crate::pipelines::processors::Processor;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

enum HashJoinStep {
    Build,
    Finalize,
    Probe,
}

pub struct TransformHashJoinProbe {
    input_data: VecDeque<DataBlock>,
    output_data_blocks: VecDeque<DataBlock>,

    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    step: HashJoinStep,
    join_state: Arc<dyn HashJoinState>,
    probe_state: ProbeState,
    block_size: u64,
}

impl TransformHashJoinProbe {
    pub fn create(
        ctx: Arc<QueryContext>,
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        join_state: Arc<dyn HashJoinState>,
        _output_schema: DataSchemaRef,
    ) -> Result<Box<dyn Processor>> {
        let default_block_size = ctx.get_settings().get_max_block_size()?;
        Ok(Box::new(TransformHashJoinProbe {
            input_data: VecDeque::new(),
            output_data_blocks: VecDeque::new(),
            input_port,
            output_port,
            step: HashJoinStep::Build,
            join_state,
            probe_state: ProbeState::with_capacity(default_block_size as usize),
            block_size: default_block_size,
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
    fn name(&self) -> String {
        "HashJoinProbe".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        match self.step {
            HashJoinStep::Build => Ok(Event::Async),
            HashJoinStep::Finalize => unreachable!(),
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

                if !self.input_data.is_empty() {
                    return Ok(Event::Sync);
                }

                if self.input_port.has_data() {
                    let data = self.input_port.pull_data().unwrap()?;
                    // Split data to `block_size` rows per sub block.
                    let (sub_blocks, remain_block) = data.split_by_rows(self.block_size as usize);
                    self.input_data.extend(sub_blocks);
                    if let Some(remain) = remain_block {
                        self.input_data.push_back(remain);
                    }
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

    fn interrupt(&self) {
        self.join_state.interrupt()
    }

    fn process(&mut self) -> Result<()> {
        match self.step {
            HashJoinStep::Build => Ok(()),
            HashJoinStep::Finalize => unreachable!(),
            HashJoinStep::Probe => {
                if let Some(data) = self.input_data.pop_front() {
                    let data = data.convert_to_full();
                    self.probe(&data)?;
                }
                Ok(())
            }
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let HashJoinStep::Build = &self.step {
            self.join_state.wait_finalize_finish().await?;
            self.step = HashJoinStep::Probe;
        }

        Ok(())
    }
}

pub struct TransformHashJoinBuild {
    input_port: Arc<InputPort>,
    input_data: Option<DataBlock>,

    step: HashJoinStep,
    join_state: Arc<dyn HashJoinState>,
    called_on_build_end: bool,
    called_on_finalize_end: bool,
}

impl Drop for TransformHashJoinBuild {
    fn drop(&mut self) {
        if !self.called_on_finalize_end {
            self.called_on_finalize_end = true;
            let _ = self.join_state.finalize_end();
        }
    }
}

impl TransformHashJoinBuild {
    pub fn create(
        input_port: Arc<InputPort>,
        join_state: Arc<dyn HashJoinState>,
    ) -> Box<dyn Processor> {
        Box::new(TransformHashJoinBuild {
            input_port,
            input_data: None,
            step: HashJoinStep::Build,
            join_state,
            called_on_build_end: false,
            called_on_finalize_end: false,
        })
    }

    pub fn attach(join_state: Arc<dyn HashJoinState>) -> Result<Arc<dyn HashJoinState>> {
        join_state.attach()?;
        Ok(join_state)
    }
}

#[async_trait::async_trait]
impl Processor for TransformHashJoinBuild {
    fn name(&self) -> String {
        "HashJoinBuild".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        match self.step {
            HashJoinStep::Build => {
                if self.input_data.is_some() {
                    return Ok(Event::Sync);
                }

                if self.input_port.is_finished() {
                    return match !self.called_on_build_end {
                        true => Ok(Event::Sync),
                        false => Ok(Event::Async),
                    };
                }

                match self.input_port.has_data() {
                    true => {
                        self.input_data = Some(self.input_port.pull_data().unwrap()?);
                        Ok(Event::Sync)
                    }
                    false => {
                        self.input_port.set_need_data();
                        Ok(Event::NeedData)
                    }
                }
            }
            HashJoinStep::Finalize => match !self.called_on_finalize_end {
                true => Ok(Event::Sync),
                false => Ok(Event::Finished),
            },
            HashJoinStep::Probe => unreachable!(),
        }
    }

    fn interrupt(&self) {
        self.join_state.interrupt()
    }

    fn process(&mut self) -> Result<()> {
        match self.step {
            HashJoinStep::Build => {
                if let Some(data_block) = self.input_data.take() {
                    self.join_state.build(data_block)?;
                } else if !self.called_on_build_end {
                    self.called_on_build_end = true;
                    self.join_state.build_end()?;
                }
                Ok(())
            }
            HashJoinStep::Finalize => {
                if !self.called_on_finalize_end && !self.join_state.finalize()? {
                    self.called_on_finalize_end = true;
                    self.join_state.finalize_end()?;
                }
                Ok(())
            }
            HashJoinStep::Probe => unreachable!(),
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let HashJoinStep::Build = &self.step {
            self.join_state.wait_build_finish().await?;
            self.step = HashJoinStep::Finalize;
        }
        Ok(())
    }
}
