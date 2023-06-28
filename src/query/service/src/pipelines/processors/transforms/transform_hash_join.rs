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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_sql::plans::JoinType;

use super::hash_join::ProbeState;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::transforms::hash_join::desc::JOIN_MAX_BLOCK_SIZE;
use crate::pipelines::processors::transforms::hash_join::HashJoinState;
use crate::pipelines::processors::Processor;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

enum HashJoinStep {
    Build,
    Finalize,
    Probe,
    FinalScan,
    Finished,
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
    outer_scan_finished: bool,
    output_buffer: Vec<DataBlock>,
    output_buffer_size: usize,
}

impl TransformHashJoinProbe {
    pub fn create(
        ctx: Arc<QueryContext>,
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        join_state: Arc<dyn HashJoinState>,
        join_type: &JoinType,
        with_conjunct: bool,
    ) -> Result<Box<dyn Processor>> {
        let default_block_size = ctx.get_settings().get_max_block_size()?;
        Ok(Box::new(TransformHashJoinProbe {
            input_data: VecDeque::new(),
            output_data_blocks: VecDeque::new(),
            input_port,
            output_port,
            step: HashJoinStep::Build,
            join_state,
            probe_state: ProbeState::create(join_type, with_conjunct, ctx.get_function_context()?),
            block_size: default_block_size,
            outer_scan_finished: false,
            output_buffer: vec![],
            output_buffer_size: 0,
        }))
    }

    pub fn attach(join_state: Arc<dyn HashJoinState>) -> Result<Arc<dyn HashJoinState>> {
        join_state.probe_attach()?;
        Ok(join_state)
    }

    fn probe(&mut self, block: &DataBlock) -> Result<()> {
        self.probe_state.clear();
        let data_blocks = self.join_state.probe(block, &mut self.probe_state)?;
        for datablock in data_blocks.into_iter() {
            if datablock.num_rows() >= JOIN_MAX_BLOCK_SIZE {
                self.output_data_blocks.push_back(datablock);
                continue;
            }
            self.output_buffer_size += datablock.num_rows();
            self.output_buffer.push(datablock);
            if self.output_buffer_size >= JOIN_MAX_BLOCK_SIZE {
                let data_block = DataBlock::concat(self.output_buffer.as_slice())?;
                self.output_data_blocks.push_back(data_block);
                self.output_buffer_size = 0;
                self.output_buffer.clear();
            }
        }
        Ok(())
    }

    fn final_scan(&mut self, task: usize) -> Result<()> {
        let data_blocks = self.join_state.final_scan(task, &mut self.probe_state)?;
        for datablock in data_blocks.into_iter() {
            if datablock.num_rows() >= JOIN_MAX_BLOCK_SIZE {
                self.output_data_blocks.push_back(datablock);
                continue;
            }
            self.output_buffer_size += datablock.num_rows();
            self.output_buffer.push(datablock);
            if self.output_buffer_size >= JOIN_MAX_BLOCK_SIZE {
                let data_block = DataBlock::concat(self.output_buffer.as_slice())?;
                self.output_data_blocks.push_back(data_block);
                self.output_buffer_size = 0;
                self.output_buffer.clear();
            }
        }
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
            HashJoinStep::Finished => {
                self.output_port.finish();
                Ok(Event::Finished)
            }
            HashJoinStep::Probe => {
                if self.output_port.is_finished() {
                    self.input_port.finish();

                    if self.join_state.need_outer_scan() || self.join_state.need_mark_scan() {
                        self.join_state.probe_done()?;
                    }

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
                    if self.output_buffer_size > 0 {
                        let data = DataBlock::concat(self.output_buffer.as_slice())?;
                        // self.output_port.can_push() is true, so we can push data.
                        self.output_port.push_data(Ok(data));
                        self.output_buffer_size = 0;
                        return Ok(Event::NeedConsume);
                    }
                    return if self.join_state.need_outer_scan() || self.join_state.need_mark_scan()
                    {
                        self.join_state.probe_done()?;
                        Ok(Event::Async)
                    } else {
                        self.output_port.finish();
                        Ok(Event::Finished)
                    };
                }

                self.input_port.set_need_data();
                Ok(Event::NeedData)
            }
            HashJoinStep::FinalScan => {
                if self.output_port.is_finished() {
                    return Ok(Event::Finished);
                }

                if !self.output_port.can_push() {
                    return Ok(Event::NeedConsume);
                }

                if !self.output_data_blocks.is_empty() {
                    let data = self.output_data_blocks.pop_front().unwrap();
                    self.output_port.push_data(Ok(data));
                    return Ok(Event::NeedConsume);
                }

                match self.outer_scan_finished {
                    false => Ok(Event::Sync),
                    true => {
                        if self.output_buffer_size > 0 {
                            let data = DataBlock::concat(self.output_buffer.as_slice())?;
                            // self.output_port.can_push() is true, so we can push data.
                            self.output_port.push_data(Ok(data));
                            self.output_buffer_size = 0;
                            return Ok(Event::NeedConsume);
                        }
                        self.output_port.finish();
                        Ok(Event::Finished)
                    }
                }
            }
        }
    }

    fn interrupt(&self) {
        self.join_state.interrupt()
    }

    fn process(&mut self) -> Result<()> {
        match self.step {
            HashJoinStep::Build => Ok(()),
            HashJoinStep::Finalize | HashJoinStep::Finished => unreachable!(),
            HashJoinStep::Probe => {
                if let Some(data) = self.input_data.pop_front() {
                    let data = data.convert_to_full();
                    self.probe(&data)?;
                }
                Ok(())
            }
            HashJoinStep::FinalScan => {
                if let Some(task) = self.join_state.final_scan_task() {
                    self.final_scan(task)?;
                } else {
                    self.outer_scan_finished = true;
                }
                Ok(())
            }
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match self.step {
            HashJoinStep::Build => {
                self.join_state.wait_finalize_finish().await?;
                if self.join_state.fast_return()? {
                    match self.join_state.join_type() {
                        JoinType::Inner
                        | JoinType::Right
                        | JoinType::Cross
                        | JoinType::RightAnti
                        | JoinType::RightSemi
                        | JoinType::LeftSemi => {
                            self.step = HashJoinStep::Finished;
                        }
                        JoinType::Left | JoinType::Full | JoinType::Single | JoinType::LeftAnti => {
                            self.step = HashJoinStep::Probe;
                        }
                        _ => {
                            return Err(ErrorCode::Internal(format!(
                                "Join type: {:?} is unexpected",
                                self.join_state.join_type()
                            )));
                        }
                    }
                    return Ok(());
                }
                self.step = HashJoinStep::Probe;
            }
            HashJoinStep::Finalize => unreachable!(),
            HashJoinStep::Probe => {
                self.join_state.wait_probe_finish().await?;
                if self.join_state.fast_return()? {
                    self.step = HashJoinStep::Finished;
                } else {
                    self.step = HashJoinStep::FinalScan;
                }
            }
            HashJoinStep::FinalScan | HashJoinStep::Finished => unreachable!(),
        };
        Ok(())
    }
}

pub struct TransformHashJoinBuild {
    input_port: Arc<InputPort>,
    input_data: Option<DataBlock>,

    step: HashJoinStep,
    join_state: Arc<dyn HashJoinState>,
    finalize_finished: bool,
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
            finalize_finished: false,
        })
    }

    pub fn attach(join_state: Arc<dyn HashJoinState>) -> Result<Arc<dyn HashJoinState>> {
        join_state.build_attach()?;
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
                    self.join_state.build_done()?;
                    return Ok(Event::Async);
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
            HashJoinStep::Finalize => match self.finalize_finished {
                false => Ok(Event::Sync),
                true => Ok(Event::Finished),
            },
            HashJoinStep::Probe => unreachable!(),
            HashJoinStep::FinalScan => unreachable!(),
            HashJoinStep::Finished => Ok(Event::Finished),
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
                }
                Ok(())
            }
            HashJoinStep::Finalize => {
                if let Some(task) = self.join_state.finalize_task() {
                    self.join_state.finalize(task)
                } else {
                    self.finalize_finished = true;
                    self.join_state.finalize_done()
                }
            }
            HashJoinStep::Probe | HashJoinStep::FinalScan | HashJoinStep::Finished => {
                unreachable!()
            }
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let HashJoinStep::Build = &self.step {
            self.join_state.wait_build_finish().await?;
            if self.join_state.fast_return()? {
                self.step = HashJoinStep::Finished;
                return Ok(());
            }
            self.step = HashJoinStep::Finalize;
        }
        Ok(())
    }
}
