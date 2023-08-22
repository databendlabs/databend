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
use common_expression::FunctionContext;
use common_sql::optimizer::ColumnSet;
use common_sql::plans::JoinType;

use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::transforms::hash_join::HashJoinProbeState;
use crate::pipelines::processors::transforms::hash_join::HashJoinState;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::Processor;

enum HashJoinProbeStep {
    // The step is to wait build phase finished.
    WaitBuild,
    // The running step of the probe phase.
    Running,
    // The final scan step is used to fill missing rows for non-inner join.
    FinalScan,
    // The fast return step indicates we can directly finish the probe phase.
    FastReturn,
}

pub struct TransformHashJoinProbe {
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,

    input_data: VecDeque<DataBlock>,
    output_data_blocks: VecDeque<DataBlock>,
    projections: ColumnSet,
    step: HashJoinProbeStep,
    join_probe_state: Arc<HashJoinProbeState>,
    probe_state: ProbeState,
    max_block_size: usize,
    outer_scan_finished: bool,
}

impl TransformHashJoinProbe {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        projections: ColumnSet,
        join_probe_state: Arc<HashJoinProbeState>,
        max_block_size: usize,
        func_ctx: FunctionContext,
        join_type: &JoinType,
        with_conjunct: bool,
    ) -> Result<Box<dyn Processor>> {
        join_probe_state.probe_attach()?;
        Ok(Box::new(TransformHashJoinProbe {
            input_port,
            output_port,
            projections,
            input_data: VecDeque::new(),
            output_data_blocks: VecDeque::new(),
            step: HashJoinProbeStep::WaitBuild,
            join_probe_state,
            probe_state: ProbeState::create(max_block_size, join_type, with_conjunct, func_ctx),
            max_block_size,
            outer_scan_finished: false,
        }))
    }

    fn probe(&mut self, block: DataBlock) -> Result<()> {
        self.probe_state.clear();
        let data_blocks = self.join_probe_state.probe(block, &mut self.probe_state)?;
        if !data_blocks.is_empty() {
            self.output_data_blocks.extend(data_blocks);
        }
        Ok(())
    }

    fn final_scan(&mut self, task: usize) -> Result<()> {
        let data_blocks = self
            .join_probe_state
            .final_scan(task, &mut self.probe_state)?;
        if !data_blocks.is_empty() {
            self.output_data_blocks.extend(data_blocks);
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
            HashJoinProbeStep::WaitBuild => Ok(Event::Async),
            HashJoinProbeStep::FastReturn => {
                self.output_port.finish();
                Ok(Event::Finished)
            }
            HashJoinProbeStep::Running => {
                if self.output_port.is_finished() {
                    self.input_port.finish();

                    if self.join_probe_state.hash_join_state.need_outer_scan()
                        || self.join_probe_state.hash_join_state.need_mark_scan()
                    {
                        self.join_probe_state.probe_done()?;
                    }

                    return Ok(Event::Finished);
                }

                if !self.output_port.can_push() {
                    self.input_port.set_not_need_data();
                    return Ok(Event::NeedConsume);
                }

                if !self.output_data_blocks.is_empty() {
                    let data = self
                        .output_data_blocks
                        .pop_front()
                        .unwrap()
                        .project(&self.projections);
                    self.output_port.push_data(Ok(data));
                    return Ok(Event::NeedConsume);
                }

                if !self.input_data.is_empty() {
                    return Ok(Event::Sync);
                }

                if self.input_port.has_data() {
                    let data = self.input_port.pull_data().unwrap()?;
                    // Split data to `block_size` rows per sub block.
                    let (sub_blocks, remain_block) = data.split_by_rows(self.max_block_size);
                    self.input_data.extend(sub_blocks);
                    if let Some(remain) = remain_block {
                        self.input_data.push_back(remain);
                    }
                    return Ok(Event::Sync);
                }

                if self.input_port.is_finished() {
                    return if self.join_probe_state.hash_join_state.need_outer_scan()
                        || self.join_probe_state.hash_join_state.need_mark_scan()
                    {
                        self.join_probe_state.probe_done()?;
                        Ok(Event::Async)
                    } else {
                        self.output_port.finish();
                        Ok(Event::Finished)
                    };
                }
                self.input_port.set_need_data();
                Ok(Event::NeedData)
            }
            HashJoinProbeStep::FinalScan => {
                if self.output_port.is_finished() {
                    return Ok(Event::Finished);
                }

                if !self.output_port.can_push() {
                    return Ok(Event::NeedConsume);
                }

                if !self.output_data_blocks.is_empty() {
                    let data = self
                        .output_data_blocks
                        .pop_front()
                        .unwrap()
                        .project(&self.projections);
                    self.output_port.push_data(Ok(data));
                    return Ok(Event::NeedConsume);
                }

                match self.outer_scan_finished {
                    false => Ok(Event::Sync),
                    true => {
                        self.output_port.finish();
                        Ok(Event::Finished)
                    }
                }
            }
        }
    }

    fn interrupt(&self) {
        self.join_probe_state.hash_join_state.interrupt()
    }

    fn process(&mut self) -> Result<()> {
        match self.step {
            HashJoinProbeStep::Running => {
                if let Some(data) = self.input_data.pop_front() {
                    let data = data.convert_to_full();
                    self.probe(data)?;
                }
                Ok(())
            }
            HashJoinProbeStep::FinalScan => {
                if let Some(task) = self.join_probe_state.final_scan_task() {
                    self.final_scan(task)?;
                } else {
                    self.outer_scan_finished = true;
                }
                Ok(())
            }
            HashJoinProbeStep::WaitBuild | HashJoinProbeStep::FastReturn => unreachable!(),
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match self.step {
            HashJoinProbeStep::WaitBuild => {
                self.join_probe_state
                    .hash_join_state
                    .wait_build_hash_table_finish()
                    .await?;
                let join_type = self
                    .join_probe_state
                    .hash_join_state
                    .hash_join_desc
                    .join_type
                    .clone();
                if self.join_probe_state.hash_join_state.fast_return()? {
                    match join_type {
                        JoinType::Inner
                        | JoinType::Cross
                        | JoinType::Right
                        | JoinType::RightSingle
                        | JoinType::RightAnti
                        | JoinType::RightSemi
                        | JoinType::LeftSemi => {
                            self.step = HashJoinProbeStep::FastReturn;
                        }
                        JoinType::Left
                        | JoinType::Full
                        | JoinType::LeftSingle
                        | JoinType::LeftAnti => {
                            self.step = HashJoinProbeStep::Running;
                        }
                        _ => {
                            return Err(ErrorCode::Internal(format!(
                                "Join type: {:?} is unexpected",
                                join_type
                            )));
                        }
                    }
                    return Ok(());
                }
                self.step = HashJoinProbeStep::Running;
            }
            HashJoinProbeStep::Running => {
                self.join_probe_state.wait_probe_finish().await?;
                if self.join_probe_state.hash_join_state.fast_return()? {
                    self.step = HashJoinProbeStep::FastReturn;
                } else {
                    self.step = HashJoinProbeStep::FinalScan;
                }
            }
            HashJoinProbeStep::FinalScan | HashJoinProbeStep::FastReturn => unreachable!(),
        };
        Ok(())
    }
}
