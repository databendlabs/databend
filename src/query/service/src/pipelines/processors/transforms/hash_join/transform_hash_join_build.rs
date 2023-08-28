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
use std::sync::Arc;

use common_exception::Result;
use common_expression::DataBlock;

use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::transforms::hash_join::BuildSpillState;
use crate::pipelines::processors::transforms::hash_join::HashJoinBuildState;
use crate::pipelines::processors::Processor;

enum HashJoinBuildStep {
    // The running step of the build phase.
    Running,
    // The finalize step is waiting all build threads to finish and build the hash table.
    Finalize,
    // The fast return step indicates there is no data in build side,
    // so we can directly finish the following steps for hash join and return empty result.
    FastReturn,
    // Wait to spill
    WaitSpill,
    // Start to spill
    Spill,
}

pub struct TransformHashJoinBuild {
    input_port: Arc<InputPort>,

    input_data: Option<DataBlock>,
    step: HashJoinBuildStep,
    build_state: Arc<HashJoinBuildState>,
    spill_state: Option<Box<BuildSpillState>>,
    finalize_finished: bool,
}

impl TransformHashJoinBuild {
    pub fn try_create(
        input_port: Arc<InputPort>,
        build_state: Arc<HashJoinBuildState>,
        spill_state: Option<Box<BuildSpillState>>,
    ) -> Result<Box<dyn Processor>> {
        if let Some(ss) = &spill_state {
            let mut count = ss.spill_coordinator.total_builder_count.write();
            *count += 1;
        }
        build_state.build_attach()?;
        Ok(Box::new(TransformHashJoinBuild {
            input_port,
            input_data: None,
            step: HashJoinBuildStep::Running,
            build_state,
            spill_state,
            finalize_finished: false,
        }))
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
            HashJoinBuildStep::Running => {
                if self.input_data.is_some() {
                    return Ok(Event::Sync);
                }

                if self.input_port.is_finished() {
                    self.build_state.row_space_build_done()?;
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
            HashJoinBuildStep::Finalize => match self.finalize_finished {
                false => Ok(Event::Sync),
                true => Ok(Event::Finished),
            },
            HashJoinBuildStep::FastReturn => Ok(Event::Finished),
            HashJoinBuildStep::WaitSpill => Ok(Event::Async),
            HashJoinBuildStep::Spill => todo!(),
        }
    }

    fn interrupt(&self) {
        self.build_state.hash_join_state.interrupt()
    }

    fn process(&mut self) -> Result<()> {
        match self.step {
            HashJoinBuildStep::Running => {
                if let Some(data_block) = self.input_data.take() {
                    if let Some(spill_state) = &mut self.spill_state {
                        // Check if need to spill
                        let need_spill = spill_state.check_need_spill(&data_block)?;
                        if need_spill {
                            self.step = HashJoinBuildStep::WaitSpill;
                            spill_state.spill_coordinator.need_spill()?;
                        } else {
                            if spill_state.spill_coordinator.get_need_spill() {
                                // even if input can fit into memory, but there exists one processor need to spill,
                                // then it needs to wait spill.
                                let wait = spill_state.spill_coordinator.wait_spill()?;
                                if wait {
                                    spill_state.buffer_data(data_block);
                                    self.step = HashJoinBuildStep::WaitSpill;
                                } else {
                                    spill_state.spill_input(data_block)?;
                                }
                            } else {
                                self.build_state.build(data_block.clone())?;
                            }
                        }
                    } else {
                        self.build_state.build(data_block.clone())?;
                    }
                }
                Ok(())
            }
            HashJoinBuildStep::Finalize => {
                if let Some(task) = self.build_state.finalize_task() {
                    self.build_state.finalize(task)
                } else {
                    self.finalize_finished = true;
                    self.build_state.build_done()
                }
            }
            HashJoinBuildStep::Spill => self.spill_state.as_mut().unwrap().spill(),
            HashJoinBuildStep::FastReturn | HashJoinBuildStep::WaitSpill => unreachable!(),
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match &self.step {
            HashJoinBuildStep::Running => {
                self.build_state.wait_row_space_build_finish().await?;
                if self.build_state.hash_join_state.fast_return()? {
                    self.step = HashJoinBuildStep::FastReturn;
                    return Ok(());
                }
                self.step = HashJoinBuildStep::Finalize;
            }
            HashJoinBuildStep::WaitSpill => {
                self.spill_state
                    .as_ref()
                    .unwrap()
                    .spill_coordinator
                    .wait_spill_notify()
                    .await;
                self.step = HashJoinBuildStep::Spill
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}
