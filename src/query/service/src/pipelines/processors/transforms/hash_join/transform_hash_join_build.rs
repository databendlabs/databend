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
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;

use crate::pipelines::processors::transforms::hash_join::build_spill::BuildSpillHandler;
use crate::pipelines::processors::transforms::hash_join::BuildSpillState;
use crate::pipelines::processors::transforms::hash_join::HashJoinBuildState;
use crate::pipelines::processors::Event;
use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::Processor;

#[derive(Clone, Debug)]
pub(crate) enum HashJoinBuildStep {
    // The running step of the build phase.
    Running,
    // The finalize step is waiting all build threads to finish and build the hash table.
    Finalize,
    // The fast return step indicates there is no data in build side,
    // so we can directly finish the following steps for hash join and return empty result.
    FastReturn,
    // Wait to spill
    WaitSpill,
    // Wait probe
    WaitProbe,
    // The whole build phase is finished.
    Finished,
}

pub struct TransformHashJoinBuild {
    input_port: Arc<InputPort>,
    input_data: Option<DataBlock>,
    step: HashJoinBuildStep,
    build_state: Arc<HashJoinBuildState>,
    finalize_finished: bool,
    processor_id: usize,

    spill_handler: BuildSpillHandler,
}

impl TransformHashJoinBuild {
    pub fn try_create(
        input_port: Arc<InputPort>,
        build_state: Arc<HashJoinBuildState>,
        spill_state: Option<Box<BuildSpillState>>,
    ) -> Result<Box<dyn Processor>> {
        let processor_id = build_state.build_attach();
        Ok(Box::new(TransformHashJoinBuild {
            input_port,
            input_data: None,
            step: HashJoinBuildStep::Running,
            build_state,
            finalize_finished: false,
            processor_id,
            spill_handler: BuildSpillHandler::create(spill_state),
        }))
    }

    // Called after processor read spilled data
    // It means next round build will start, need to reset some variables.
    async fn reset(&mut self) -> Result<()> {
        self.finalize_finished = false;
        self.spill_handler.set_after_spill(true);
        // Only need to reset the following variables once
        if self
            .build_state
            .row_space_builders
            .fetch_add(1, Ordering::Acquire)
            == 0
        {
            self.build_state.send_val.store(2, Ordering::Release);
            // Before build processors into `WaitProbe` state, set the channel message to false.
            // Then after all probe processors are ready, the last one will send true to channel and wake up all build processors.
            self.build_state
                .hash_join_state
                .continue_build_watcher
                .send(false)
                .map_err(|_| ErrorCode::TokioError("continue_build_watcher channel is closed"))?;
            let worker_num = self.build_state.build_worker_num.load(Ordering::Relaxed) as usize;
            self.build_state
                .hash_join_state
                .hash_table_builders
                .store(worker_num, Ordering::Relaxed);
            self.build_state.hash_join_state.reset();
        }
        self.step = HashJoinBuildStep::Running;
        self.build_state.restore_barrier.wait().await;
        Ok(())
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
                if self.spill_handler.check_need_spill(&mut self.input_data)? {
                    self.step = HashJoinBuildStep::WaitSpill;
                    return Ok(Event::Async);
                }

                if self.input_data.is_some() {
                    return Ok(Event::Sync);
                }

                if self.input_port.is_finished() {
                    if self.spill_handler.enabled_spill() {
                        self.spill_handler
                            .finalize_spill(&self.build_state, self.processor_id)?;
                    }
                    self.build_state.row_space_build_done()?;
                    return Ok(Event::Async);
                }

                match self.input_port.has_data() {
                    true => {
                        self.input_data = Some(self.input_port.pull_data().unwrap()?);
                        if self.spill_handler.check_need_spill(&mut self.input_data)? {
                            self.step = HashJoinBuildStep::WaitSpill;
                            return Ok(Event::Async);
                        }
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
                true => {
                    // If join spill is enabled, we should wait probe to spill even if the processor didn't spill really.
                    // It needs to consume the barrier in next steps.
                    // Then restore data from disk and build hash table, util all spilled data are processed.
                    if self.spill_handler.enabled_spill() {
                        self.step = HashJoinBuildStep::WaitProbe;
                        Ok(Event::Async)
                    } else {
                        Ok(Event::Finished)
                    }
                }
            },
            HashJoinBuildStep::FastReturn | HashJoinBuildStep::Finished => {
                self.input_port.finish();
                Ok(Event::Finished)
            }
            HashJoinBuildStep::WaitSpill | HashJoinBuildStep::WaitProbe => Ok(Event::Async),
        }
    }

    fn interrupt(&self) {
        self.build_state.hash_join_state.interrupt()
    }

    fn process(&mut self) -> Result<()> {
        match self.step {
            HashJoinBuildStep::Running => {
                if let Some(data_block) = self.input_data.take() {
                    if self.spill_handler.after_spill() {
                        return self.build_state.build(data_block);
                    }
                    self.build_state.build(data_block)?;
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
            HashJoinBuildStep::FastReturn
            | HashJoinBuildStep::WaitSpill
            | HashJoinBuildStep::WaitProbe
            | HashJoinBuildStep::Finished => unreachable!(),
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match &self.step {
            HashJoinBuildStep::Running => {
                self.build_state.barrier.wait().await;
                if self
                    .build_state
                    .hash_join_state
                    .fast_return
                    .load(Ordering::Relaxed)
                {
                    self.step = HashJoinBuildStep::FastReturn;
                    return Ok(());
                }
                self.step = HashJoinBuildStep::Finalize;
            }
            HashJoinBuildStep::WaitSpill => {
                self.spill_handler.spill().await?;
                // After spill, the processor should continue to run, and process incoming data.
                self.step = HashJoinBuildStep::Running;
            }
            HashJoinBuildStep::WaitProbe => {
                self.build_state.hash_join_state.wait_probe_notify().await?;
                // Currently, each processor will read its own partition
                // Note: we assume that the partition files will fit into memory
                // later, will introduce multiple level spill or other way to handle this.
                let partition_id = self
                    .build_state
                    .hash_join_state
                    .partition_id
                    .load(Ordering::Relaxed);
                // If there is no partition to restore, probe will send `-1` to build
                // Which means it's time to finish.
                if partition_id == -1 {
                    self.build_state
                        .hash_join_state
                        .build_done_watcher
                        .send(2)
                        .map_err(|_| {
                            ErrorCode::TokioError("build_done_watcher channel is closed")
                        })?;
                    self.step = HashJoinBuildStep::Finished;
                    return Ok(());
                }
                self.input_data = self.spill_handler.restore(partition_id).await?;
                self.build_state.restore_barrier.wait().await;
                self.reset().await?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}
