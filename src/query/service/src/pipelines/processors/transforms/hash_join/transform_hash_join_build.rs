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
use log::debug;

use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::transforms::hash_join::BuildSpillState;
use crate::pipelines::processors::transforms::hash_join::HashJoinBuildState;
use crate::pipelines::processors::Processor;

#[derive(Clone, Debug)]
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
    // Start the first spill
    FirstSpill,
    // Following spill after the first spill
    FollowSpill,
    // Wait probe
    WaitProbe,
}

pub struct TransformHashJoinBuild {
    input_port: Arc<InputPort>,

    input_data: Option<DataBlock>,
    step: HashJoinBuildStep,
    build_state: Arc<HashJoinBuildState>,
    spill_state: Option<Box<BuildSpillState>>,
    spill_data: Option<DataBlock>,
    finalize_finished: bool,
    processor_id: usize,
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
        let processor_id = build_state.build_attach()?;
        Ok(Box::new(TransformHashJoinBuild {
            input_port,
            input_data: None,
            step: HashJoinBuildStep::Running,
            build_state,
            spill_state,
            spill_data: None,
            finalize_finished: false,
            processor_id,
        }))
    }

    fn wait_spill(&mut self, data_block: DataBlock) -> Result<()> {
        let spill_state = self.spill_state.as_ref().unwrap();
        let wait = spill_state.spill_coordinator.wait_spill()?;
        self.input_data = Some(data_block);
        if wait {
            debug!("Processor {} needs to wait spill", self.processor_id);
            self.step = HashJoinBuildStep::WaitSpill;
        } else {
            dbg!(
                "Processor {} is the last processor, it will split spill tasks and notify all processors to spill",
                self.processor_id
            );
            // Make `need_spill` to false for `SpillCoordinator`
            spill_state.spill_coordinator.no_need_spill();
            // Before notify all processors to spill, we need to collect all buffered data in `RowSpace` and `Chunks`
            // Partition all rows and stat how many partitions and rows in each partition.
            // Then choose the largest partitions(which contain rows that can avoid oom exactly) to spill.
            // For each partition, we should equally divide the rows into each processor.
            // Then all processors will spill same partitions.
            spill_state.split_spill_tasks()?;
            spill_state.spill_coordinator.notify_spill();
            self.step = HashJoinBuildStep::FirstSpill;
        }
        Ok(())
    }

    fn reset(&mut self) -> Result<()> {
        self.build_state.build_attach()?;
        self.step = HashJoinBuildStep::Running;
        self.finalize_finished = false;
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
                true => {
                    // If join spill is enabled, we should wait probe to spill.
                    // Then restore data from disk and build hash table, util all spilled data are processed.
                    if let Some(spill_state) = &mut self.spill_state && !spill_state.spill_coordinator.spill_tasks.read().is_empty() {
                        dbg!("?");
                        // Send spilled partition to `HashJoinState`, used by probe spill.
                        self.build_state
                            .hash_join_state
                            .set_spilled_partition(&spill_state.spiller.spilled_partition_set);
                        self.step = HashJoinBuildStep::WaitProbe;
                        Ok(Event::Async)
                    } else {
                        dbg!("??");
                        Ok(Event::Finished)
                    }
                }
            },
            HashJoinBuildStep::FastReturn => Ok(Event::Finished),
            HashJoinBuildStep::WaitSpill
            | HashJoinBuildStep::FirstSpill
            | HashJoinBuildStep::FollowSpill
            | HashJoinBuildStep::WaitProbe => Ok(Event::Async),
        }
    }

    fn interrupt(&self) {
        self.build_state.hash_join_state.interrupt()
    }

    fn process(&mut self) -> Result<()> {
        match self.step {
            HashJoinBuildStep::Running => {
                if let Some(data_block) = self.input_data.take() {
                    if let Some(spill_state) = &mut self.spill_state && !spill_state.spiller.is_all_spilled(){
                        // Check if need to spill
                        let need_spill = spill_state.check_need_spill()?;
                        if need_spill {
                            spill_state.spill_coordinator.need_spill()?;
                            self.wait_spill(data_block)?;
                        } else {
                            if spill_state.spill_coordinator.get_need_spill() {
                                // even if input can fit into memory, but there exists one processor need to spill,
                                // then it needs to wait spill.
                                self.wait_spill(data_block)?;
                            } else {
                                // If the processor had spilled data, we should continue to spill
                                if spill_state.spiller.is_any_spilled() {
                                    self.step = HashJoinBuildStep::FollowSpill;
                                    self.spill_data = Some(data_block);
                                } else {
                                    self.build_state.build(data_block.clone())?;
                                }
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
            HashJoinBuildStep::FastReturn
            | HashJoinBuildStep::WaitSpill
            | HashJoinBuildStep::FirstSpill
            | HashJoinBuildStep::FollowSpill
            | HashJoinBuildStep::WaitProbe => unreachable!(),
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
                self.step = HashJoinBuildStep::FirstSpill
            }
            HashJoinBuildStep::FirstSpill => {
                self.spill_state.as_mut().unwrap().spill().await?;
                // After spill, the processor should continue to run, and process incoming data.
                // FIXME: We should wait all processors finish spill, and then continue to run.
                self.step = HashJoinBuildStep::Running;
            }
            HashJoinBuildStep::FollowSpill => {
                if let Some(data) = self.spill_data.take() {
                    let unspilled_data =
                        self.spill_state.as_mut().unwrap().spill_input(data).await?;
                    if !unspilled_data.is_empty() {
                        self.build_state.build(unspilled_data)?;
                    }
                }
                self.step = HashJoinBuildStep::Running;
            }
            HashJoinBuildStep::WaitProbe => {
                dbg!("wait probe");
                if !*self.build_state.hash_join_state.probe_spill_done.lock() {
                    self.build_state.hash_join_state.wait_probe_spill().await;
                } else {
                    self.build_state
                        .hash_join_state
                        .notify_build
                        .notified()
                        .await;
                }
                // Currently, each processor will read its own partition
                // Note: we assume that the partition files will fit into memory
                // later, will introduce multiple level spill or other way to handle this.
                let partition_id = *self.build_state.hash_join_state.partition_id.read();
                dbg!("start to read data");
                let spilled_data = self
                    .spill_state
                    .as_ref()
                    .unwrap()
                    .spiller
                    .read_spilled_data(&partition_id)
                    .await?;
                dbg!("finish read");
                debug!(
                    "processor: {}, finish reading spilled data",
                    self.processor_id
                );
                self.input_data = Some(DataBlock::concat(&spilled_data)?);
                self.reset()?;
            }
            _ => {}
        }
        Ok(())
    }
}
