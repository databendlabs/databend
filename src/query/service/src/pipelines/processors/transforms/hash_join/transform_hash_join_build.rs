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

use common_exception::Result;
use common_expression::DataBlock;

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
    // The whole build phase is finished.
    Finished,
}

pub struct TransformHashJoinBuild {
    input_port: Arc<InputPort>,

    input_data: Option<DataBlock>,
    step: HashJoinBuildStep,
    build_state: Arc<HashJoinBuildState>,
    spill_state: Option<Box<BuildSpillState>>,
    spill_data: Option<DataBlock>,
    finalize_finished: bool,
    // The flag indicates whether data is from spilled data.
    from_spill: bool,
    _processor_id: usize,
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
            spill_state,
            spill_data: None,
            finalize_finished: false,
            from_spill: false,
            _processor_id: processor_id,
        }))
    }

    fn wait_spill(&mut self, data_block: DataBlock) -> Result<()> {
        let spill_state = self.spill_state.as_ref().unwrap();
        let wait = {
            let mut spill_coordinator = spill_state.spill_coordinator.write();
            spill_coordinator.wait_spill()?
        };
        self.input_data = Some(data_block);
        if wait {
            self.step = HashJoinBuildStep::WaitSpill;
        } else {
            let mut spill_coordinator = spill_state.spill_coordinator.write();
            // Make `need_spill` to false for `SpillCoordinator`
            spill_coordinator.no_need_spill();
            // Before notify all processors to spill, we need to collect all buffered data in `RowSpace` and `Chunks`
            // Partition all rows and stat how many partitions and rows in each partition.
            // Then choose the largest partitions(which contain rows that can avoid oom exactly) to spill.
            // For each partition, we should equally divide the rows into each processor.
            // Then all processors will spill same partitions.
            let active_processors_num =
                spill_coordinator.total_builder_count - spill_coordinator.non_spill_processors;
            let spill_tasks = &mut spill_coordinator.spill_tasks;
            spill_state.split_spill_tasks(active_processors_num, spill_tasks)?;
            spill_state.notify_spill();
            self.step = HashJoinBuildStep::FirstSpill;
        }
        Ok(())
    }

    // Called after processor read spilled data
    // It means next round build will start, need to reset some variables.
    fn reset(&mut self) -> Result<()> {
        self.finalize_finished = false;
        // Only need to reset the following variables once
        let mut count = self.build_state.row_space_builders.lock();
        if *count == 0 {
            let worker_num = self.build_state.build_worker_num.load(Ordering::Relaxed) as usize;
            *count = worker_num;
            let mut count = self.build_state.hash_join_state.hash_table_builders.lock();
            *count = worker_num;
            let mut row_space_build_done = self.build_state.row_space_build_done.lock();
            *row_space_build_done = false;
            self.build_state.hash_join_state.reset();
        }
        self.step = HashJoinBuildStep::Running;
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
                    if let Some(spill_state) = &self.spill_state && !self.from_spill {
                        // The processor won't be triggered spill, because there won't be data from input port
                        // Add the processor to `non_spill_processors`
                        let mut spill_coordinator = spill_state.spill_coordinator.write();
                        spill_coordinator.non_spill_processors += 1;
                        let waiting_spill_count = spill_coordinator.waiting_spill_count;
                        let non_spill_processors = spill_coordinator.non_spill_processors;
                        if waiting_spill_count != 0 && non_spill_processors + waiting_spill_count == spill_coordinator.total_builder_count {
                            spill_coordinator.no_need_spill();
                            let active_processors_num = spill_coordinator.total_builder_count
                                - non_spill_processors;
                            spill_state.split_spill_tasks(active_processors_num, &mut spill_coordinator.spill_tasks)?;
                            spill_state.notify_spill();
                            spill_coordinator.waiting_spill_count = 0;
                        }
                    }
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
                    if let Some(spill_state) = &mut self.spill_state && !spill_state.spiller.partition_location.is_empty() {
                        // Send spilled partition to `HashJoinState`, used by probe spill.
                        // The method should be called only once.
                        let mut spill_coordinator = spill_state.spill_coordinator.write();
                        if !spill_coordinator.send_partition_set {
                            self.build_state
                                .hash_join_state
                                .set_spilled_partition(&spill_state.spiller.spilled_partition_set);
                            spill_coordinator.send_partition_set = true;
                        }
                        self.step = HashJoinBuildStep::WaitProbe;
                        Ok(Event::Async)
                    } else {
                        self.build_state.build_worker_num.fetch_sub(1, Ordering::SeqCst);
                        Ok(Event::Finished)
                    }
                }
            },
            HashJoinBuildStep::FastReturn | HashJoinBuildStep::Finished => Ok(Event::Finished),
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
                    if self.from_spill {
                        return self.build_state.build(data_block);
                    }
                    if let Some(spill_state) = &mut self.spill_state && !spill_state.spiller.is_all_spilled() {
                        // Check if need to spill
                        let need_spill = spill_state.check_need_spill()?;
                        if need_spill {
                            spill_state.spill_coordinator.write().need_spill()?;
                            self.wait_spill(data_block)?;
                        } else if spill_state.spill_coordinator.read().get_need_spill() {
                                // even if input can fit into memory, but there exists one processor need to spill,
                                // then it needs to wait spill.
                                self.wait_spill(data_block)?;
                            } else {
                                // If the processor had spilled data, we should continue to spill
                                if spill_state.spiller.is_any_spilled() {
                                    self.step = HashJoinBuildStep::FollowSpill;
                                    self.spill_data = Some(data_block);
                                } else {
                                    self.build_state.build(data_block)?;
                                }
                            }
                    } else {
                        if let Some(spill_state) = &mut self.spill_state {
                            if spill_state.spiller.is_all_spilled() {
                                self.step = HashJoinBuildStep::FollowSpill;
                                self.spill_data = Some(data_block);
                                return Ok(());
                            }
                        }
                        self.build_state.build(data_block)?;
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
            | HashJoinBuildStep::WaitProbe
            | HashJoinBuildStep::Finished => unreachable!(),
        }
    }

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
                self.spill_state.as_ref().unwrap().wait_spill_notify().await;
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
                    let spill_state = self.spill_state.as_mut().unwrap();
                    let mut hashes = Vec::with_capacity(data.num_rows());
                    spill_state.get_hashes(&data, &mut hashes)?;
                    let unspilled_data = spill_state.spiller.spill_input(data, &hashes).await?;
                    if !unspilled_data.is_empty() {
                        self.build_state.build(unspilled_data)?;
                    }
                }
                self.step = HashJoinBuildStep::Running;
            }
            HashJoinBuildStep::WaitProbe => {
                self.build_state.hash_join_state.wait_probe_notify().await;
                // Currently, each processor will read its own partition
                // Note: we assume that the partition files will fit into memory
                // later, will introduce multiple level spill or other way to handle this.
                let partition_id = *self.build_state.hash_join_state.partition_id.read();
                // If there is no partition to restore, probe will send `-1` to build
                // Which means it's time to finish.
                if partition_id == -1 {
                    self.step = HashJoinBuildStep::Finished;
                    return Ok(());
                }
                if !self
                    .build_state
                    .hash_join_state
                    .spill_partition
                    .read()
                    .contains(&(partition_id as u8))
                {
                    // Skip if the partition is not spilled in build side
                    self.from_spill = true;
                    self.reset()?;
                    return Ok(());
                }
                let spilled_data = self
                    .spill_state
                    .as_ref()
                    .unwrap()
                    .spiller
                    .read_spilled_data(&(partition_id as u8))
                    .await?;
                self.input_data = Some(DataBlock::concat(&spilled_data)?);
                self.from_spill = true;
                self.reset()?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}
