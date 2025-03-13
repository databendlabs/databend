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
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_sql::plans::JoinType;

use crate::pipelines::processors::transforms::hash_join::HashJoinBuildState;
use crate::pipelines::processors::transforms::hash_join::HashJoinSpiller;
use crate::pipelines::processors::Event;
use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::Processor;

/// There are three types of hash table:
/// 1. FirstRound: it is the first time the hash table is constructed.
/// 2. Restored: the hash table is restored from the spilled data.
/// 3. Empty: the hash table is empty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HashTableType {
    FirstRound,
    Restored,
    Empty,
    UnFinished,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Step {
    Sync(SyncStep),
    Async(AsyncStep),
    Finish,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum SyncStep {
    // Collect data blocks to BuildState.
    Collect,
    // Build the hash table.
    Finalize,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum AsyncStep {
    // Check if spill happens.
    CheckSpillHappen,
    // Wait all collect processors to finish.
    WaitCollect,
    // Spill data blocks.
    Spill,
    // Wait all probe processors to finish.
    WaitProbe,
    // Restore spilled data.
    Restore,
    // Prepare for next round.
    NextRound,
}

pub struct TransformHashJoinBuild {
    input_port: Arc<InputPort>,
    data_blocks: Vec<DataBlock>,
    data_blocks_memory_size: usize,

    build_state: Arc<HashJoinBuildState>,
    hash_table_type: HashTableType,

    // States for various steps.
    // Whether we have checked if spill happens.
    is_spill_happen_checked: bool,
    // Whether spill has happened.
    is_spill_happened: bool,
    // Whether the collect step is finished.
    is_collect_finished: bool,
    // Whether the spilled partitions of this processor are added to the hash join state.
    is_spilled_partitions_added: bool,
    // Whether the finalize step is finished.
    is_finalize_finished: bool,
    // Whether the data blocks are restored from spilled data.
    is_from_restore: bool,

    // Spill related states.
    // The spiller is used to spill/restore data blocks.
    spiller: HashJoinSpiller,
    memory_settings: MemorySettings,

    step: Step,
}

impl TransformHashJoinBuild {
    pub fn try_create(
        input_port: Arc<InputPort>,
        build_state: Arc<HashJoinBuildState>,
    ) -> Result<Box<dyn Processor>> {
        build_state.build_attach();

        // Create a hash join spiller.
        let hash_join_state = build_state.hash_join_state.clone();
        let hash_keys = hash_join_state.hash_join_desc.build_keys.clone();
        let hash_method = build_state.method.clone();
        let spiller = HashJoinSpiller::create(
            build_state.ctx.clone(),
            hash_join_state,
            hash_keys,
            hash_method,
            build_state.hash_join_state.spill_partition_bits,
            build_state.hash_join_state.spill_buffer_threshold,
            true,
        )?;

        let memory_settings = build_state.memory_settings.clone();

        Ok(Box::new(TransformHashJoinBuild {
            input_port,
            data_blocks: vec![],
            data_blocks_memory_size: 0,
            build_state,
            hash_table_type: HashTableType::FirstRound,
            is_spill_happen_checked: false,
            is_spill_happened: false,
            is_collect_finished: false,
            is_spilled_partitions_added: false,
            is_finalize_finished: false,
            is_from_restore: false,
            spiller,
            memory_settings,
            step: Step::Sync(SyncStep::Collect),
        }))
    }

    fn next_step(&mut self, step: Step) -> Result<Event> {
        let event = match step {
            Step::Sync(_) => Event::Sync,
            Step::Async(_) => Event::Async,
            Step::Finish => {
                self.input_port.finish();
                Event::Finished
            }
        };
        self.step = step;
        Ok(event)
    }

    fn collect(&mut self) -> Result<Event> {
        if self.input_port.has_data() {
            self.add_data_block(self.input_port.pull_data().unwrap()?);
            if self.need_spill() {
                return self.next_step(Step::Async(AsyncStep::Spill));
            }
        }

        if self.input_port.is_finished() {
            if self.need_check_spill_happen() {
                self.next_step(Step::Async(AsyncStep::CheckSpillHappen))
            } else if self.need_collect_data_block() {
                self.next_step(Step::Sync(SyncStep::Collect))
            } else {
                self.next_step(Step::Async(AsyncStep::WaitCollect))
            }
        } else {
            self.input_port.set_need_data();
            Ok(Event::NeedData)
        }
    }

    fn finalize(&mut self) -> Result<Event> {
        if self.is_finalize_finished() {
            if self.need_next_round() {
                self.next_step(Step::Async(AsyncStep::WaitProbe))
            } else {
                self.next_step(Step::Finish)
            }
        } else {
            self.next_step(Step::Sync(SyncStep::Finalize))
        }
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
        match &self.step {
            Step::Sync(step) => match step {
                SyncStep::Collect => self.collect(),
                SyncStep::Finalize => self.finalize(),
            },
            Step::Async(step) => {
                match step {
                    AsyncStep::CheckSpillHappen | AsyncStep::Spill | AsyncStep::NextRound => {
                        // Continue to collect data to BuildState.
                        self.collect()
                    }
                    AsyncStep::WaitCollect => {
                        if self.can_fast_return() {
                            self.next_step(Step::Finish)
                        } else {
                            self.finalize()
                        }
                    }
                    AsyncStep::WaitProbe => {
                        if self.need_next_round() {
                            self.next_step(Step::Async(AsyncStep::Restore))
                        } else {
                            self.next_step(Step::Finish)
                        }
                    }
                    AsyncStep::Restore => self.next_step(Step::Async(AsyncStep::NextRound)),
                }
            }
            Step::Finish => self.next_step(Step::Finish),
        }
    }

    fn interrupt(&self) {
        self.build_state.hash_join_state.interrupt()
    }

    fn process(&mut self) -> Result<()> {
        match self.step {
            Step::Sync(SyncStep::Collect) => {
                // If spill happens, we buffer data blocks to SpillBuffer, but if the data blocks
                // are restored from spilled partitions, we can build the hash table directly.
                if self.is_spill_happened() && !self.is_from_restore() {
                    self.spiller.buffer(&self.data_blocks)?;
                } else {
                    for data_block in self.data_blocks.iter() {
                        self.build_state.build(data_block.clone())?;
                    }
                }
                self.data_blocks.clear();
                self.is_collect_finished = true;
                self.build_state.collect_done()
            }
            Step::Sync(SyncStep::Finalize) => {
                if let Some(task) = self.build_state.finalize_task() {
                    self.build_state.finalize(task)
                } else {
                    self.is_finalize_finished = true;
                    self.build_state.finalize_done(self.hash_table_type)
                }
            }
            _ => unreachable!(),
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match &self.step {
            Step::Async(AsyncStep::CheckSpillHappen) => {
                self.build_state.barrier.wait().await;
                self.is_spill_happen_checked = true;
                self.is_spill_happened = self
                    .build_state
                    .hash_join_state
                    .is_spill_happened
                    .load(Ordering::Acquire);
            }
            Step::Async(AsyncStep::WaitCollect) => {
                if !self.is_spilled_partitions_added {
                    let spilled_partitions = self.spiller.spilled_partitions();
                    self.build_state
                        .hash_join_state
                        .add_spilled_partitions(&spilled_partitions);
                    self.is_spilled_partitions_added = true;
                }
                if self.has_unrestored_data() {
                    self.set_need_next_round()
                }
                self.build_state.barrier.wait().await;
            }
            Step::Async(AsyncStep::Spill) => {
                self.spiller.spill(&self.data_blocks, None).await?;
                self.build_state
                    .hash_join_state
                    .is_spill_happened
                    .store(true, Ordering::Release);
                self.data_blocks.clear();
                self.data_blocks_memory_size = 0;
            }
            Step::Async(AsyncStep::WaitProbe) => {
                self.build_state.hash_join_state.wait_probe_notify().await?;
            }
            Step::Async(AsyncStep::Restore) => {
                let partition_id_to_restore = self.partition_to_restore();
                self.data_blocks = self.spiller.restore(partition_id_to_restore).await?;
            }
            Step::Async(AsyncStep::NextRound) => {
                self.reset_build_state()?;
                self.reset_for_next_round();
                self.build_state.barrier.wait().await;
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}

impl TransformHashJoinBuild {
    fn add_data_block(&mut self, data_block: DataBlock) {
        self.data_blocks_memory_size += data_block.memory_size();
        self.data_blocks.push(data_block);
    }

    fn need_check_spill_happen(&self) -> bool {
        !self.is_spill_happen_checked
    }

    fn need_collect_data_block(&self) -> bool {
        !self.is_collect_finished
    }

    fn can_fast_return(&self) -> bool {
        self.build_state
            .hash_join_state
            .fast_return
            .load(Ordering::Acquire)
    }

    fn is_finalize_finished(&self) -> bool {
        self.is_finalize_finished
    }

    fn is_spill_happened(&self) -> bool {
        self.is_spill_happened
    }

    fn partition_to_restore(&self) -> usize {
        self.build_state
            .hash_join_state
            .partition_id
            .load(Ordering::Acquire)
    }

    fn set_need_next_round(&self) {
        self.build_state
            .hash_join_state
            .need_next_round
            .store(true, Ordering::Release);
    }

    fn need_next_round(&self) -> bool {
        self.build_state
            .hash_join_state
            .need_next_round
            .load(Ordering::Acquire)
    }

    fn has_unrestored_data(&self) -> bool {
        if self.build_state.join_type() == JoinType::Cross {
            self.spiller.has_next_restore_file()
        } else {
            !self
                .build_state
                .hash_join_state
                .spilled_partitions
                .read()
                .is_empty()
        }
    }

    fn is_from_restore(&self) -> bool {
        self.is_from_restore
    }

    // Reset variables for the next round build.
    fn reset_for_next_round(&mut self) {
        self.is_from_restore = true;
        self.is_collect_finished = false;
        self.is_finalize_finished = false;
        self.hash_table_type = HashTableType::Restored;
    }

    // Reset build state for the next round.
    fn reset_build_state(&mut self) -> Result<()> {
        // Only need to reset the following variables once
        if self
            .build_state
            .next_round_counter
            .fetch_sub(1, Ordering::AcqRel)
            == 1
        {
            self.build_state
                .hash_join_state
                .need_next_round
                .store(false, Ordering::Release);
            // Before build processors into `WaitProbe` state, set the channel message to false.
            // Then after all probe processors are ready, the last one will send true to channel and wake up all build processors.
            self.build_state
                .hash_join_state
                .continue_build_watcher
                .send(false)
                .map_err(|_| ErrorCode::TokioError("continue_build_watcher channel is closed"))?;
            let worker_num = self.build_state.build_worker_num.load(Ordering::Acquire) as usize;
            self.build_state
                .collect_counter
                .store(worker_num, Ordering::Release);
            self.build_state
                .finalize_counter
                .store(worker_num, Ordering::Release);
            self.build_state
                .next_round_counter
                .store(worker_num, Ordering::Release);
            self.build_state.hash_join_state.reset();
        }
        Ok(())
    }

    fn need_spill(&mut self) -> bool {
        self.memory_settings.check_spill()
    }
}
