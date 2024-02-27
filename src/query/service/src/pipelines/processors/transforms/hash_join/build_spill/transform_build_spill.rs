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

use std::collections::HashSet;
use std::sync::atomic::Ordering;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use log::info;

use crate::pipelines::processors::transforms::hash_join::transform_hash_join_build::HashJoinBuildStep;
use crate::pipelines::processors::transforms::BuildSpillState;

pub struct BuildSpillHandler {
    // The flag indicates whether data is from spilled data.
    from_spill: bool,
    spill_state: Option<Box<BuildSpillState>>,
    spill_data: Option<DataBlock>,
    // If processor has sent partition set to probe
    sent_partition_set: bool,
}

impl BuildSpillHandler {
    pub fn create(spill_state: Option<Box<BuildSpillState>>) -> Self {
        Self {
            from_spill: false,
            spill_state,
            spill_data: None,
            sent_partition_set: false,
        }
    }

    // If spilling is enabled, return true
    pub(crate) fn enabled_spill(&self) -> bool {
        self.spill_state.is_some()
    }

    // Note: the caller should ensure `spill_state` is Some
    pub(crate) fn spill_state(&self) -> &BuildSpillState {
        self.spill_state.as_ref().unwrap()
    }

    pub(crate) fn spill_state_mut(&mut self) -> &mut BuildSpillState {
        self.spill_state.as_mut().unwrap()
    }

    pub(crate) fn from_spill(&self) -> bool {
        self.from_spill
    }

    pub(crate) fn set_from_spill(&mut self, val: bool) {
        self.from_spill = val;
    }

    pub(crate) fn set_spill_data(&mut self, spill_data: DataBlock) {
        self.spill_data = Some(spill_data);
    }

    // Get `spilled_partition_set` from spiller and set `sent_partition_set` to true
    pub(crate) fn spilled_partition_set(&mut self) -> Option<HashSet<u8>> {
        if !self.enabled_spill() {
            return None;
        }
        if !self.sent_partition_set {
            self.sent_partition_set = true;
            return Some(self.spill_state().spiller.spilled_partition_set.clone());
        }
        None
    }

    // Request `spill_coordinator` to spill, it will return two possible steps:
    // 1. WaitSpill
    // 2. Start the first spilling if the processor is the last processor which waits for spilling
    pub(crate) fn request_spill(&mut self) -> Result<HashJoinBuildStep> {
        let spill_state = self.spill_state_mut();
        let wait = spill_state.spill_coordinator.wait_spill()?;
        if wait {
            return Ok(HashJoinBuildStep::WaitSpill);
        }
        // Before notify all processors to spill, we need to collect all buffered data in `RowSpace` and `Chunks`
        // Partition all rows and stat how many partitions and rows in each partition.
        // Then choose the largest partitions(which contain rows that can avoid oom exactly) to spill.
        // Then all processors will spill same partitions.
        let mut spill_tasks = spill_state.spill_coordinator.spill_tasks.lock();
        spill_state.split_spill_tasks(
            spill_state.spill_coordinator.active_processor_num(),
            &mut spill_tasks,
        )?;
        spill_state
            .spill_coordinator
            .ready_spill_watcher
            .send(true)
            .map_err(|_| ErrorCode::TokioError("ready_spill_watcher channel is closed"))?;
        Ok(HashJoinBuildStep::FirstSpill)
    }

    // Check if fit into memory and return next step
    // If step doesn't be changed, return None.
    pub(crate) fn check_memory_and_next_step(&mut self) -> Result<Option<HashJoinBuildStep>> {
        let spill_state = self.spill_state();
        if !self.enabled_spill() || self.from_spill() || spill_state.spiller.is_all_spilled() {
            return Ok(None);
        }
        if spill_state.check_need_spill()? {
            spill_state.spill_coordinator.need_spill()?;
            return Ok(Some(self.request_spill()?));
        } else if spill_state.spill_coordinator.get_need_spill() {
            // even if input can fit into memory, but there exists one processor need to spill,
            // then it needs to wait spill.
            return Ok(Some(self.request_spill()?));
        }
        Ok(None)
    }

    // Check if current processor is the last processor that is responsible for notifying spilling
    pub(crate) fn try_notify_spill(&self) -> Result<()> {
        if !self.enabled_spill() || self.from_spill() {
            return Ok(());
        }
        let spill_state = self.spill_state();
        // The current processor won't be triggered spill, because there isn't data from input port
        // Add the processor to `non_spill_processors`
        let spill_coordinator = &spill_state.spill_coordinator;
        let mut non_spill_processors = spill_coordinator.non_spill_processors.write();
        *non_spill_processors += 1;
        let waiting_spill_count = spill_coordinator
            .waiting_spill_count
            .load(Ordering::Acquire);
        info!(
            "waiting_spill_count: {:?}, non_spill_processors: {:?}, total_builder_count: {:?}",
            waiting_spill_count,
            *non_spill_processors,
            spill_state.spill_coordinator.total_builder_count
        );
        if (waiting_spill_count != 0
            && *non_spill_processors + waiting_spill_count
                == spill_state.spill_coordinator.total_builder_count)
            && spill_coordinator.get_need_spill()
        {
            spill_coordinator.no_need_spill();
            drop(non_spill_processors);
            let mut spill_task = spill_coordinator.spill_tasks.lock();
            spill_state
                .split_spill_tasks(spill_coordinator.active_processor_num(), &mut spill_task)?;
            spill_coordinator
                .waiting_spill_count
                .store(0, Ordering::Relaxed);
            spill_coordinator
                .ready_spill_watcher
                .send(true)
                .map_err(|_| ErrorCode::TokioError("ready_spill_watcher channel is closed"))?;
        }
        Ok(())
    }

    // Wait `spill_coordinator` to send spill notify
    pub(crate) async fn wait_spill_notify(&self) -> Result<()> {
        let spill_state = self.spill_state();
        spill_state.spill_coordinator.wait_spill_notify().await
    }

    pub(crate) async fn spill_buffered_data(&mut self, processor_id: usize) -> Result<()> {
        let spill_state = self.spill_state_mut();
        spill_state.spill(processor_id).await
    }

    // Spill data block and return data that wasn't spilled
    pub(crate) async fn spill(&mut self, processor_id: usize) -> Result<DataBlock> {
        let mut unspilled_data = DataBlock::empty();
        if let Some(block) = self.spill_data.take() {
            let mut hashes = Vec::with_capacity(block.num_rows());
            let spill_state = self.spill_state_mut();
            spill_state.get_hashes(&block, &mut hashes)?;
            let spilled_partition_set = spill_state.spiller.spilled_partition_set.clone();
            unspilled_data = spill_state
                .spiller
                .spill_input(block, &hashes, &spilled_partition_set, processor_id)
                .await?;
        }
        Ok(unspilled_data)
    }

    // Restore
    pub(crate) async fn restore(
        &mut self,
        partition_id: i8,
        processor_id: usize,
    ) -> Result<Option<DataBlock>> {
        let spill_state = self.spill_state();
        if spill_state
            .spiller
            .partition_location
            .contains_key(&(partition_id as u8))
        {
            let spilled_data = spill_state
                .spiller
                .read_spilled_data(&(partition_id as u8), processor_id)
                .await?;
            if !spilled_data.is_empty() {
                return Ok(Some(DataBlock::concat(&spilled_data)?));
            }
        }
        Ok(None)
    }
}
