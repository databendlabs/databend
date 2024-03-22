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
use std::collections::VecDeque;
use std::sync::atomic::Ordering;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_sql::plans::JoinType;
use log::info;

use crate::pipelines::processors::transforms::hash_join::transform_hash_join_probe::HashJoinProbeStep;
use crate::pipelines::processors::transforms::ProbeSpillState;
use crate::pipelines::processors::transforms::TransformHashJoinProbe;

pub struct ProbeSpillHandler {
    // If `spill_state` is `None`, it means spilling isn't enabled.
    spill_state: Option<Box<ProbeSpillState>>,
    // If the processor has finished spill, set it to true.
    spill_done: bool,
    // If needs to probe first round hash table, default is false
    probe_first_round_hashtable: bool,
    // Next restore file index, only used for cross join
    next_restore_file: usize,
    // Save input block from the processor if the input block has zero columns.
    input_blocks: Vec<DataBlock>,
    // The flag indicates whether spill the buffer data
    spill_buffer: bool,
}

impl ProbeSpillHandler {
    pub fn new(spill_state: Option<Box<ProbeSpillState>>) -> Self {
        Self {
            spill_state,
            spill_done: false,
            probe_first_round_hashtable: true,
            next_restore_file: 0,
            input_blocks: vec![],
            spill_buffer: false,
        }
    }

    // Note: the caller should ensure `spill_state` is Some
    fn spill_state(&self) -> &ProbeSpillState {
        debug_assert!(self.spill_state.is_some());
        self.spill_state.as_ref().unwrap()
    }

    fn spill_state_mut(&mut self) -> &mut ProbeSpillState {
        debug_assert!(self.spill_state.is_some());
        self.spill_state.as_mut().unwrap()
    }

    // If the first round hash table isn't empty, after probing it, set `probe_first_round_hashtable` to true.
    pub fn set_probe_first_round_hashtable(&mut self) {
        self.probe_first_round_hashtable = true;
    }

    // The method will be called before finishing spilling.
    // If the join type needs to final scan, need to do final scan for the first round hash join.
    pub fn probe_first_round_hashtable(&self) -> bool {
        self.probe_first_round_hashtable
    }

    pub fn is_spill_enabled(&self) -> bool {
        self.spill_state.is_some()
    }

    pub fn spill_done(&self) -> bool {
        self.spill_done
    }

    pub fn set_spill_done(&mut self) {
        self.spill_done = true;
    }

    pub fn need_spill_buffer(&self) -> bool {
        self.spill_buffer
    }

    pub fn set_need_spill_buffer(&mut self) {
        self.spill_buffer = true;
    }

    pub fn add_partition_loc(&mut self, id: u8, loc: Vec<String>) {
        self.spill_state_mut()
            .spiller
            .partition_location
            .insert(id, loc);
    }

    // Get spilled partitions
    pub fn spilled_partitions(&self) -> HashSet<u8> {
        self.spill_state().spiller.spilled_partitions()
    }

    // Get spilled files
    pub fn spilled_files(&self) -> Vec<String> {
        self.spill_state().spiller.spilled_files()
    }

    // Read spilled partition
    pub async fn read_spilled_partition(&self, p_id: &u8) -> Result<Vec<DataBlock>> {
        self.spill_state()
            .spiller
            .read_spilled_partition(p_id)
            .await
    }

    // Read spilled file
    pub async fn read_spilled_file(&self, file: &str) -> Result<DataBlock> {
        self.spill_state().spiller.read_spilled_file(file).await
    }

    // Get hashes for input data
    pub fn get_hashes(
        &self,
        block: &DataBlock,
        join_type: &JoinType,
        hashes: &mut Vec<u64>,
    ) -> Result<()> {
        self.spill_state().get_hashes(block, join_type, hashes)
    }

    // Spill input data
    pub async fn spill_input(
        &mut self,
        data_block: DataBlock,
        hashes: &[u64],
        left_related_join: bool,
        spilled_partitions: Option<&HashSet<u8>>,
    ) -> Result<Option<DataBlock>> {
        self.spill_state_mut()
            .spiller
            .spill_input(data_block, hashes, left_related_join, spilled_partitions)
            .await
    }

    // Print spill info
    pub fn format_spill_info(&self) -> String {
        self.spill_state().spiller.format_spill_info()
    }

    // Check if spiller buffer is empty
    pub fn empty_buffer(&self) -> bool {
        self.spill_state().spiller.empty_buffer()
    }

    // Spill buffer data
    pub async fn spill_buffer(&mut self) -> Result<()> {
        self.spill_state_mut().spiller.spill_buffer().await
    }
}

/// The following methods only used for cross join
impl ProbeSpillHandler {
    pub fn input_blocks(&self) -> Vec<DataBlock> {
        self.input_blocks.clone()
    }

    pub fn add_input_block(&mut self, data: DataBlock) {
        self.input_blocks.push(data)
    }

    // Directly spill data block
    pub async fn spill_block(&mut self, data: DataBlock) -> Result<()> {
        self.spill_state_mut().spiller.spill_block(data).await?;
        Ok(())
    }

    pub fn next_restore_file(&mut self) -> usize {
        self.next_restore_file += 1;
        self.next_restore_file - 1
    }

    // The method is called after finishing each round
    pub fn reset_next_restore_file(&mut self) {
        self.next_restore_file = 0;
    }

    // The method checks whether the next restore file exists
    // If not, restore finished.
    pub fn has_next_restore_file(&self) -> bool {
        self.next_restore_file < self.spilled_files().len()
    }
}

/// Spilling related methods for `TransformHashJoinProbe`
impl TransformHashJoinProbe {
    // Restore step
    // If `input_data` isn't empty, go to `Running` step
    pub(crate) fn restore(&mut self) -> Result<Event> {
        debug_assert!(self.input_port.is_finished());
        if !self.input_data.is_empty() {
            if self.output_port.is_finished() {
                // If `output_port` is finished, drop `input_data` and go to `async` restore
                self.input_data.clear();
                return Ok(Event::Async);
            }
            self.step = HashJoinProbeStep::Running;
            self.step_logs.push(HashJoinProbeStep::Running);
            Ok(Event::Sync)
        } else {
            // Read spilled data
            Ok(Event::Async)
        }
    }

    // Go to next round hash join if still has the partition in `spill_partitions`
    pub(crate) fn next_round(&mut self) -> Result<Event> {
        // For cross join, before go to next round, check if has restore all spilled file for the current round
        if self.join_probe_state.join_type() == JoinType::Cross {
            if self.spill_handler.has_next_restore_file() {
                self.step = HashJoinProbeStep::Restore;
                return Ok(Event::Async);
            }
            self.spill_handler.reset_next_restore_file();
        }
        // If `spill_partitions` isn't empty, there are still spilled data which need to restore.
        if !self.join_probe_state.spill_partitions.read().is_empty() {
            self.join_probe_state.finish_final_probe()?;
            self.step = HashJoinProbeStep::WaitBuild;
            self.step_logs.push(HashJoinProbeStep::WaitBuild);
            return Ok(Event::Async);
        }
        // Spill is enabled, but `spill_partitions` is empty,
        // so only finish final probe and don't need to change state to `WaitBuild`.
        // The last processor will notify build side to finish.
        self.join_probe_state.finish_final_probe()?;
        self.output_port.finish();
        self.input_port.finish();
        Ok(Event::Finished)
    }

    // Spill step, it contains two scenarios
    // 1. there is input data, so change event to async
    // 2. the input port has finished and spill finished,
    //    then add spill_partitions to `spill_partition_set` and set `spill_done` to true.
    //    change current step to `WaitBuild`
    pub(crate) fn spill_finished(&mut self, processor_id: usize) -> Result<Event> {
        if !self.spill_handler.empty_buffer() {
            self.step = HashJoinProbeStep::Spill;
            self.step_logs.push(HashJoinProbeStep::Spill);
            self.spill_handler.set_need_spill_buffer();
            return Ok(Event::Async);
        }
        self.spill_handler.set_spill_done();
        // Add spilled partition ids to `spill_partitions` of `HashJoinProbeState`
        let spilled_partition_set = &self.spill_handler.spilled_partitions();
        if self.join_probe_state.join_type() != JoinType::Cross {
            info!(
                "Processor: {}, spill info: {}",
                processor_id,
                self.spill_handler.format_spill_info()
            );
        }
        if self.join_probe_state.hash_join_state.need_final_scan() {
            // Assign build spilled partitions to `self.join_probe_state.spill_partitions`
            let mut spill_partitions = self.join_probe_state.spill_partitions.write();
            *spill_partitions = self
                .join_probe_state
                .hash_join_state
                .build_spilled_partitions
                .read()
                .clone();
        } else if !spilled_partition_set.is_empty() {
            let mut spill_partitions = self.join_probe_state.spill_partitions.write();
            spill_partitions.extend(spilled_partition_set);
        }
        self.join_probe_state.finish_spill()?;
        // Wait build side to build hash table
        self.step = HashJoinProbeStep::WaitBuild;
        self.step_logs.push(HashJoinProbeStep::WaitBuild);
        Ok(Event::Async)
    }

    // Change current step to `Spill` step and event to `Async`
    pub(crate) fn set_spill_step(&mut self) -> Result<Event> {
        self.step = HashJoinProbeStep::Spill;
        self.step_logs.push(HashJoinProbeStep::Spill);
        Ok(Event::Async)
    }

    // Reset some states after restoring probe spilled data
    // Then go to normal probe process.
    pub(crate) async fn reset(&mut self) -> Result<()> {
        self.step = HashJoinProbeStep::Running;
        self.step_logs.push(HashJoinProbeStep::Running);
        self.probe_state.reset();
        if self.join_probe_state.hash_join_state.need_final_scan()
            && self.join_probe_state.probe_workers.load(Ordering::Relaxed) == 0
        {
            self.join_probe_state
                .probe_workers
                .store(self.join_probe_state.processor_count, Ordering::Relaxed);
        }
        let old_final_probe_workers = self
            .join_probe_state
            .final_probe_workers
            .fetch_add(1, Ordering::Acquire);
        if old_final_probe_workers == 0 {
            // Before probe processor into `WaitBuild` state, send `1` to channel
            // After all build processors are finished, the last one will send `2` to channel and wake up all probe processors.
            self.join_probe_state
                .hash_join_state
                .build_done_watcher
                .send(1)
                .map_err(|_| ErrorCode::TokioError("build_done_watcher channel is closed"))?;
        }
        self.outer_scan_finished = false;
        self.join_probe_state.restore_barrier.wait().await;
        Ok(())
    }

    // Async spill action
    pub(crate) async fn spill_action(&mut self) -> Result<()> {
        if self.spill_handler.need_spill_buffer() {
            self.spill_handler.spill_buffer().await?;
            self.spill_finished(self.processor_id)?;
            return Ok(());
        }
        // Before spilling, if there is a hash table, probe the hash table first
        self.try_probe_first_round_hashtable(self.input_data.clone())?;
        let left_related_join_type = matches!(
            self.join_probe_state.join_type(),
            JoinType::Left | JoinType::LeftSingle | JoinType::Full
        );
        let mut unmatched_data_blocks = vec![];
        debug_assert!(self.input_data.len() == 1);
        for data in self.input_data.drain(..) {
            if self.join_probe_state.join_type() == JoinType::Cross {
                if data.columns().is_empty() {
                    self.spill_handler.add_input_block(data);
                } else {
                    self.spill_handler.spill_block(data).await?;
                }
                // Add a dummy partition id to indicate spilling has happened.
                self.spill_handler.add_partition_loc(0, vec![]);
                continue;
            }
            let mut hashes = Vec::with_capacity(data.num_rows());
            self.spill_handler.get_hashes(
                &data,
                &self.join_probe_state.join_type(),
                &mut hashes,
            )?;
            // Pass build spilled partition set, we only need to spill data in build spilled partition set
            let build_spilled_partitions = self
                .join_probe_state
                .hash_join_state
                .build_spilled_partitions
                .read()
                .clone();
            let unmatched_data_block = self
                .spill_handler
                .spill_input(
                    data,
                    &hashes,
                    left_related_join_type,
                    Some(&build_spilled_partitions),
                )
                .await?;
            if let Some(unmatched_data_block) = unmatched_data_block {
                unmatched_data_blocks.push(unmatched_data_block);
            }
        }

        if left_related_join_type {
            for unmatched_data_block in unmatched_data_blocks {
                self.probe(unmatched_data_block)?;
            }
        }

        // Back to Running step and try to pull input data
        self.step = HashJoinProbeStep::Running;
        self.step_logs.push(HashJoinProbeStep::Running);
        Ok(())
    }

    // Restore for cross join
    pub(crate) async fn restore_cross_join(&mut self) -> Result<()> {
        // First, check out if there is a hash table, if not, finish hash join.
        if unsafe { &*self.join_probe_state.hash_join_state.build_state.get() }
            .generation_state
            .build_num_rows
            == 0
        {
            return self.finish_cross_join_spill();
        }

        if !self.spill_handler.input_blocks.is_empty() {
            self.input_data.extend(self.spill_handler.input_blocks());
        } else {
            // Restore each spilled file and probe hash table.
            let next_restore_file = self.spill_handler.next_restore_file();
            let spilled_files = self.spill_handler.spilled_files();
            if !spilled_files.is_empty() {
                let spilled_data = self
                    .spill_handler
                    .read_spilled_file(&spilled_files[next_restore_file])
                    .await?;
                debug_assert!(spilled_data.num_rows() <= self.max_block_size);
                self.input_data.push_back(spilled_data);
            }

            if self.spill_handler.has_next_restore_file() {
                self.step = HashJoinProbeStep::Running;
                self.step_logs.push(HashJoinProbeStep::Running);
                return Ok(());
            }
        }

        self.join_probe_state.restore_barrier.wait().await;
        self.reset().await?;
        Ok(())
    }

    // Async restore action
    pub(crate) async fn restore_action(&mut self) -> Result<()> {
        if self.join_probe_state.join_type() == JoinType::Cross {
            return self.restore_cross_join().await;
        }
        let p_id = self
            .join_probe_state
            .hash_join_state
            .partition_id
            .load(Ordering::Relaxed);
        if p_id == -1 {
            self.step = HashJoinProbeStep::FastReturn;
            self.step_logs.push(HashJoinProbeStep::FastReturn);
            return Ok(());
        }
        if self
            .spill_handler
            .spilled_partitions()
            .contains(&(p_id as u8))
        {
            let spilled_data = DataBlock::concat(
                &self
                    .spill_handler
                    .read_spilled_partition(&(p_id as u8))
                    .await?,
            )?;
            if !spilled_data.is_empty() {
                // Split data to `block_size` rows per sub block.
                let (sub_blocks, remain_block) = spilled_data.split_by_rows(self.max_block_size);
                self.input_data.extend(sub_blocks);
                if let Some(remain) = remain_block {
                    self.input_data.push_back(remain);
                }
            }
        }
        self.join_probe_state.restore_barrier.wait().await;
        self.reset().await
    }

    // Try to probe the first round hashtable
    pub(crate) fn try_probe_first_round_hashtable(
        &mut self,
        input_data: VecDeque<DataBlock>,
    ) -> Result<()> {
        if unsafe { &*self.join_probe_state.hash_join_state.build_state.get() }
            .generation_state
            .build_num_rows
            == 0
        {
            return Ok(());
        }

        self.spill_handler.set_probe_first_round_hashtable();
        for data in input_data.iter() {
            let data = data.convert_to_full();
            self.probe(data)?;
        }
        Ok(())
    }

    fn finish_cross_join_spill(&mut self) -> Result<()> {
        self.step = HashJoinProbeStep::FastReturn;
        self.step_logs.push(HashJoinProbeStep::FastReturn);
        self.join_probe_state
            .hash_join_state
            .partition_id
            .store(-1, Ordering::Relaxed);
        self.join_probe_state
            .hash_join_state
            .continue_build_watcher
            .send(true)
            .map_err(|_| ErrorCode::TokioError("continue_build_watcher channel is closed"))?;
        Ok(())
    }
}
