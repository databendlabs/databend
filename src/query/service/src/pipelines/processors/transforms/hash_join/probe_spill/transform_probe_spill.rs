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
}

impl ProbeSpillHandler {
    pub fn new(spill_state: Option<Box<ProbeSpillState>>) -> Self {
        Self {
            spill_state,
            spill_done: false,
            probe_first_round_hashtable: true,
        }
    }

    pub fn set_probe_first_round_hashtable(&mut self, probe_first_round_hashtable: bool) {
        self.probe_first_round_hashtable = probe_first_round_hashtable;
    }

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

    // Note: the caller should ensure `spill_state` is Some
    pub fn spill_state(&self) -> &ProbeSpillState {
        debug_assert!(self.spill_state.is_some());
        self.spill_state.as_ref().unwrap()
    }

    pub fn spill_state_mut(&mut self) -> &mut ProbeSpillState {
        debug_assert!(self.spill_state.is_some());
        self.spill_state.as_mut().unwrap()
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
        // Add spilled partition ids to `spill_partitions` of `HashJoinProbeState`
        let spilled_partition_set = &self
            .spill_handler
            .spill_state()
            .spiller
            .spilled_partitions();
        info!(
            "probe processor-{:?}: spill finished with spilled partitions {:?}",
            processor_id, spilled_partition_set
        );
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

        self.spill_handler.set_spill_done();
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
        // Before spilling, if there is a hash table, probe the hash table first
        self.try_probe_first_round_hashtable(self.input_data.clone())?;
        let left_related_join_type = matches!(
            self.join_probe_state.join_type(),
            JoinType::Left | JoinType::LeftSingle | JoinType::Full
        );
        let mut unmatched_data_blocks = vec![];
        for data in self.input_data.drain(..) {
            let spill_state = self.spill_handler.spill_state_mut();
            let mut hashes = Vec::with_capacity(data.num_rows());
            spill_state.get_hashes(&data, &self.join_probe_state.join_type(), &mut hashes)?;
            // Pass build spilled partition set, we only need to spill data in build spilled partition set
            let build_spilled_partitions = self
                .join_probe_state
                .hash_join_state
                .build_spilled_partitions
                .read()
                .clone();
            let unmatched_data_block = spill_state
                .spiller
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

    // Async restore action
    pub(crate) async fn restore_action(&mut self) -> Result<()> {
        let spill_state = self.spill_handler.spill_state();
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
        if spill_state
            .spiller
            .spilled_partitions()
            .contains(&(p_id as u8))
        {
            let spilled_data =
                DataBlock::concat(&spill_state.spiller.read_spilled_data(&(p_id as u8)).await?)?;
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

        self.spill_handler.set_probe_first_round_hashtable(true);
        for data in input_data.iter() {
            let data = data.convert_to_full();
            self.probe(data)?;
        }
        Ok(())
    }
}
