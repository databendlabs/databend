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

use std::sync::atomic::Ordering;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline_core::processors::Event;
use log::info;

use crate::pipelines::processors::transforms::hash_join::transform_hash_join_probe::HashJoinProbeStep;
use crate::pipelines::processors::transforms::ProbeSpillState;
use crate::pipelines::processors::transforms::TransformHashJoinProbe;

pub struct SpillHandler {
    // If `spill_state` is `None`, it means spilling isn't enabled.
    spill_state: Option<Box<ProbeSpillState>>,
    // If the processor has finished spill, set it to true.
    spill_done: bool,
}

impl SpillHandler {
    pub fn new(spill_state: Option<Box<ProbeSpillState>>) -> Self {
        Self {
            spill_state,
            spill_done: false,
        }
    }

    pub fn is_spill_enabled(&self) -> bool {
        self.spill_state.is_some()
    }

    pub fn spill_done(&self) -> bool {
        // Only spilling is enabled, `spill done` is meaningful
        // So check if enabling spilling firstly, then check `spill_done`.
        self.spill_state.is_some() && !self.spill_done
    }

    pub fn set_spill_done(&mut self) {
        self.spill_done = true;
    }

    // Note: the caller should ensure `spill_state` is Some
    pub fn spill_state(&self) -> &Box<ProbeSpillState> {
        debug_assert!(self.spill_state.is_some());
        self.spill_state.as_ref().unwrap()
    }
}

/// Spilling related methods for `TransformHashJoinProbe`
impl TransformHashJoinProbe {
    // Restore spilled data from storage
    // If `input_data` isn't empty, go to `Running` step
    pub(crate) fn restore(&mut self) -> Result<Event> {
        debug_assert!(self.input_port.is_finished());
        if !self.input_data.is_empty() {
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
        Ok(Event::Finished)
    }

    // Spill step, it contains two scenarios
    // 1. there is input data, so change event to async
    // 2. the input port has finished and spill finished,
    //    then add spill_partitions to `spill_partition_set` and set `spill_done` to true.
    //    change current step to `WaitBuild`
    pub(crate) fn spill(&mut self) -> Result<Event> {
        if !self.input_data.is_empty() {
            return Ok(Event::Async);
        }
        debug_assert!(self.input_port.is_finished());
        // Add spilled partition ids to `spill_partitions` of `HashJoinProbeState`
        let spilled_partition_set = &self
            .spill_handler
            .spill_state()
            .spiller
            .spilled_partition_set;
        if !spilled_partition_set.is_empty() {
            info!("probe spilled partitions: {:?}", spilled_partition_set);
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
        if (self.join_probe_state.hash_join_state.need_outer_scan()
            || self.join_probe_state.hash_join_state.need_mark_scan())
            && self.join_probe_state.probe_workers.load(Ordering::Relaxed) == 0
        {
            self.join_probe_state
                .probe_workers
                .store(self.join_probe_state.processor_count, Ordering::Relaxed);
        }

        if self
            .join_probe_state
            .final_probe_workers
            .fetch_add(1, Ordering::Acquire)
            == 0
        {
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
        if let Some(data) = self.input_data.pop_front() {
            let spill_state = self.spill_handler.spill_state.as_mut().unwrap();
            let mut hashes = Vec::with_capacity(data.num_rows());
            spill_state.get_hashes(&data, &mut hashes)?;
            // Pass build spilled partition set, we only need to spill data in build spilled partition set
            let build_spilled_partitions = self
                .join_probe_state
                .hash_join_state
                .build_spilled_partitions
                .read()
                .clone();
            let non_matched_data = spill_state
                .spiller
                .spill_input(data, &hashes, &build_spilled_partitions, self.processor_id)
                .await?;
            // Use `non_matched_data` to probe the first round hashtable (if the hashtable isn't empty)
            if !non_matched_data.is_empty()
                && unsafe { &*self.join_probe_state.hash_join_state.build_state.get() }
                    .generation_state
                    .build_num_rows
                    != 0
            {
                self.probe(non_matched_data)?;
            }
        }
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
            .spilled_partition_set
            .contains(&(p_id as u8))
        {
            let spilled_data = spill_state
                .spiller
                .read_spilled_data(&(p_id as u8), self.processor_id)
                .await?;
            if !spilled_data.is_empty() {
                self.input_data.extend(spilled_data);
            }
        }
        self.join_probe_state.restore_barrier.wait().await;
        self.reset().await
    }
}
