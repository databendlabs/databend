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
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_sql::optimizer::ColumnSet;
use databend_common_sql::plans::JoinType;
use log::info;

use crate::pipelines::processors::transforms::hash_join::probe_spill::ProbeSpillState;
use crate::pipelines::processors::transforms::hash_join::HashJoinProbeState;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::Event;
use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::OutputPort;
use crate::pipelines::processors::Processor;

#[derive(Debug, Clone)]
enum HashJoinProbeStep {
    // The step is to wait build phase finished.
    WaitBuild,
    // The running step of the probe phase.
    Running,
    // The final scan step is used to fill missing rows for non-inner join.
    FinalScan,
    // The fast return step indicates we can directly finish the probe phase.
    FastReturn,
    // Spill step is used to spill the probe side data.
    Spill,
    // Async running will read the spilled data, then go to probe
    AsyncRunning,
}

pub struct TransformHashJoinProbe {
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,

    input_data: VecDeque<DataBlock>,
    output_data_blocks: VecDeque<DataBlock>,
    projections: ColumnSet,
    step: HashJoinProbeStep,
    step_logs: Vec<HashJoinProbeStep>,
    join_probe_state: Arc<HashJoinProbeState>,
    probe_state: ProbeState,
    max_block_size: usize,
    outer_scan_finished: bool,
    processor_id: usize,

    // If the processor has finished spill, set it to true.
    spill_done: bool,
    spill_state: Option<Box<ProbeSpillState>>,
    // If input data can't find proper partitions to spill,
    // directly probe them with hashtable.
    need_spill: bool,
}

impl TransformHashJoinProbe {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        projections: ColumnSet,
        join_probe_state: Arc<HashJoinProbeState>,
        probe_spill_state: Option<Box<ProbeSpillState>>,
        max_block_size: usize,
        func_ctx: FunctionContext,
        join_type: &JoinType,
        with_conjunct: bool,
        has_string_column: bool,
    ) -> Result<Box<dyn Processor>> {
        let id = join_probe_state.probe_attach()?;
        Ok(Box::new(TransformHashJoinProbe {
            input_port,
            output_port,
            projections,
            input_data: VecDeque::new(),
            output_data_blocks: VecDeque::new(),
            step: HashJoinProbeStep::WaitBuild,
            step_logs: vec![HashJoinProbeStep::WaitBuild],
            join_probe_state,
            probe_state: ProbeState::create(
                max_block_size,
                join_type,
                with_conjunct,
                has_string_column,
                func_ctx,
            ),
            max_block_size,
            outer_scan_finished: false,
            spill_done: false,
            spill_state: probe_spill_state,
            processor_id: id,
            need_spill: true,
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

    fn async_run(&mut self) -> Result<Event> {
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

    fn run(&mut self) -> Result<Event> {
        if self.output_port.is_finished() {
            self.input_port.finish();
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
            if self.spill_state.is_some() {
                self.need_spill = true;
                self.step = HashJoinProbeStep::Spill;
                self.step_logs.push(HashJoinProbeStep::Spill);
                return Ok(Event::Async);
            }
            return Ok(Event::Sync);
        }

        if self.spill_state.is_some() && !self.spill_done {
            self.need_spill = true;
            self.step = HashJoinProbeStep::Spill;
            self.step_logs.push(HashJoinProbeStep::Spill);
            return Ok(Event::Async);
        }

        if self.input_port.is_finished() {
            return if self.join_probe_state.hash_join_state.need_outer_scan()
                || self.join_probe_state.hash_join_state.need_mark_scan()
            {
                self.join_probe_state.probe_done()?;
                Ok(Event::Async)
            } else {
                if !self.join_probe_state.spill_partitions.read().is_empty() {
                    self.join_probe_state.finish_final_probe()?;
                    self.step = HashJoinProbeStep::WaitBuild;
                    self.step_logs.push(HashJoinProbeStep::WaitBuild);
                    return Ok(Event::Async);
                }
                if self
                    .join_probe_state
                    .ctx
                    .get_settings()
                    .get_join_spilling_threshold()?
                    != 0
                {
                    self.join_probe_state.finish_final_probe()?;
                }
                self.output_port.finish();
                Ok(Event::Finished)
            };
        }
        self.input_port.set_need_data();
        Ok(Event::NeedData)
    }

    async fn reset(&mut self) -> Result<()> {
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
            HashJoinProbeStep::Spill => {
                if !self.input_data.is_empty() {
                    if !self.need_spill {
                        self.step = HashJoinProbeStep::Running;
                        self.step_logs.push(HashJoinProbeStep::Running);
                        return Ok(Event::Sync);
                    }
                    return Ok(Event::Async);
                }

                if self.input_port.has_data() {
                    let data = self.input_port.pull_data().unwrap()?;
                    self.input_data.push_back(data);
                    return Ok(Event::Async);
                }

                if self.input_port.is_finished() {
                    // Add spilled partition ids to `spill_partitions` of `HashJoinProbeState`
                    let spilled_partition_set = &self
                        .spill_state
                        .as_ref()
                        .unwrap()
                        .spiller
                        .spilled_partition_set;
                    if !spilled_partition_set.is_empty() {
                        info!("probe spilled partitions: {:?}", spilled_partition_set);
                        let mut spill_partitions = self.join_probe_state.spill_partitions.write();
                        spill_partitions.extend(spilled_partition_set);
                    }

                    self.spill_done = true;
                    self.join_probe_state.finish_spill()?;
                    // Wait build side to build hash table
                    self.step = HashJoinProbeStep::WaitBuild;
                    self.step_logs.push(HashJoinProbeStep::WaitBuild);
                    return Ok(Event::Async);
                }
                self.input_port.set_need_data();
                Ok(Event::NeedData)
            }
            HashJoinProbeStep::FastReturn => {
                self.input_port.finish();
                self.output_port.finish();
                Ok(Event::Finished)
            }
            HashJoinProbeStep::Running => self.run(),
            HashJoinProbeStep::AsyncRunning => self.async_run(),
            HashJoinProbeStep::FinalScan => {
                if self.output_port.is_finished() {
                    self.input_port.finish();
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
                        if !self.join_probe_state.spill_partitions.read().is_empty() {
                            self.join_probe_state.finish_final_probe()?;
                            self.step = HashJoinProbeStep::WaitBuild;
                            self.step_logs.push(HashJoinProbeStep::WaitBuild);
                            return Ok(Event::Async);
                        }
                        if self
                            .join_probe_state
                            .ctx
                            .get_settings()
                            .get_join_spilling_threshold()?
                            != 0
                        {
                            self.join_probe_state.finish_final_probe()?;
                        }
                        self.input_port.finish();
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
            HashJoinProbeStep::FastReturn
            | HashJoinProbeStep::WaitBuild
            | HashJoinProbeStep::Spill
            | HashJoinProbeStep::AsyncRunning => unreachable!("{:?}", self.step),
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match self.step {
            HashJoinProbeStep::WaitBuild => {
                if !self.spill_done {
                    self.join_probe_state
                        .hash_join_state
                        .wait_first_round_build_done()
                        .await?;
                } else {
                    self.join_probe_state
                        .hash_join_state
                        .wait_build_finish()
                        .await?;
                }

                let join_type = self
                    .join_probe_state
                    .hash_join_state
                    .hash_join_desc
                    .join_type
                    .clone();
                if self
                    .join_probe_state
                    .hash_join_state
                    .fast_return
                    .load(Ordering::Relaxed)
                {
                    match join_type {
                        JoinType::Inner
                        | JoinType::Cross
                        | JoinType::Right
                        | JoinType::RightSingle
                        | JoinType::RightAnti
                        | JoinType::RightSemi
                        | JoinType::LeftSemi => {
                            self.step = HashJoinProbeStep::FastReturn;
                            self.step_logs.push(HashJoinProbeStep::FastReturn);
                        }
                        JoinType::Left
                        | JoinType::Full
                        | JoinType::LeftSingle
                        | JoinType::LeftAnti => {
                            self.step = HashJoinProbeStep::Running;
                            self.step_logs.push(HashJoinProbeStep::Running);
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
                if self
                    .join_probe_state
                    .ctx
                    .get_settings()
                    .get_join_spilling_threshold()?
                    != 0
                {
                    if !self.spill_done {
                        self.step = HashJoinProbeStep::Spill;
                        self.step_logs.push(HashJoinProbeStep::Spill);
                    } else {
                        self.step = HashJoinProbeStep::AsyncRunning;
                        self.step_logs.push(HashJoinProbeStep::AsyncRunning);
                    }
                } else {
                    self.step = HashJoinProbeStep::Running;
                    self.step_logs.push(HashJoinProbeStep::Running);
                }
            }
            HashJoinProbeStep::Running => {
                self.join_probe_state
                    .barrier_count
                    .fetch_add(1, Ordering::SeqCst);
                self.join_probe_state.barrier.wait().await;
                if self
                    .join_probe_state
                    .hash_join_state
                    .fast_return
                    .load(Ordering::Relaxed)
                {
                    self.step = HashJoinProbeStep::FastReturn;
                    self.step_logs.push(HashJoinProbeStep::FastReturn);
                } else {
                    self.step = HashJoinProbeStep::FinalScan;
                    self.step_logs.push(HashJoinProbeStep::FinalScan);
                }
            }
            HashJoinProbeStep::Spill => {
                if let Some(data) = self.input_data.pop_front() {
                    let spill_state = self.spill_state.as_mut().unwrap();
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
                        self.input_data.push_back(non_matched_data);
                        self.need_spill = false;
                    }
                }
            }
            HashJoinProbeStep::AsyncRunning => {
                let spill_state = self.spill_state.as_ref().unwrap();
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
                self.reset().await?;
            }
            HashJoinProbeStep::FinalScan | HashJoinProbeStep::FastReturn => unreachable!(),
        };
        Ok(())
    }

    fn details_status(&self) -> Option<String> {
        #[derive(Debug)]
        #[allow(dead_code)]
        struct Display {
            begin_barrier_count: usize,
            step_logs: Vec<HashJoinProbeStep>,
        }

        Some(format!("{:?}", Display {
            step_logs: self.step_logs.clone(),
            begin_barrier_count: self.join_probe_state.barrier_count.load(Ordering::SeqCst),
        }))
    }
}
