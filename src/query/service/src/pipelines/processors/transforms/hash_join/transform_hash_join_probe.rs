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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_sql::optimizer::ColumnSet;
use databend_common_sql::plans::JoinType;

use crate::pipelines::processors::transforms::hash_join::probe_spill::ProbeSpillHandler;
use crate::pipelines::processors::transforms::hash_join::probe_spill::ProbeSpillState;
use crate::pipelines::processors::transforms::hash_join::HashJoinProbeState;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::Event;
use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::OutputPort;
use crate::pipelines::processors::Processor;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum HashJoinProbeStep {
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
    // Restore the spilled data, then go to probe
    Restore,
}

pub struct TransformHashJoinProbe {
    pub(crate) input_port: Arc<InputPort>,
    pub(crate) output_port: Arc<OutputPort>,

    pub(crate) input_data: VecDeque<DataBlock>,
    pub(crate) output_data_blocks: VecDeque<DataBlock>,
    pub(crate) projections: ColumnSet,
    pub(crate) step: HashJoinProbeStep,
    pub(crate) step_logs: Vec<HashJoinProbeStep>,
    pub(crate) join_probe_state: Arc<HashJoinProbeState>,
    pub(crate) probe_state: ProbeState,
    pub(crate) max_block_size: usize,
    pub(crate) outer_scan_finished: bool,
    pub(crate) processor_id: usize,

    pub(crate) spill_handler: ProbeSpillHandler,
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
        let other_predicate = join_probe_state
            .hash_join_state
            .hash_join_desc
            .other_predicate
            .clone();
        let spill_handler = ProbeSpillHandler::new(probe_spill_state);
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
                other_predicate,
            ),
            max_block_size,
            outer_scan_finished: false,
            processor_id: id,
            spill_handler,
        }))
    }

    // Wait build side to finish asynchronously
    // Then go to next step: `FastReturn/Restore/Running.
    async fn async_wait_build(&mut self) -> Result<()> {
        if !self.spill_handler.spill_done() {
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

        if self.check_fast_return() {
            self.step = HashJoinProbeStep::FastReturn;
            self.step_logs.push(HashJoinProbeStep::FastReturn);
            return Ok(());
        }

        if self.spill_handler.spill_done() {
            self.step = HashJoinProbeStep::Restore;
            self.step_logs.push(HashJoinProbeStep::Restore);
            return Ok(());
        }

        self.step = HashJoinProbeStep::Running;
        self.step_logs.push(HashJoinProbeStep::Running);
        Ok(())
    }

    // Wait probe to finish asynchronously
    // Then go to next step: `FastReturn/FinalScan`
    async fn async_wait_probe(&mut self) -> Result<()> {
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
        Ok(())
    }

    // Probe with hashtable
    pub(crate) fn probe(&mut self, block: DataBlock) -> Result<()> {
        self.probe_state.clear();
        let data_blocks = self.join_probe_state.probe(block, &mut self.probe_state)?;
        if !data_blocks.is_empty() {
            self.output_data_blocks.extend(data_blocks);
        }
        Ok(())
    }

    // For right-related join type, such as right/full outer join
    // if rows in build side can't find matched rows in probe side
    // create null blocks for unmatched rows in probe side.
    fn fill_rows(&mut self, task: usize) -> Result<()> {
        let data_blocks = self
            .join_probe_state
            .final_scan(task, &mut self.probe_state)?;
        if !data_blocks.is_empty() {
            self.output_data_blocks.extend(data_blocks);
        }
        Ok(())
    }

    // Check if directly go to fast return
    fn check_fast_return(&mut self) -> bool {
        let mut fast_return = false;
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
                | JoinType::LeftSemi => fast_return = true,
                _ => {}
            }
        }
        fast_return
    }

    // Check if probe side needs to probe
    fn need_spill(&self) -> bool {
        let build_spilled_partitions = self
            .join_probe_state
            .hash_join_state
            .build_spilled_partitions
            .read();
        self.spill_handler.is_spill_enabled() && !build_spilled_partitions.is_empty()
    }
}

// Methods about state transitions, return `Event`
impl TransformHashJoinProbe {
    // WaitBuild
    fn wait_build(&self) -> Result<Event> {
        Ok(Event::Async)
    }

    fn spill(&self) -> Result<Event> {
        Ok(Event::Async)
    }

    // Running
    // When spilling is enabled, the method contains two running paths
    // 1. Before spilling, it will pull data from input port and go to spill
    // 2. After spilling done, it will use restored data to proceed normal probe.
    fn run(&mut self) -> Result<Event> {
        if self.output_port.is_finished() {
            self.input_port.finish();
            if self.join_probe_state.hash_join_state.need_final_scan() {
                return Ok(Event::Async);
            }
            if self.need_spill() {
                return self.next_round();
            }
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
            if self.need_spill() {
                self.input_data.push_back(data);
                return self.set_spill_step();
            }
            // Split data to `block_size` rows per sub block.
            let (sub_blocks, remain_block) = data.split_by_rows(self.max_block_size);
            self.input_data.extend(sub_blocks);
            if let Some(remain) = remain_block {
                self.input_data.push_back(remain);
            }
            return Ok(Event::Sync);
        }

        if !self.input_port.is_finished() {
            self.input_port.set_need_data();
            return Ok(Event::NeedData);
        }

        // Input port is finished, make spilling finished
        if self.need_spill() && !self.spill_handler.spill_done() {
            // For the first round probe hash table, before finishing spilling, we should check if needs final scan.
            if self.join_probe_state.hash_join_state.need_final_scan()
                && self.spill_handler.probe_first_round_hashtable()
            {
                self.join_probe_state.probe_done()?;
                return Ok(Event::Async);
            }
            return self.spill_finished(self.processor_id);
        }

        if self.join_probe_state.hash_join_state.need_final_scan() {
            self.join_probe_state.probe_done()?;
            return Ok(Event::Async);
        }

        if self
            .join_probe_state
            .hash_join_state
            .merge_into_need_target_partial_modified_scan()
        {
            self.join_probe_state
                .probe_merge_into_partial_modified_done()?;
            return Ok(Event::Async);
        }

        if !self.need_spill() {
            self.output_port.finish();
            return Ok(Event::Finished);
        }

        // If spill is enabled, go to next round.
        self.next_round()
    }

    // FinalScan
    fn final_scan(&mut self) -> Result<Event> {
        if self.output_port.is_finished() {
            if self.need_spill() {
                if !self.spill_handler.spill_done() {
                    return self.spill_finished(self.processor_id);
                }
                return self.next_round();
            }
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
                self.input_port.finish();
                if !self.need_spill() {
                    self.output_port.finish();
                    return Ok(Event::Finished);
                }
                if !self.spill_handler.spill_done() {
                    return self.spill_finished(self.processor_id);
                }
                // If spill is enabled, go to next round.
                self.next_round()
            }
        }
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
            HashJoinProbeStep::WaitBuild => self.wait_build(),
            HashJoinProbeStep::FastReturn => {
                self.input_port.finish();
                self.output_port.finish();
                Ok(Event::Finished)
            }
            HashJoinProbeStep::Running => self.run(),
            HashJoinProbeStep::Restore => self.restore(),
            HashJoinProbeStep::FinalScan => self.final_scan(),
            HashJoinProbeStep::Spill => self.spill(),
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
                if self
                    .join_probe_state
                    .hash_join_state
                    .merge_into_need_target_partial_modified_scan()
                {
                    if let Some(item) = self
                        .join_probe_state
                        .final_merge_into_partial_unmodified_scan_task()
                    {
                        self.final_merge_into_partial_unmodified_scan(item)?;
                        return Ok(());
                    }
                } else if let Some(task) = self.join_probe_state.final_scan_task() {
                    self.fill_rows(task)?;
                    return Ok(());
                }
                self.outer_scan_finished = true;
                Ok(())
            }
            HashJoinProbeStep::FastReturn
            | HashJoinProbeStep::WaitBuild
            | HashJoinProbeStep::Spill
            | HashJoinProbeStep::Restore => unreachable!("{:?}", self.step),
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match self.step {
            HashJoinProbeStep::WaitBuild => self.async_wait_build().await,
            HashJoinProbeStep::Running => self.async_wait_probe().await,
            HashJoinProbeStep::Spill => self.spill_action().await,
            HashJoinProbeStep::Restore => self.restore_action().await,
            HashJoinProbeStep::FinalScan | HashJoinProbeStep::FastReturn => unreachable!(),
        }
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
