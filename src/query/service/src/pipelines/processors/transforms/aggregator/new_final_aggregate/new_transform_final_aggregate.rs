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
use std::borrow::BorrowMut;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::PartitionedPayload;
use databend_common_expression::PayloadFlushState;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use log::debug;
use parking_lot::Mutex;
use tokio::sync::Barrier;

use super::new_aggregate_spiller::NewAggregateSpiller;
use super::new_final_aggregate_state::FinalAggregateSharedState;
use super::new_final_aggregate_state::LocalRoundState;
use super::new_final_aggregate_state::RepartitionedQueues;
use super::new_final_aggregate_state::RoundPhase;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatePayload;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;

pub struct NewFinalAggregateTransform {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    id: usize,
    partition_count: usize,

    /// final aggregate
    params: Arc<AggregatorParams>,
    flush_state: PayloadFlushState,

    /// storing repartition result
    repartitioned_queues: RepartitionedQueues,

    /// schedule
    round_state: LocalRoundState,
    barrier: Arc<Barrier>,
    shared_state: Arc<Mutex<FinalAggregateSharedState>>,

    /// spill
    spiller: NewAggregateSpiller,
    agg_hashtable: AggregateHashTable,
}

impl NewFinalAggregateTransform {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        id: usize,
        params: Arc<AggregatorParams>,
        partition_count: usize,
        barrier: Arc<Barrier>,
        shared_state: Arc<Mutex<FinalAggregateSharedState>>,
        spiller: NewAggregateSpiller,
        max_aggregate_spill_level: usize,
    ) -> Result<Box<dyn Processor>> {
        let round_state = LocalRoundState::new(max_aggregate_spill_level);
        let agg_hashtable = AggregateHashTable::new(
            params.group_data_types.clone(),
            params.aggregate_functions.clone(),
            HashTableConfig::default().with_initial_radix_bits(0),
            Arc::new(Bump::new()),
        );
        Ok(Box::new(NewFinalAggregateTransform {
            input,
            output,
            id,
            params,
            flush_state: PayloadFlushState::default(),
            round_state,
            partition_count,
            repartitioned_queues: RepartitionedQueues::create(partition_count),
            barrier,
            shared_state,
            spiller,
            agg_hashtable,
        }))
    }

    /// Repartition the given AggregateMeta into `partition_count` partitions
    /// in aggregate stage, `partition_count` processors will handle each partition respectively.
    fn repartition(&mut self, meta: AggregateMeta) -> Result<()> {
        // Step 1: normalize input into a single Payload to scatter.
        let mut src_payload = match meta {
            // Deserialize into a hashtable with radix_bits = 0. This yields a single payload.
            AggregateMeta::Serialized(payload) => {
                let p = payload.convert_to_partitioned_payload(
                    self.params.group_data_types.clone(),
                    self.params.aggregate_functions.clone(),
                    self.params.num_states(),
                    0,
                    Arc::new(Bump::new()),
                )?;
                debug_assert_eq!(p.partition_count(), 1);
                // Safe to unwrap due to partition_count == 1
                p.payloads.into_iter().next().unwrap()
            }
            // Already a single payload for one upstream bucket.
            AggregateMeta::AggregatePayload(agg_payload) => agg_payload.payload,
            _ => {
                return Err(ErrorCode::Internal(
                    "Unexpected meta type for repartitioning",
                ));
            }
        };

        // Step 2: scatter this payload across all partitions using modulo hashing.
        let arena = src_payload.arena.clone();
        let mut repartitioned = PartitionedPayload::new(
            self.params.group_data_types.clone(),
            self.params.aggregate_functions.clone(),
            self.partition_count as u64,
            vec![arena],
        );

        let mut state = PayloadFlushState::default();
        while src_payload.scatter(&mut state, self.partition_count) {
            for partition_id in 0..self.partition_count {
                let count = state.probe_state.partition_count[partition_id];
                if count == 0 {
                    continue;
                }

                let sel = &state.probe_state.partition_entries[partition_id];
                repartitioned.payloads[partition_id].copy_rows(sel, count, &state.addresses);
            }
        }
        // Avoid double drop of states moved into new payloads.
        src_payload.state_move_out = true;

        // Step 3: enqueue into per-partition queues.
        let mut new_produced = RepartitionedQueues::create(self.partition_count);
        for (partition_id, payload) in repartitioned.payloads.into_iter().enumerate() {
            if payload.len() == 0 {
                continue;
            }
            let meta = AggregateMeta::AggregatePayload(AggregatePayload {
                bucket: partition_id as isize,
                payload,
                max_partition_count: self.partition_count,
            });
            new_produced.push_to_queue(partition_id, meta);
        }

        // if spill already triggered, local repartition queue is all spilled out
        // we only need to spill the new produced repartitioned queues out
        if self.round_state.is_spilled {
            // when no more task, we need to finalize the partition stream
            if self.round_state.working_queue.is_empty() {
                self.spill(new_produced, true)?;
            } else {
                self.spill(new_produced, false)?;
            }
            return Ok(());
        }

        // merge new produced repartitioned queues into local repartitioned queues
        self.repartitioned_queues.merge_queues(new_produced);

        // if the queue is triggered spill and repartition too many times, considering performance affect, we may not
        // continue to trigger spill
        let can_trigger_spill =
            self.round_state.current_queue_spill_round < self.round_state.max_aggregate_spill_level;
        let need_spill = self.spiller.memory_settings.check_spill();

        if !can_trigger_spill {
            if need_spill {
                debug!(
                    "NewFinalAggregateTransform[{}] skip spill after {} rounds",
                    self.id, self.round_state.current_queue_spill_round
                );
            }
            return Ok(());
        }

        if need_spill {
            debug!(
                "NewFinalAggregateTransform[{}] trigger spill due to memory limit, spilled round {}",
                self.id, self.round_state.current_queue_spill_round
            );
            self.shared_state.lock().is_spilled = true;
        }

        // if other processor or itself trigger spill, this processor will need spill its local repartitioned queue out
        if self.shared_state.lock().is_spilled
            && !self.round_state.is_spilled
            && !self.round_state.working_queue.is_empty()
        {
            self.round_state.is_spilled = true;
            let queues = self.repartitioned_queues.take_queues();
            self.spill(queues, false)?;
        }

        Ok(())
    }

    fn push_output(&mut self) -> Result<Event> {
        if let RoundPhase::OutputReady(data_block) = self.round_state.take_phase() {
            self.output.push_data(Ok(data_block));
            Ok(Event::NeedConsume)
        } else {
            Err(ErrorCode::Internal(
                "NewFinalAggregateTransform output called in invalid state",
            ))
        }
    }

    fn final_aggregate(&mut self, mut queue: Vec<AggregateMeta>) -> Result<()> {
        self.agg_hashtable.clear_for_reuse();

        while let Some(meta) = queue.pop() {
            match meta {
                AggregateMeta::Serialized(payload) => {
                    let partitioned = payload.convert_to_partitioned_payload(
                        self.params.group_data_types.clone(),
                        self.params.aggregate_functions.clone(),
                        self.params.num_states(),
                        0,
                        Arc::new(Bump::new()),
                    )?;
                    self.agg_hashtable
                        .combine_payloads(&partitioned, &mut self.flush_state)?;
                }
                AggregateMeta::AggregatePayload(payload) => {
                    self.agg_hashtable
                        .combine_payload(&payload.payload, &mut self.flush_state)?;
                }
                _ => unreachable!(),
            }
        }

        let output_block = if self.agg_hashtable.len() == 0 {
            self.params.empty_result_block()
        } else {
            let mut blocks = vec![];
            self.flush_state.clear();

            loop {
                if self.agg_hashtable.merge_result(&mut self.flush_state)? {
                    let mut entries = self.flush_state.take_aggregate_results();
                    let group_columns = self.flush_state.take_group_columns();
                    entries.extend_from_slice(&group_columns);
                    let num_rows = entries[0].len();
                    blocks.push(DataBlock::new(entries, num_rows));
                } else {
                    break;
                }
            }

            if blocks.is_empty() {
                self.params.empty_result_block()
            } else {
                DataBlock::concat(&blocks)?
            }
        };

        self.agg_hashtable.clear_for_reuse();

        if output_block.is_empty() {
            self.round_state.phase = RoundPhase::Idle;
        } else {
            self.round_state.phase = RoundPhase::OutputReady(output_block);
        }

        Ok(())
    }

    pub fn spill(&mut self, mut queues: RepartitionedQueues, finalize: bool) -> Result<()> {
        for (id, queue) in queues.0.iter_mut().enumerate() {
            while let Some(meta) = queue.pop() {
                match meta {
                    AggregateMeta::AggregatePayload(AggregatePayload { payload, .. }) => {
                        let data_block = payload.aggregate_flush_all()?.consume_convert_to_full();
                        self.spiller.spill(id, data_block)?;
                    }
                    _ => {
                        return Err(ErrorCode::Internal(
                            "NewAggregateSpiller expects AggregatePayload in repartitioned queue",
                        ));
                    }
                }
            }
        }

        if finalize {
            let spilled_payloads = self.spiller.spill_finish()?;
            for payload in spilled_payloads {
                self.repartitioned_queues.push_to_queue(
                    payload.bucket as usize,
                    AggregateMeta::NewBucketSpilled(payload),
                );
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl Processor for NewFinalAggregateTransform {
    fn name(&self) -> String {
        "NewFinalAggregateTransform".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        let round_state = &mut self.round_state;

        if matches!(round_state.phase, RoundPhase::OutputReady(_)) {
            return self.push_output();
        }

        if matches!(round_state.phase, RoundPhase::Aggregate) {
            return Ok(Event::Sync);
        }

        // schedule a task from local working queue first
        if let Some(event) = round_state.schedule_next_task() {
            return Ok(event);
        }

        // no more task in local working queue, means we need report repartitioned queues to shared state
        if !round_state.is_reported && round_state.first_data_ready {
            return Ok(round_state.schedule_async_wait());
        }

        // after reported, try get datablock from shared state
        let next_datablock = self.shared_state.lock().borrow_mut().get_next_datablock();
        if let Some((mut datablock, spill_round)) = next_datablock {
            // begin a new round, reset spilled flag and reported flag
            round_state.reset_for_new_round(spill_round);

            round_state.enqueue_partitioned_meta(&mut datablock)?;

            // schedule next task from working queue, if empty, begin to wait other processors
            if let Some(event) = round_state.schedule_next_task() {
                return Ok(event);
            } else {
                return Ok(round_state.schedule_async_wait());
            }
        }

        // no more work from shared state, try pull data from input
        if self.input.has_data() {
            // begin a new round, reset spilled flag and reported flag
            round_state.reset_for_new_round(0);
            round_state.first_data_ready = true;

            let mut data_block = self.input.pull_data().unwrap()?;
            round_state.enqueue_partitioned_meta(&mut data_block)?;

            // schedule next task from working queue, if empty, begin to wait other processors
            if let Some(event) = round_state.schedule_next_task() {
                return Ok(event);
            } else {
                return Ok(round_state.schedule_async_wait());
            }
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        let phase = self.round_state.take_phase();
        match phase {
            RoundPhase::NewTask(meta) => {
                let meta = match meta {
                    AggregateMeta::NewBucketSpilled(p) => self.spiller.restore(p)?,
                    AggregateMeta::BucketSpilled(p) => self.spiller.restore_legacy(p)?,
                    other => other,
                };
                self.repartition(meta)?;

                Ok(())
            }
            RoundPhase::Aggregate => {
                let queue = self
                    .shared_state
                    .lock()
                    .repartitioned_queues
                    .take_queue(self.id);
                self.final_aggregate(queue)
            }
            _ => Err(ErrorCode::Internal(format!(
                "NewFinalAggregateTransform process called in {} state",
                phase
            ))),
        }
    }

    async fn async_process(&mut self) -> Result<()> {
        let phase = self.round_state.take_phase();
        match phase {
            RoundPhase::AsyncWait => {
                // report local repartitioned queues to shared state
                let queues = self.repartitioned_queues.take_queues();
                self.shared_state.lock().add_repartitioned_queue(queues);

                self.barrier.wait().await;

                // we can only begin aggregate when last round no processor spills
                if !self.shared_state.lock().last_round_is_spilled {
                    self.round_state.phase = RoundPhase::Aggregate;
                }
                Ok(())
            }
            _ => Err(ErrorCode::Internal(
                "NewFinalAggregateTransform async_process called in invalid state",
            ))?,
        }
    }
}
