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
use std::fmt::Display;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockPartitionStream;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::PartitionedPayload;
use databend_common_expression::PayloadFlushState;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use parking_lot::Mutex;
use tokio::sync::Barrier;

use super::final_aggregate_spiller::FinalAggregateSpiller;
use super::split_partitioned_meta_into_datablocks;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatePayload;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::sessions::QueryContext;

pub struct RepartitionedQueues(Vec<Vec<AggregateMeta>>);

impl RepartitionedQueues {
    pub fn create(partition_count: usize) -> Self {
        let queues = (0..partition_count).map(|_| Vec::new()).collect();
        Self(queues)
    }

    pub fn take_queues(&mut self) -> Self {
        let partition_count = self.0.len();
        std::mem::replace(self, Self::create(partition_count))
    }

    pub fn take_queue(&mut self, partition_idx: usize) -> Vec<AggregateMeta> {
        std::mem::take(&mut self.0[partition_idx])
    }

    pub fn merge_queues(&mut self, other: Self) {
        for (idx, mut queue) in other.0.into_iter().enumerate() {
            self.0[idx].append(&mut queue);
        }
    }

    pub fn push_to_queue(&mut self, partition_idx: usize, meta: AggregateMeta) {
        self.0[partition_idx].push(meta);
    }
}

pub enum LocalState {
    Idle,
    NewTask(AggregateMeta),
    OutputReady(DataBlock),
    Aggregate,
    AsyncWait,
}

impl Display for LocalState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LocalState::Idle => write!(f, "Idle"),
            LocalState::NewTask(_) => write!(f, "NewTask"),
            LocalState::OutputReady(_) => write!(f, "OutputReady"),
            LocalState::AsyncWait => write!(f, "AsyncWait"),
            LocalState::Aggregate => write!(f, "Aggregate"),
        }
    }
}

pub struct FinalAggregateSharedState {
    is_spilled: bool,
    last_round_is_spilled: bool,
    finished_count: usize,
    repartitioned_queues: RepartitionedQueues,
    next_round: Vec<DataBlock>,
    pending_queues: Vec<Vec<AggregateMeta>>,
    partition_count: usize,
}

impl FinalAggregateSharedState {
    pub fn new(partition_count: usize) -> Self {
        Self {
            is_spilled: false,
            last_round_is_spilled: false,
            finished_count: 0,
            repartitioned_queues: RepartitionedQueues::create(partition_count),
            next_round: Vec::with_capacity(partition_count),
            pending_queues: vec![],
            partition_count,
        }
    }

    pub fn add_repartitioned_queue(&mut self, queues: RepartitionedQueues) {
        self.repartitioned_queues.merge_queues(queues);

        self.finished_count += 1;
        if self.finished_count == self.partition_count {
            self.finished_count = 0;
            self.last_round_is_spilled = self.is_spilled;
            if self.is_spilled {
                self.is_spilled = false;
                // flush all repartitioned queues to pending queues
                let queues = self.repartitioned_queues.take_queues();
                for queue in queues.0.into_iter() {
                    if queue.is_empty() {
                        continue;
                    }
                    self.pending_queues.push(queue);
                }
            }

            // pop a queue and repartition in datablock level
            if let Some(queue) = self.pending_queues.pop() {
                self.next_round =
                    split_partitioned_meta_into_datablocks(0, queue, self.partition_count);
            }
        }
    }

    pub fn get_next_datablock(&mut self) -> Option<DataBlock> {
        self.next_round.pop()
    }
}

pub struct NewFinalAggregateTransform {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    id: usize,
    params: Arc<AggregatorParams>,
    flush_state: PayloadFlushState,
    working_queue: Vec<AggregateMeta>,
    state: LocalState,
    partition_count: usize,
    repartitioned_queues: RepartitionedQueues,
    barrier: Arc<Barrier>,
    shared_state: Arc<Mutex<FinalAggregateSharedState>>,
    first_data_ready: bool,
    spiller: FinalAggregateSpiller,
    block_partition_stream: BlockPartitionStream,
    is_spilled: bool,
    is_reported: bool,
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
        spiller: FinalAggregateSpiller,
        ctx: Arc<QueryContext>,
    ) -> Result<Box<dyn Processor>> {
        let block_bytes = ctx.get_settings().get_max_block_bytes()? as usize;
        let block_partition_stream = BlockPartitionStream::create(0, block_bytes, partition_count);

        Ok(Box::new(NewFinalAggregateTransform {
            input,
            output,
            id,
            params,
            flush_state: PayloadFlushState::default(),
            working_queue: vec![],
            state: LocalState::Idle,
            partition_count,
            repartitioned_queues: RepartitionedQueues::create(partition_count),
            barrier,
            shared_state,
            first_data_ready: false,
            spiller,
            block_partition_stream,
            is_spilled: false,
            is_reported: false,
        }))
    }

    /// Repartition the given AggregateMeta into `partition_count` partitions
    /// in aggregate stage, `partition_count` processors will handle each partition respectively.
    fn repartition(&mut self, meta: AggregateMeta) -> Result<()> {
        let mut flush_state = PayloadFlushState::default();

        let partition_payload = match meta {
            AggregateMeta::Serialized(payload) => payload.convert_to_partitioned_payload(
                self.params.group_data_types.clone(),
                self.params.aggregate_functions.clone(),
                self.params.num_states(),
                0,
                Arc::new(Bump::new()),
            )?,
            AggregateMeta::AggregatePayload(agg_payload) => {
                let payload = agg_payload.payload;
                let arena = payload.arena.clone();
                let mut partitioned = PartitionedPayload::new(
                    self.params.group_data_types.clone(),
                    self.params.aggregate_functions.clone(),
                    1,
                    vec![arena],
                );
                partitioned.combine_single(payload, &mut flush_state, None);
                partitioned
            }
            _ => {
                return Err(ErrorCode::Internal(
                    "Unexpected meta type for repartitioning",
                ));
            }
        };

        let repartitioned = partition_payload.repartition(self.partition_count, &mut flush_state);
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
        if self.is_spilled {
            // when no more task, we need to finalize the partition stream
            if self.working_queue.is_empty() {
                self.spill(new_produced, true)?;
            } else {
                self.spill(new_produced, false)?;
            }
            return Ok(());
        }

        // merge new produced repartitioned queues into local repartitioned queues
        self.repartitioned_queues.merge_queues(new_produced);

        // if self.spiller.memory_settings.check_spill() {
        //     info!(
        //         "NewFinalAggregateTransform[{}] trigger spill due to memory limit",
        //         self.id
        //     );
        //     self.shared_state.lock().is_spilled = true;
        // }

        // if other processor or itself trigger spill, this processor will need spill its local repartitioned queue out
        if self.shared_state.lock().is_spilled && !self.is_spilled && !self.working_queue.is_empty()
        {
            self.is_spilled = true;
            let queues = self.repartitioned_queues.take_queues();
            self.spill(queues, false)?;
        }

        Ok(())
    }

    fn push_output(&mut self) -> Result<Event> {
        if let LocalState::OutputReady(data_block) =
            std::mem::replace(&mut self.state, LocalState::Idle)
        {
            self.output.push_data(Ok(data_block));
            Ok(Event::NeedConsume)
        } else {
            Err(ErrorCode::Internal(
                "NewFinalAggregateTransform output called in invalid state",
            ))
        }
    }

    fn final_aggregate(&mut self, mut queue: Vec<AggregateMeta>) -> Result<()> {
        let total_row_count = queue
            .iter()
            .map(|meta| match meta {
                AggregateMeta::Serialized(payload) => payload.data_block.num_rows(),
                AggregateMeta::AggregatePayload(payload) => payload.payload.len(),
                _ => 0,
            })
            .sum::<usize>();

        let mut agg_hashtable = if total_row_count > 0 {
            let capacity = AggregateHashTable::get_capacity_for_count(total_row_count);
            Some(AggregateHashTable::new_with_capacity(
                self.params.group_data_types.clone(),
                self.params.aggregate_functions.clone(),
                HashTableConfig::default().with_initial_radix_bits(0),
                capacity,
                Arc::new(Bump::new()),
            ))
        } else {
            None
        };

        if let Some(ht) = agg_hashtable.as_mut() {
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
                        ht.combine_payloads(&partitioned, &mut self.flush_state)?;
                    }
                    AggregateMeta::AggregatePayload(payload) => {
                        ht.combine_payload(&payload.payload, &mut self.flush_state)?;
                    }
                    _ => {
                        return Err(ErrorCode::Internal(
                            "Unexpected meta type in aggregate queue when final aggregate",
                        ));
                    }
                }
            }
        }

        let output_block = if let Some(mut ht) = agg_hashtable {
            let mut blocks = vec![];
            self.flush_state.clear();

            loop {
                if ht.merge_result(&mut self.flush_state)? {
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
        } else {
            self.params.empty_result_block()
        };

        if output_block.is_empty() {
            self.state = LocalState::Idle;
        } else {
            self.state = LocalState::OutputReady(output_block);
        }

        Ok(())
    }

    pub fn spill(&mut self, mut queues: RepartitionedQueues, finalize: bool) -> Result<()> {
        let mut ready_blocks: Vec<Vec<DataBlock>> = vec![vec![]; self.partition_count];

        for (id, queue) in queues.0.iter_mut().enumerate() {
            while let Some(meta) = queue.pop() {
                match meta {
                    AggregateMeta::AggregatePayload(AggregatePayload { payload, .. }) => {
                        let data_block = payload.aggregate_flush_all()?;
                        let indices = vec![id as u64; data_block.num_rows()];
                        let blocks = self
                            .block_partition_stream
                            .partition(indices, data_block, true);
                        for (part_id, block) in blocks.into_iter() {
                            ready_blocks[part_id].push(block);
                        }
                    }
                    _ => {
                        return Err(ErrorCode::Internal(
                            "FinalAggregateSpiller expects AggregatePayload in repartitioned queue",
                        ));
                    }
                }
            }
        }

        if finalize {
            for (id, ready_block) in ready_blocks
                .iter_mut()
                .enumerate()
                .take(self.partition_count)
            {
                if let Some(block) = self.block_partition_stream.finalize_partition(id) {
                    ready_block.push(block);
                }
            }
        }

        for (partition_id, blocks) in ready_blocks.into_iter().enumerate() {
            if blocks.is_empty() {
                continue;
            }
            for block in blocks {
                let bucket_spilled_payload = self.spiller.spill(partition_id, block)?;
                self.repartitioned_queues.push_to_queue(
                    partition_id,
                    AggregateMeta::BucketSpilled(bucket_spilled_payload),
                );
            }
        }

        Ok(())
    }

    fn enqueue_partitioned_meta(&mut self, datablock: &mut DataBlock) -> Result<()> {
        if let Some(block_meta) = datablock.take_meta().and_then(AggregateMeta::downcast_from) {
            match block_meta {
                AggregateMeta::Partitioned { data, .. } => {
                    self.working_queue.extend(data);
                }
                _ => {
                    return Err(ErrorCode::Internal(
                        "NewFinalAggregateTransform expects Partitioned AggregateMeta from upstream",
                    ));
                }
            }
        }
        Ok(())
    }

    fn schedule_next_task(&mut self) -> Option<Event> {
        self.working_queue.pop().map(|aggregate_meta| {
            self.state = LocalState::NewTask(aggregate_meta);
            Event::Sync
        })
    }

    fn schedule_async_wait(&mut self) -> Event {
        self.is_reported = true;
        self.state = LocalState::AsyncWait;
        Event::Async
    }

    fn debug_event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        match self.state {
            LocalState::OutputReady(_) => return self.push_output(),
            LocalState::Aggregate => return Ok(Event::Sync),
            _ => {}
        }

        // schedule a task from local working queue first
        if let Some(event) = self.schedule_next_task() {
            return Ok(event);
        }

        // no more task in local working queue, means we need report repartitioned queues to shared state
        if !self.is_reported && self.first_data_ready {
            return Ok(self.schedule_async_wait());
        }

        // after reported, try get datablock from shared state
        let datablock_opt = self.shared_state.lock().borrow_mut().get_next_datablock();
        if let Some(mut datablock) = datablock_opt {
            // begin a new round, reset spilled flag and reported flag
            self.is_spilled = false;
            self.is_reported = false;

            self.enqueue_partitioned_meta(&mut datablock)?;

            // schedule next task from working queue, if empty, begin to wait other processors
            if let Some(event) = self.schedule_next_task() {
                return Ok(event);
            } else {
                return Ok(self.schedule_async_wait());
            }
        }

        // no more work from shared state, try pull data from input
        if self.input.has_data() {
            // begin a new round, reset spilled flag and reported flag
            self.is_spilled = false;
            self.is_reported = false;

            self.first_data_ready = true;

            let mut data_block = self.input.pull_data().unwrap()?;
            self.enqueue_partitioned_meta(&mut data_block)?;

            // schedule next task from working queue, if empty, begin to wait other processors
            if let Some(event) = self.schedule_next_task() {
                return Ok(event);
            } else {
                return Ok(self.schedule_async_wait());
            }
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
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
        let _before_state = self.state.to_string();
        let event = self.debug_event()?;
        let _after_state = self.state.to_string();
        // info!(
        //     "NewFinalAggregateTransform[{}] return event: {:?}, state: {} -> {}",
        //     self.id, event, before_state, after_state
        // );
        Ok(event)
    }

    fn process(&mut self) -> Result<()> {
        let state = std::mem::replace(&mut self.state, LocalState::Idle);
        match state {
            LocalState::NewTask(meta) => {
                let meta = match meta {
                    AggregateMeta::BucketSpilled(p) => self.spiller.restore(p)?,
                    other => other,
                };
                self.repartition(meta)?;

                Ok(())
            }
            LocalState::Aggregate => {
                let queue = self
                    .shared_state
                    .lock()
                    .repartitioned_queues
                    .take_queue(self.id);
                self.final_aggregate(queue)
            }
            _ => Err(ErrorCode::Internal(format!(
                "NewFinalAggregateTransform process called in {} state",
                state
            ))),
        }
    }

    async fn async_process(&mut self) -> Result<()> {
        let state = std::mem::replace(&mut self.state, LocalState::Idle);
        match state {
            LocalState::AsyncWait => {
                // report local repartitioned queues to shared state
                let queues = self.repartitioned_queues.take_queues();
                self.shared_state.lock().add_repartitioned_queue(queues);

                self.barrier.wait().await;

                // we can only begin aggregate when last round no processor spills
                if !self.shared_state.lock().last_round_is_spilled {
                    self.state = LocalState::Aggregate;
                }
                Ok(())
            }
            _ => Err(ErrorCode::Internal(
                "NewFinalAggregateTransform async_process called in invalid state",
            ))?,
        }
    }
}
