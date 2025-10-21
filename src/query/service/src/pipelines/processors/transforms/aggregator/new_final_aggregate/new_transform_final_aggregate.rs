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
use std::fmt::Display;
use std::sync::Arc;

use crate::pipelines::processors::transforms::aggregator::new_final_aggregate::final_aggregate_state::FinalAggregateSharedState;
use crate::pipelines::processors::transforms::aggregator::new_final_aggregate::final_aggregate_state::FinalAggregateSpiller;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatePayload;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::sessions::QueryContext;
use bumpalo::Bump;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::PartitionedPayload;
use databend_common_expression::PayloadFlushState;
use databend_common_expression::{AggregateHashTable, BlockMetaInfoDowncast, HashTableConfig};
use databend_common_expression::{BlockPartitionStream, DataBlock};
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use parking_lot::Mutex;
use tokio::sync::Barrier;

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

pub struct NewFinalAggregateTransform {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    id: usize,
    params: Arc<AggregatorParams>,
    flush_state: PayloadFlushState,
    working_queue: Vec<AggregateMeta>,
    state: LocalState,
    partition_count: usize,
    repartitioned_queue: Vec<Vec<AggregateMeta>>,
    barrier: Arc<Barrier>,
    shared_state: Arc<Mutex<FinalAggregateSharedState>>,
    data_ready: bool,
    spiller: FinalAggregateSpiller,
    block_partition_stream: BlockPartitionStream,
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
            repartitioned_queue: vec![vec![]; partition_count],
            barrier,
            shared_state,
            data_ready: false,
            spiller,
            block_partition_stream,
        }))
    }

    /// Repartition the given AggregateMeta into `partition_count` partitions
    /// in aggregate stage, `partition_count` processors will handle each partition respectively.
    fn repartition(&self, meta: AggregateMeta) -> Result<()> {
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

        for (partition_id, payload) in repartitioned.payloads.into_iter().enumerate() {
            if payload.len() == 0 {
                continue;
            }

            let meta = AggregateMeta::AggregatePayload(AggregatePayload {
                bucket: partition_id as isize,
                payload,
                max_partition_count: self.partition_count,
            });

            self.repartitioned_queue[partition_id].push(meta);
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
        let mut agg_hashtable: Option<AggregateHashTable> = None;

        while let Ok(meta) = queue.pop() {
            match meta {
                AggregateMeta::Serialized(payload) => match agg_hashtable.as_mut() {
                    Some(ht) => {
                        let payload = payload.convert_to_partitioned_payload(
                            self.params.group_data_types.clone(),
                            self.params.aggregate_functions.clone(),
                            self.params.num_states(),
                            0,
                            Arc::new(Bump::new()),
                        )?;
                        ht.combine_payloads(&payload, &mut self.flush_state)?;
                    }
                    None => {
                        agg_hashtable = Some(payload.convert_to_aggregate_table(
                            self.params.group_data_types.clone(),
                            self.params.aggregate_functions.clone(),
                            self.params.num_states(),
                            0,
                            Arc::new(Bump::new()),
                            true,
                        )?);
                    }
                },
                AggregateMeta::AggregatePayload(payload) => match agg_hashtable.as_mut() {
                    Some(ht) => {
                        ht.combine_payload(&payload.payload, &mut self.flush_state)?;
                    }
                    None => {
                        let capacity =
                            AggregateHashTable::get_capacity_for_count(payload.payload.len());
                        let mut hashtable = AggregateHashTable::new_with_capacity(
                            self.params.group_data_types.clone(),
                            self.params.aggregate_functions.clone(),
                            HashTableConfig::default().with_initial_radix_bits(0),
                            capacity,
                            Arc::new(Bump::new()),
                        );
                        hashtable.combine_payload(&payload.payload, &mut self.flush_state)?;
                        agg_hashtable = Some(hashtable);
                    }
                },
                _ => {
                    return Err(ErrorCode::Internal(
                        "Unexpected meta type in aggregate queue when final aggregate",
                    ));
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

        self.state = LocalState::OutputReady(output_block);
        Ok(())
    }

    pub fn spill(&mut self) -> Result<()> {
        let mut ready_blocks: Vec<Vec<DataBlock>> = vec![vec![]; self.partition_count];

        for (id, queue) in self.repartitioned_queue.iter_mut().enumerate() {
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

        for (partition_id, blocks) in ready_blocks.into_iter().enumerate() {
            if blocks.is_empty() {
                continue;
            }
            for block in blocks {
                let bucket_spilled_payload = self.spiller.spill(partition_id, block)?;
                self.repartitioned_queue[partition_id]
                    .push(AggregateMeta::BucketSpilled(bucket_spilled_payload));
            }
        }

        Ok(())
    }

    fn spill_if_memory_pressure(&mut self) -> Result<()> {
        if self.spiller.is_spilled {}

        if self.spiller.memory_settings.check_spill() {}

        Ok(())
    }
}

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

        match self.state {
            LocalState::OutputReady(_) => return self.push_output(),
            LocalState::Aggregate => return Ok(Event::Sync),
            _ => {}
        }

        if self.working_queue.is_empty() && self.data_ready {
            self.data_ready = false;
            self.state = LocalState::AsyncWait;
            return Ok(Event::Async);
        }

        if let Some(aggregate_meta) = self.working_queue.pop() {
            self.state = LocalState::NewTask(aggregate_meta);
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;

            if let Some(block_meta) = data_block
                .take_meta()
                .and_then(AggregateMeta::downcast_from)
            {
                self.data_ready = true;
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

            if let Some(aggregate_meta) = self.working_queue.pop() {
                self.state = LocalState::NewTask(aggregate_meta);
                return Ok(Event::Sync);
            }
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        let state = std::mem::replace(&mut self.state, LocalState::Idle);
        match state {
            LocalState::NewTask(meta) => {
                let meta = match meta {
                    AggregateMeta::BucketSpilled(p) => self.spiller.restore(p)?,
                    other => other,
                };
                self.repartition(meta)
            }
            LocalState::Aggregate => {
                let queue = self.shared_state.lock().take_aggregate_queue(self.id);
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
                let queues = self
                    .repartitioned_queue
                    .drain(..)
                    .map(|queue| queue)
                    .collect::<Vec<_>>();
                self.shared_state.lock().merge_aggregate_queues(queues);
                self.barrier.wait().await;
                self.state = LocalState::Aggregate;
                Ok(())
            }
            _ => Err(ErrorCode::Internal(
                "NewFinalAggregateTransform async_process called in invalid state",
            ))?,
        }
    }
}
