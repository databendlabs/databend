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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use bumpalo::Bump;
use concurrent_queue::ConcurrentQueue;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::arrow::deserialize_column;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::PartitionedPayload;
use databend_common_expression::PayloadFlushState;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use log::info;
use opendal::Operator;
use tokio::sync::Barrier;
use tokio::sync::Semaphore;

use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregatePayload;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::BucketSpilledPayload;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;

// Shared state across all NewTransformAggregateFinal processors
pub struct SharedRestoreState {
    // Aggregate worker queues: one queue per partition (after repartition)
    // Each processor will work on different partitions (orthogonal)
    aggregate_queues: Vec<ConcurrentQueue<AggregateMeta>>,

    // Restore phase finished flag
    pub bucket_finished: AtomicUsize,

    pub restored_finished: AtomicBool,
    barrier: Barrier,
}

impl SharedRestoreState {
    pub fn new(partition_count: usize) -> Arc<Self> {
        // Create aggregate queues for each partition
        let aggregate_queues = (0..partition_count)
            .map(|_| ConcurrentQueue::unbounded())
            .collect();

        Arc::new(SharedRestoreState {
            aggregate_queues,
            bucket_finished: AtomicUsize::new(0),
            barrier: Barrier::new(partition_count),
            restored_finished: AtomicBool::new(false),
        })
    }
}

// Local state for each processor
#[derive(Debug)]
enum LocalState {
    Idle,
    AsyncReading(BucketSpilledPayload),
    Deserializing(BucketSpilledPayload, Vec<u8>),
    Repartition(AggregateMeta),
    AsyncWait,
    Aggregating,
    OutputReady(DataBlock),
}

// NewTransformAggregateFinal is an experimental final aggregate processor
// It combines spill reader and final aggregate with work-stealing pattern
pub struct NewTransformAggregateFinal {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    operator: Operator,
    semaphore: Arc<Semaphore>,
    params: Arc<AggregatorParams>,

    // Shared state across all processors
    shared_state: Arc<SharedRestoreState>,

    // Number of partitions for repartitioning
    partition_count: usize,

    id: usize,

    // Local state
    state: LocalState,

    // Flush state for final aggregation
    flush_state: PayloadFlushState,

    // aggregated this round
    aggregated: bool,
}

impl NewTransformAggregateFinal {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        operator: Operator,
        semaphore: Arc<Semaphore>,
        params: Arc<AggregatorParams>,
        shared_state: Arc<SharedRestoreState>,
        partition_count: usize,
        id: usize,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(NewTransformAggregateFinal {
            input,
            output,
            operator,
            semaphore,
            params,
            shared_state,
            partition_count,
            id,
            state: LocalState::Idle,
            flush_state: PayloadFlushState::default(),
            aggregated: false,
        })))
    }

    fn try_get_work(&mut self) -> Result<Event> {
        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;

            if let Some(block_meta) = data_block
                .take_meta()
                .and_then(AggregateMeta::downcast_from)
            {
                info!("Aggregate id_{} receive meta {:?}", self.id, block_meta);
                if let AggregateMeta::BucketSpilled(payload) = block_meta {
                    self.state = LocalState::AsyncReading(payload);
                    return Ok(Event::Async);
                };

                if matches!(
                    block_meta,
                    AggregateMeta::AggregatePayload(..) | AggregateMeta::AggregateSpilling(..)
                ) {
                    self.state = LocalState::Repartition(block_meta);
                    return Ok(Event::Sync);
                }

                if let AggregateMeta::Wait = block_meta {
                    info!("Aggregate id_{} receive async wait flag", self.id);
                    self.state = LocalState::AsyncWait;
                    return Ok(Event::Async);
                }

                unreachable!(
                    "Only BucketSpilled and Wait meta is expected from input, we got: {:?}",
                    block_meta
                );
            } else {
                unreachable!("Only Aggregate meta is expected from input");
            }
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }
        info!("id_{} flag need data", self.id);
        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn deserialize(payload: BucketSpilledPayload, data: Vec<u8>) -> AggregateMeta {
        let mut begin = 0;
        let mut columns = Vec::with_capacity(payload.columns_layout.len());

        for column_layout in &payload.columns_layout {
            columns
                .push(deserialize_column(&data[begin..begin + *column_layout as usize]).unwrap());
            begin += *column_layout as usize;
        }

        AggregateMeta::Serialized(
            crate::pipelines::processors::transforms::aggregator::SerializedPayload {
                bucket: payload.bucket,
                data_block: DataBlock::new_from_columns(columns),
                max_partition_count: payload.max_partition_count,
            },
        )
    }

    fn process_deserializing(
        &mut self,
        payload: BucketSpilledPayload,
        data: Vec<u8>,
    ) -> Result<()> {
        let deserialized = Self::deserialize(payload, data);

        self.repartition_and_push(deserialized)?;

        self.state = LocalState::Idle;

        Ok(())
    }

    fn repartition_and_push(&mut self, meta: AggregateMeta) -> Result<()> {
        let mut flush_state = PayloadFlushState::default();

        // Convert to PartitionedPayload
        let single_partition_payload = match meta {
            AggregateMeta::Serialized(payload) => {
                // Convert serialized payload to PartitionedPayload with partition_count=1
                payload.convert_to_partitioned_payload(
                    self.params.group_data_types.clone(),
                    self.params.aggregate_functions.clone(),
                    self.params.num_states(),
                    0, // radix_bits=0 => partition_count=1
                    Arc::new(Bump::new()),
                )?
            }
            AggregateMeta::AggregatePayload(agg_payload) => {
                // Wrap single payload into PartitionedPayload with partition_count=1
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
            AggregateMeta::AggregateSpilling(partitioned_payload) => {
                // Already a PartitionedPayload
                partitioned_payload
            }
            _ => {
                return Err(ErrorCode::Internal(
                    "Unexpected meta type for repartitioning",
                ));
            }
        };

        // Repartition to self.partition_count
        let repartitioned =
            single_partition_payload.repartition(self.partition_count, &mut flush_state);

        // Push each partition to corresponding aggregate queue
        for (partition_id, payload) in repartitioned.payloads.into_iter().enumerate() {
            if payload.len() == 0 {
                continue; // Skip empty partitions
            }

            let meta = AggregateMeta::AggregatePayload(AggregatePayload {
                bucket: partition_id as isize,
                payload,
                max_partition_count: self.partition_count,
            });

            self.shared_state.aggregate_queues[partition_id]
                .push(meta)
                .map_err(|_| ErrorCode::Internal("Failed to push to aggregate queue"))?;
        }

        Ok(())
    }

    fn process_aggregating(&mut self) -> Result<()> {
        // Collect all data from the partition queue
        let mut agg_hashtable: Option<AggregateHashTable> = None;
        info!(
            "Aggregate id_{} start aggregating with len {}",
            self.id,
            self.shared_state.aggregate_queues[self.id].len()
        );

        // Pop all items from aggregate_queues[partition_id]
        while let Ok(meta) = self.shared_state.aggregate_queues[self.id].pop() {
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
                        "Unexpected meta type in aggregate queue",
                    ));
                }
            }
        }

        // Generate output block
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

        // Set state to OutputReady
        self.aggregated = true;
        info!("flag id_{} as aggregated finish", self.id);
        self.state = LocalState::OutputReady(output_block);
        Ok(())
    }

    async fn async_reading(&mut self, payload: BucketSpilledPayload) -> Result<()> {
        let _guard = self.semaphore.acquire().await;
        let instant = Instant::now();
        let data = self
            .operator
            .read_with(&payload.location)
            .range(payload.data_range.clone())
            .await?
            .to_vec();

        // perf
        {
            Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillReadCount, 1);
            Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillReadBytes, data.len());
            Profile::record_usize_profile(
                ProfileStatisticsName::RemoteSpillReadTime,
                instant.elapsed().as_millis() as usize,
            );
        }

        info!(
            "Read aggregate spill {} successfully, elapsed: {:?}",
            &payload.location,
            instant.elapsed()
        );

        self.state = LocalState::Deserializing(payload, data);

        Ok(())
    }
}

#[async_trait::async_trait]
impl Processor for NewTransformAggregateFinal {
    fn name(&self) -> String {
        String::from("NewTransformAggregateFinal")
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
        info!("id_{} current before event {:?}", self.id, self.state);

        match &self.state {
            LocalState::Deserializing(_, _) | LocalState::Aggregating => Ok(Event::Sync),
            LocalState::AsyncReading(_) | LocalState::AsyncWait | LocalState::Repartition(..) => {
                // handled in try_get_work
                unreachable!("Logic error")
            }
            LocalState::OutputReady(_) => {
                let LocalState::OutputReady(data_block) =
                    std::mem::replace(&mut self.state, LocalState::Idle)
                else {
                    unreachable!()
                };
                self.output.push_data(Ok(data_block));
                Ok(Event::NeedConsume)
            }
            LocalState::Idle => self.try_get_work(),
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, LocalState::Idle) {
            LocalState::Deserializing(payload, data) => self.process_deserializing(payload, data),
            LocalState::Repartition(meta) => self.repartition_and_push(meta),
            LocalState::Aggregating => self.process_aggregating(),

            LocalState::Idle
            | LocalState::AsyncReading(_)
            | LocalState::OutputReady(_)
            | LocalState::AsyncWait => {
                unreachable!("Logic error: only Deserializing, Aggregating can enter process");
            }
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        let state = std::mem::replace(&mut self.state, LocalState::Idle);
        match state {
            LocalState::AsyncReading(payload) => self.async_reading(payload).await?,
            LocalState::AsyncWait => {
                info!("Aggregate id_{} enter barrier wait", self.id);
                if self.aggregated {
                    info!("aggregated, let +1");
                    self.shared_state
                        .bucket_finished
                        .fetch_add(1, Ordering::SeqCst);
                }
                // Wait for all processors to finish AsyncReading and Deserializing works
                self.shared_state.barrier.wait().await;
                if self.aggregated {
                    // if aggregated this round, we enter next round
                    self.state = LocalState::Idle;
                } else {
                    self.state = LocalState::Aggregating;
                }
            }
            _ => {
                unreachable!(
                    "Logic error: only AsyncReading and AsyncWait can enter async_process"
                );
            }
        }

        Ok(())
    }
}
