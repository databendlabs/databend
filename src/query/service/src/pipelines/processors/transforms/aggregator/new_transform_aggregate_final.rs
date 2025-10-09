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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

use bumpalo::Bump;
use concurrent_queue::ConcurrentQueue;
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
use opendal::Operator;
use tokio::sync::Semaphore;

use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregatePayload;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::BucketSpilledPayload;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;

// Metadata structure extracted from TransformPartitionBucket output
#[derive(Debug)]
struct PartitionedMeta {
    bucket: isize,
    data: Vec<AggregateMeta>,
}

// Shared state across all NewTransformAggregateFinal processors
pub struct SharedRestoreState {
    // Ready queue: stores complete Partitioned meta pulled from input (with Mutex)
    ready_queue: Mutex<VecDeque<PartitionedMeta>>,

    // Work queue: stores BucketSpilled tasks to be processed (lock-free)
    work_queue: ConcurrentQueue<BucketSpilledPayload>,

    // Number of tasks currently being processed
    active_tasks: AtomicUsize,

    // Aggregate worker queues: one queue per partition (after repartition)
    // Each processor will work on different partitions (orthogonal)
    aggregate_queues: Vec<ConcurrentQueue<AggregateMeta>>,

    // Total number of spilled tasks expected
    total_tasks: AtomicUsize,

    // Number of completed tasks
    completed_tasks: AtomicUsize,

    // Input finished flag
    input_finished: AtomicBool,

    // Restore phase finished flag
    restore_finished: AtomicBool,

    // Next partition to be assigned for aggregation (phase 2)
    next_partition: AtomicUsize,
}

impl SharedRestoreState {
    pub fn new(partition_count: usize) -> Arc<Self> {
        // Create aggregate queues for each partition
        let aggregate_queues = (0..partition_count)
            .map(|_| ConcurrentQueue::unbounded())
            .collect();

        Arc::new(SharedRestoreState {
            ready_queue: Mutex::new(VecDeque::new()),
            work_queue: ConcurrentQueue::unbounded(),
            active_tasks: AtomicUsize::new(0),
            aggregate_queues,
            total_tasks: AtomicUsize::new(0),
            completed_tasks: AtomicUsize::new(0),
            input_finished: AtomicBool::new(false),
            restore_finished: AtomicBool::new(false),
            next_partition: AtomicUsize::new(0),
        })
    }
}

// Local state for each processor
enum LocalState {
    Idle,
    DispatchingTasks(PartitionedMeta),
    Reading(BucketSpilledPayload),
    Deserializing(BucketSpilledPayload, Vec<u8>),
    Aggregating(usize), // partition_id
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

    // Local state
    state: LocalState,

    // Flush state for final aggregation
    flush_state: PayloadFlushState,
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
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(NewTransformAggregateFinal {
            input,
            output,
            operator,
            semaphore,
            params,
            shared_state,
            partition_count,
            state: LocalState::Idle,
            flush_state: PayloadFlushState::default(),
        })))
    }

    fn try_get_work(&mut self) -> Result<Event> {
        // Try to steal task from work queue
        if let Ok(task) = self.shared_state.work_queue.pop() {
            self.shared_state
                .active_tasks
                .fetch_add(1, Ordering::SeqCst);
            self.state = LocalState::Reading(task);
            return Ok(Event::Async);
        }

        // Work queue is empty, check if we need to dispatch from ready queue
        if self.shared_state.active_tasks.load(Ordering::SeqCst) == 0 {
            // Try to get a meta from ready queue
            let mut ready_queue = self.shared_state.ready_queue.lock().unwrap();

            if let Some(partitioned_meta) = ready_queue.pop_front() {
                drop(ready_queue); // Release lock early
                self.state = LocalState::DispatchingTasks(partitioned_meta);
                return Ok(Event::Sync);
            }

            drop(ready_queue);

            // Ready queue is also empty, try to pull from input
            if self.input.has_data() {
                let mut data_block = self.input.pull_data().unwrap()?;
                let meta = data_block
                    .take_meta()
                    .and_then(AggregateMeta::downcast_from);

                match meta {
                    Some(AggregateMeta::Partitioned { bucket, data }) => {
                        // Normal case: Partitioned meta
                        let mut ready_queue = self.shared_state.ready_queue.lock().unwrap();
                        ready_queue.push_back(PartitionedMeta { bucket, data });
                        drop(ready_queue);

                        if self.input.is_finished() {
                            self.shared_state
                                .input_finished
                                .store(true, Ordering::SeqCst);
                        }
                        return self.try_get_work();
                    }
                    Some(AggregateMeta::BucketSpilled(payload)) => {
                        // Direct BucketSpilled meta (single spilled bucket)
                        // Increment total tasks
                        self.shared_state.total_tasks.fetch_add(1, Ordering::SeqCst);

                        // Push to work queue
                        self.shared_state.work_queue.push(payload).map_err(|_| {
                            ErrorCode::Internal("Failed to push task to work queue")
                        })?;

                        if self.input.is_finished() {
                            self.shared_state
                                .input_finished
                                .store(true, Ordering::SeqCst);
                        }
                        return self.try_get_work();
                    }
                    _ => {
                        return Err(ErrorCode::Internal(
                            "Expected Partitioned or BucketSpilled meta from upstream",
                        ));
                    }
                }
            }

            if self.input.is_finished() {
                self.shared_state
                    .input_finished
                    .store(true, Ordering::SeqCst);
            }

            if self.input.is_finished() && self.shared_state.input_finished.load(Ordering::SeqCst) {
                // Phase 1 (restore) finished, check if we can start phase 2 (aggregate)
                if !self.shared_state.restore_finished.load(Ordering::SeqCst) {
                    let total = self.shared_state.total_tasks.load(Ordering::SeqCst);
                    let completed = self.shared_state.completed_tasks.load(Ordering::SeqCst);

                    if completed == total {
                        self.shared_state
                            .restore_finished
                            .store(true, Ordering::SeqCst);
                    } else {
                        return Ok(Event::NeedData);
                    }
                }

                if self.shared_state.restore_finished.load(Ordering::SeqCst) {
                    // Try to get a partition to aggregate
                    let partition_id = self
                        .shared_state
                        .next_partition
                        .fetch_add(1, Ordering::SeqCst);

                    if partition_id < self.partition_count {
                        self.state = LocalState::Aggregating(partition_id);
                        return Ok(Event::Sync);
                    }

                    // All partitions have been assigned
                    self.output.finish();
                    return Ok(Event::Finished);
                }

                return Ok(Event::NeedData);
            }

            self.input.set_need_data();
            return Ok(Event::NeedData);
        }

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

    fn process_dispatching_tasks(&mut self, partitioned_meta: PartitionedMeta) -> Result<()> {
        // Dispatch tasks to work queue
        let PartitionedMeta { data, .. } = partitioned_meta;

        for item in data {
            match item {
                AggregateMeta::BucketSpilled(payload) => {
                    // Increment total tasks
                    self.shared_state.total_tasks.fetch_add(1, Ordering::SeqCst);

                    // Push to work queue
                    self.shared_state
                        .work_queue
                        .push(payload)
                        .map_err(|_| ErrorCode::Internal("Failed to push task to work queue"))?;
                }
                other => {
                    // Directly processed tasks should also be tracked
                    self.shared_state.total_tasks.fetch_add(1, Ordering::SeqCst);

                    // Non-spilled data: directly repartition and push to aggregate queues
                    self.repartition_and_push(other)?;
                }
            }
        }

        self.state = LocalState::Idle;
        Ok(())
    }

    fn process_deserializing(
        &mut self,
        payload: BucketSpilledPayload,
        data: Vec<u8>,
    ) -> Result<()> {
        // Deserialize the data
        let deserialized = Self::deserialize(payload, data);

        // Decrement active tasks
        self.shared_state
            .active_tasks
            .fetch_sub(1, Ordering::SeqCst);

        // Repartition and push to aggregate queues directly
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
                let mut payload = agg_payload.payload;
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

        // Increment completed tasks
        let completed = self
            .shared_state
            .completed_tasks
            .fetch_add(1, Ordering::SeqCst)
            + 1;
        let total = self.shared_state.total_tasks.load(Ordering::SeqCst);

        // Check if all restore tasks are completed
        if completed == total && self.shared_state.input_finished.load(Ordering::SeqCst) {
            self.shared_state
                .restore_finished
                .store(true, Ordering::SeqCst);
        }

        Ok(())
    }

    fn process_aggregating(&mut self, partition_id: usize) -> Result<()> {
        // Collect all data from the partition queue
        let mut agg_hashtable: Option<AggregateHashTable> = None;

        // Pop all items from aggregate_queues[partition_id]
        while let Ok(meta) = self.shared_state.aggregate_queues[partition_id].pop() {
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
        self.state = LocalState::OutputReady(output_block);
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

        // Priority 1: Output ready
        if matches!(&self.state, LocalState::OutputReady(_)) {
            if !self.output.can_push() {
                return Ok(Event::NeedConsume);
            }

            if let LocalState::OutputReady(data_block) =
                std::mem::replace(&mut self.state, LocalState::Idle)
            {
                self.output.push_data(Ok(data_block));
            }

            return Ok(Event::NeedData);
        }

        // Priority 2: Async reading
        if matches!(&self.state, LocalState::Reading(_)) {
            return Ok(Event::Async);
        }

        // Priority 3: Sync processing (dispatching, deserializing, aggregating)
        if matches!(
            &self.state,
            LocalState::DispatchingTasks(_)
                | LocalState::Deserializing(_, _)
                | LocalState::Aggregating(_)
        ) {
            return Ok(Event::Sync);
        }

        // Priority 4: Try to get work
        if matches!(&self.state, LocalState::Idle) {
            return self.try_get_work();
        }

        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, LocalState::Idle) {
            LocalState::DispatchingTasks(partitioned_meta) => {
                self.process_dispatching_tasks(partitioned_meta)
            }

            LocalState::Deserializing(payload, data) => self.process_deserializing(payload, data),

            LocalState::Aggregating(partition_id) => self.process_aggregating(partition_id),

            LocalState::Idle | LocalState::Reading(_) | LocalState::OutputReady(_) => {
                // Nothing to do (OutputReady is handled in event())
                Ok(())
            }
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let LocalState::Reading(payload) = std::mem::replace(&mut self.state, LocalState::Idle) {
            // Async read data
            let _guard = self.semaphore.acquire().await;
            let data = self
                .operator
                .read_with(&payload.location)
                .range(payload.data_range.clone())
                .await?
                .to_vec();

            // Set state to deserializing (will be processed in process())
            self.state = LocalState::Deserializing(payload, data);
        }

        Ok(())
    }
}
