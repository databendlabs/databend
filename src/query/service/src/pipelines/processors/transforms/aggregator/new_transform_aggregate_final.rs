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

use concurrent_queue::ConcurrentQueue;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::arrow::deserialize_column;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use opendal::Operator;
use tokio::sync::Semaphore;

use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
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
    ) -> Result<Box<dyn Processor>> {
        Ok(Box::new(NewTransformAggregateFinal {
            input,
            output,
            operator,
            semaphore,
            params,
            shared_state,
            partition_count,
            state: LocalState::Idle,
        }))
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
                        self.shared_state
                            .work_queue
                            .push(payload)
                            .map_err(|_| ErrorCode::Internal("Failed to push task to work queue"))?;

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

            if self.input.is_finished() && self.shared_state.input_finished.load(Ordering::SeqCst) {
                self.output.finish();
                return Ok(Event::Finished);
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
                    // Non-spilled data: directly repartition and push to aggregate queues
                    self.repartition_and_push(other)?;
                }
            }
        }

        self.state = LocalState::Idle;
        Ok(())
    }

    fn process_deserializing(&mut self, payload: BucketSpilledPayload, data: Vec<u8>) -> Result<()> {
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

    fn repartition_and_push(&mut self, _meta: AggregateMeta) -> Result<()> {
        // TODO: Repartition the meta into self.partition_count partitions
        // and push to corresponding aggregate_queues

        // Increment completed tasks
        let completed = self.shared_state.completed_tasks.fetch_add(1, Ordering::SeqCst) + 1;
        let total = self.shared_state.total_tasks.load(Ordering::SeqCst);

        // Check if all restore tasks are completed
        if completed == total && self.shared_state.input_finished.load(Ordering::SeqCst) {
            self.shared_state.restore_finished.store(true, Ordering::SeqCst);
        }

        todo!("Repartitioning logic not implemented yet")
    }

    fn process_aggregating(&mut self, _partition_id: usize) -> Result<()> {
        // TODO: Aggregate data from aggregate_queues[partition_id]
        // This is now exclusive to each processor (orthogonal partitions)
        todo!("Aggregation logic not implemented yet")
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

        // Priority 1: Async reading
        if matches!(&self.state, LocalState::Reading(_)) {
            return Ok(Event::Async);
        }

        // Priority 2: Sync processing (dispatching, deserializing, aggregating)
        if matches!(
            &self.state,
            LocalState::DispatchingTasks(_)
                | LocalState::Deserializing(_, _)
                | LocalState::Aggregating(_)
        ) {
            return Ok(Event::Sync);
        }

        // Priority 3: Try to get work
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

            LocalState::Deserializing(payload, data) => {
                self.process_deserializing(payload, data)
            }

            LocalState::Aggregating(partition_id) => {
                self.process_aggregating(partition_id)
            }

            LocalState::Idle | LocalState::Reading(_) => {
                // Nothing to do
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
