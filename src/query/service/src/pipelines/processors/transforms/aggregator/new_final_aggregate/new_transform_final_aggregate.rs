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
use std::time::Instant;

use bumpalo::Bump;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::arrow::deserialize_column;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::PartitionedPayload;
use databend_common_expression::PayloadFlushState;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use opendal::BlockingOperator;
use tokio::sync::Barrier;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatePayload;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::BucketSpilledPayload;
use crate::pipelines::processors::transforms::aggregator::SerializedPayload;

pub enum LocalState {
    Idle,
    Payload(AggregateMeta),
    SpilledPayload(AggregateMeta),
    OutputReady(DataBlock),
    Aggregate,
    AsyncWait,
}

impl std::fmt::Display for LocalState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LocalState::Idle => write!(f, "Idle"),
            LocalState::Payload(_) => write!(f, "Payload"),
            LocalState::SpilledPayload(_) => write!(f, "SpilledPayload"),
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
    operator: BlockingOperator,
    partition_count: usize,
    repartitioned_queue: Vec<Vec<AggregateMeta>>,
    barrier: Arc<Barrier>,
}

impl NewFinalAggregateTransform {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        id: usize,
        params: Arc<AggregatorParams>,
        operator: BlockingOperator,
        partition_count: usize,
        barrier: Arc<Barrier>,
    ) -> Result<Box<dyn Processor>> {
        Ok(Box::new(NewFinalAggregateTransform {
            input,
            output,
            id,
            params,
            flush_state: PayloadFlushState::default(),
            working_queue: vec![],
            state: LocalState::Idle,
            operator,
            partition_count,
            repartitioned_queue: vec![vec![]; partition_count],
            barrier,
        }))
    }

    fn read(&mut self, spilled_payload: &BucketSpilledPayload) -> Result<Vec<u8>> {
        let instant = Instant::now();

        let data = self
            .operator
            .read_with(&spilled_payload.location)
            .range(spilled_payload.data_range.clone())
            .call()?
            .to_vec();

        {
            Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillReadCount, 1);
            Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillReadBytes, data.len());
            Profile::record_usize_profile(
                ProfileStatisticsName::RemoteSpillReadTime,
                instant.elapsed().as_millis() as usize,
            );
        }

        Ok(data)
    }

    fn deserialize(&self, payload: BucketSpilledPayload, data: Vec<u8>) -> AggregateMeta {
        let mut begin = 0;
        let mut columns = Vec::with_capacity(payload.columns_layout.len());

        for column_layout in &payload.columns_layout {
            columns
                .push(deserialize_column(&data[begin..begin + *column_layout as usize]).unwrap());
            begin += *column_layout as usize;
        }

        AggregateMeta::Serialized(SerializedPayload {
            bucket: payload.bucket,
            data_block: DataBlock::new_from_columns(columns),
            max_partition_count: payload.max_partition_count,
        })
    }

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

        if let Some(aggregate_meta) = self.working_queue.pop() {
            match &aggregate_meta{
                AggregateMeta::AggregatePayload(_)=>{
                    self.state = LocalState::Payload(aggregate_meta);
                }
                AggregateMeta::BucketSpilled(_)=>{
                    self.state = LocalState::SpilledPayload(aggregate_meta);

                }
                _ => return Err(ErrorCode::Internal(
                    "NewFinalAggregateTransform expects AggregatePayload or BucketSpilled from working queue",
                ))
            }
            return Ok(Event::Sync);
        }

        if self.working_queue.is_empty() {
            self.state = LocalState::AsyncWait;
            return Ok(Event::Async);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;

            if let Some(block_meta) = data_block
                .take_meta()
                .and_then(AggregateMeta::downcast_from)
            {
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
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        let state = std::mem::replace(&mut self.state, LocalState::Idle);
        match state {
            LocalState::Payload(payload) => self.repartition(payload),
            LocalState::SpilledPayload(meta) => {
                let AggregateMeta::BucketSpilled(spilled_payload) = meta else {
                    return Err(ErrorCode::Internal(
                        "NewFinalAggregateTransform expects BucketSpilledPayload in SpilledPayload state",
                    ));
                };
                let read_data = self.read(&spilled_payload)?;
                let serialized_payload = self.deserialize(spilled_payload, read_data);
                self.repartition(serialized_payload)
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
                // TODO: add to global queue
                self.barrier.wait().await;
                Ok(())
            }
            _ => Err(ErrorCode::Internal(
                "NewFinalAggregateTransform async_process called in invalid state",
            ))?,
        }
    }
}
