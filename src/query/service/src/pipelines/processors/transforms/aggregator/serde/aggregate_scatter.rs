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

use std::sync::Arc;

use bumpalo::Bump;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::PartitionedPayload;
use databend_common_expression::Payload;
use databend_common_expression::PayloadFlushState;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::Transform;
use databend_common_pipeline_transforms::Transformer;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatePayload;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::servers::flight::v1::exchange::ExchangeShuffleMeta;
use crate::servers::flight::v1::scatter::FlightScatter;

pub trait LocalScatter: Sync + Send {
    fn name(&self) -> &'static str;

    fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>>;
}

pub struct LocalScatterTransform {
    scatter: Arc<Box<dyn LocalScatter>>,
}

impl LocalScatterTransform {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        scatter: Arc<Box<dyn LocalScatter>>,
    ) -> ProcessorPtr {
        ProcessorPtr::create(Transformer::create(input, output, LocalScatterTransform {
            scatter,
        }))
    }
}

impl Transform for LocalScatterTransform {
    const NAME: &'static str = "LocalScatterTransform";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let blocks = self.scatter.execute(data)?;
        Ok(DataBlock::empty_with_meta(ExchangeShuffleMeta::create(
            blocks,
        )))
    }

    fn name(&self) -> String {
        format!("LocalScatterTransform({})", self.scatter.name())
    }
}

pub struct AggregateRowScatter {
    pub buckets: usize,
    pub(crate) aggregate_params: Arc<AggregatorParams>,
}

fn scatter_payload(mut payload: Payload, buckets: usize) -> Result<Vec<Payload>> {
    let mut buckets = Vec::with_capacity(buckets);

    let group_types = payload.group_types.clone();
    let aggrs = payload.aggrs.clone();
    let mut state = PayloadFlushState::default();

    for _ in 0..buckets.capacity() {
        let p = Payload::new(
            payload.arena.clone(),
            group_types.clone(),
            aggrs.clone(),
            payload.states_layout().cloned(),
        );
        buckets.push(p);
    }

    // scatter each page of the payload.
    while payload.scatter(&mut state, buckets.len()) {
        // copy to the corresponding bucket.
        let probe_state = &*state.probe_state;
        for (bucket, (count, sel)) in buckets.iter_mut().zip(&probe_state.partition_entries) {
            if *count > 0 {
                bucket.copy_rows(&sel[..*count as usize], &state.addresses);
            }
        }
    }
    payload.state_move_out = true;

    Ok(buckets)
}

pub fn scatter_partitioned_payload(
    partitioned_payload: PartitionedPayload,
    buckets: usize,
) -> Result<Vec<PartitionedPayload>> {
    let mut buckets = Vec::with_capacity(buckets);

    let group_types = partitioned_payload.group_types.clone();
    let aggrs = partitioned_payload.aggrs.clone();
    let partition_count = partitioned_payload.partition_count() as u64;
    let mut state = PayloadFlushState::default();

    for _ in 0..buckets.capacity() {
        buckets.push(PartitionedPayload::new(
            group_types.clone(),
            aggrs.clone(),
            partition_count,
            partitioned_payload.arenas.clone(),
        ));
    }

    let mut payloads = Vec::with_capacity(buckets.len());

    for _ in 0..payloads.capacity() {
        payloads.push(Payload::new(
            Arc::new(Bump::new()),
            group_types.clone(),
            aggrs.clone(),
            partitioned_payload.states_layout().cloned(),
        ));
    }

    for mut payload in partitioned_payload.payloads.into_iter() {
        // scatter each page of the payload.
        while payload.scatter(&mut state, buckets.len()) {
            // copy to the corresponding bucket.
            for (bucket, (count, sel)) in payloads
                .iter_mut()
                .zip(&state.probe_state.partition_entries)
            {
                if *count > 0 {
                    bucket.copy_rows(&sel[..*count as usize], &state.addresses);
                }
            }
        }
        state.clear();
        payload.state_move_out = true;
    }

    for (idx, payload) in payloads.into_iter().enumerate() {
        buckets[idx].combine_single(payload, &mut state, None);
    }

    Ok(buckets)
}

impl AggregateRowScatter {
    pub fn scatter(&self, mut data_block: DataBlock) -> Result<Vec<DataBlock>> {
        if let Some(block_meta) = data_block.take_meta() {
            if let Some(block_meta) = AggregateMeta::downcast_from(block_meta) {
                let mut blocks = Vec::with_capacity(self.buckets);
                match block_meta {
                    AggregateMeta::Spilled(_) => unreachable!(),
                    AggregateMeta::BucketSpilled(_) => unreachable!(),
                    AggregateMeta::Serialized(payload) => {
                        let mut partition = payload.convert_to_partitioned_payload(
                            self.aggregate_params.group_data_types.clone(),
                            self.aggregate_params.aggregate_functions.clone(),
                            self.aggregate_params.num_states(),
                            0,
                            self.aggregate_params.enable_experiment_hash_index,
                            Arc::new(Bump::new()),
                        )?;
                        let payload = partition.payloads.pop();
                        if let Some(payload) = payload {
                            for (bucket, payload) in scatter_payload(payload, self.buckets)?
                                .into_iter()
                                .enumerate()
                            {
                                blocks.push(DataBlock::empty_with_meta(
                                    AggregateMeta::create_agg_payload(bucket as isize, payload, 0),
                                ));
                            }
                        }
                    }
                    AggregateMeta::Partitioned { data, bucket } => {
                        let mut partitions = Vec::with_capacity(self.buckets);
                        partitions.resize_with(self.buckets, Vec::new);

                        for meta in data {
                            match meta {
                                AggregateMeta::AggregatePayload(payload) => {
                                    let bucket_id = payload.bucket;
                                    for (index, payload) in
                                        scatter_payload(payload.payload, self.buckets)?
                                            .into_iter()
                                            .enumerate()
                                    {
                                        partitions[index].push(AggregateMeta::AggregatePayload(
                                            AggregatePayload {
                                                bucket: bucket_id,
                                                payload,
                                                max_partition_count: 0,
                                            },
                                        ));
                                    }
                                }
                                AggregateMeta::Serialized(payload) => {
                                    let bucket_id = payload.bucket;
                                    let mut partition = payload.convert_to_partitioned_payload(
                                        self.aggregate_params.group_data_types.clone(),
                                        self.aggregate_params.aggregate_functions.clone(),
                                        self.aggregate_params.num_states(),
                                        0,
                                        self.aggregate_params.enable_experiment_hash_index,
                                        Arc::new(Bump::new()),
                                    )?;
                                    let payload = partition.payloads.pop();
                                    if let Some(payload) = payload {
                                        for (index, payload) in
                                            scatter_payload(payload, self.buckets)?
                                                .into_iter()
                                                .enumerate()
                                        {
                                            partitions[index].push(
                                                AggregateMeta::AggregatePayload(AggregatePayload {
                                                    bucket: bucket_id,
                                                    payload,
                                                    max_partition_count: 0,
                                                }),
                                            );
                                        }
                                    }
                                }
                                AggregateMeta::NewBucketSpilled(payload) => {
                                    // we will restore it after sending to the target node
                                    // so we just use bucket id to scatter here
                                    let bucket = payload.bucket as usize;
                                    partitions[bucket % self.buckets]
                                        .push(AggregateMeta::NewBucketSpilled(payload));
                                }
                                _ => {
                                    unreachable!(
                                        "Internal, AggregateRowScatter only recv AggregatePayload/Serialized in Partitioned"
                                    )
                                }
                            }
                        }

                        for payloads in partitions {
                            blocks.push(DataBlock::empty_with_meta(
                                AggregateMeta::create_partitioned(bucket, payloads),
                            ));
                        }
                    }
                    AggregateMeta::NewBucketSpilled(_) => unreachable!(),
                    AggregateMeta::NewSpilled(_) => unreachable!(),
                    AggregateMeta::AggregateSpilling(payload) => {
                        for p in scatter_partitioned_payload(payload, self.buckets)? {
                            blocks.push(DataBlock::empty_with_meta(
                                AggregateMeta::create_agg_spilling(p),
                            ));
                        }
                    }
                    AggregateMeta::AggregatePayload(p) => {
                        for payload in scatter_payload(p.payload, self.buckets)? {
                            blocks.push(DataBlock::empty_with_meta(
                                AggregateMeta::create_agg_payload(
                                    p.bucket,
                                    payload,
                                    p.max_partition_count,
                                ),
                            ));
                        }
                    }
                };

                return Ok(blocks);
            }
        }

        Err(ErrorCode::Internal(
            "Internal, HashTableHashScatter only recv AggregateMeta",
        ))
    }
}

impl FlightScatter for AggregateRowScatter {
    fn name(&self) -> &'static str {
        "RowHash"
    }

    fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        self.scatter(data_block)
    }
}

impl LocalScatter for AggregateRowScatter {
    fn name(&self) -> &'static str {
        "RowHash"
    }

    fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        self.scatter(data_block)
    }
}

pub struct AggregateBucketScatter {
    pub buckets: usize,
}

impl AggregateBucketScatter {
    pub fn scatter(&self, mut data_block: DataBlock, is_local: bool) -> Result<Vec<DataBlock>> {
        if let Some(block_meta) = data_block.take_meta() {
            if let Some(block_meta) = AggregateMeta::downcast_from(block_meta) {
                match block_meta {
                    AggregateMeta::Partitioned { data, .. } => {
                        let mut chunks = (0..self.buckets).map(|_| vec![]).collect::<Vec<_>>();
                        for bucket_payload in data {
                            match bucket_payload {
                                AggregateMeta::Serialized(mut payload) => {
                                    let bucket = payload.bucket as usize;
                                    if !is_local {
                                        payload.bucket /= self.buckets as isize;
                                    }
                                    chunks[bucket % self.buckets]
                                        .push(AggregateMeta::Serialized(payload));
                                }
                                AggregateMeta::AggregatePayload(mut payload) => {
                                    let bucket = payload.bucket as usize;
                                    if !is_local {
                                        payload.bucket /= self.buckets as isize;
                                    }
                                    chunks[bucket % self.buckets]
                                        .push(AggregateMeta::AggregatePayload(payload));
                                }
                                AggregateMeta::NewBucketSpilled(mut spilled_payload) => {
                                    let bucket = spilled_payload.bucket as usize;
                                    if !is_local {
                                        spilled_payload.bucket /= self.buckets as isize;
                                    }
                                    chunks[bucket % self.buckets]
                                        .push(AggregateMeta::NewBucketSpilled(spilled_payload));
                                }

                                _ => {
                                    unreachable!(
                                        "Internal, AggregateBucketScatter only recv AggregatePayload/SerializedPayload in Partitioned"
                                    )
                                }
                            }
                        }
                        let blocks = chunks
                            .into_iter()
                            .map(|payload| {
                                DataBlock::empty_with_meta(AggregateMeta::create_partitioned(
                                    None, payload,
                                ))
                            })
                            .collect::<Vec<_>>();
                        return Ok(blocks);
                    }
                    _ => {
                        unreachable!(
                            "Internal, AggregateBucketScatter only recv Partitioned AggregateMeta"
                        )
                    }
                };
            }
        }

        Err(ErrorCode::Internal(
            "Internal, AggregateBucketScatter only recv AggregateMeta",
        ))
    }
}

impl FlightScatter for AggregateBucketScatter {
    fn name(&self) -> &'static str {
        "Bucket"
    }

    fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        self.scatter(data_block, false)
    }
}

impl LocalScatter for AggregateBucketScatter {
    fn name(&self) -> &'static str {
        "Bucket"
    }

    fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        self.scatter(data_block, true)
    }
}
