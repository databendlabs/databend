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
use databend_common_expression::AggregatePayload;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::Payload;
use databend_common_expression::SerializedPayload;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::Transform;
use databend_common_pipeline_transforms::Transformer;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::PartitionItem;
use crate::pipelines::processors::transforms::aggregator::PartitionedData;
use crate::servers::flight::v1::exchange::ExchangeShuffleMeta;

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

impl AggregateRowScatter {
    pub fn scatter(&self, mut data_block: DataBlock) -> Result<Vec<DataBlock>> {
        if let Some(block_meta) = data_block.take_meta() {
            if let Some(block_meta) = AggregateMeta::downcast_from(block_meta) {
                return Ok(self
                    .scatter_by_rows(block_meta)?
                    .into_iter()
                    .map(|meta| DataBlock::empty_with_meta(Box::new(meta)))
                    .collect());
            }
        }

        Err(ErrorCode::Internal(
            "Internal, HashTableHashScatter only recv AggregateMeta",
        ))
    }

    fn scatter_by_rows(&self, block_meta: AggregateMeta) -> Result<Vec<AggregateMeta>> {
        let params = &self.aggregate_params;
        match block_meta {
            AggregateMeta::Serialized(payload) => {
                let payload = payload.convert_to_single_payload(
                    params.group_data_types.clone(),
                    params.aggregate_functions.clone(),
                    params.num_states(),
                    Arc::new(Bump::new()),
                )?;
                Ok(payload
                    .scatter_into_buckets(self.buckets)
                    .into_iter()
                    .enumerate()
                    .map(|(bucket, payload)| {
                        AggregateMeta::AggregatePayload(AggregatePayload {
                            bucket: bucket as isize,
                            payload,
                            max_partition_count: 0,
                        })
                    })
                    .collect())
            }
            AggregateMeta::Partitioned { bucket, data } => match data {
                PartitionedData::Empty => Ok((0..self.buckets)
                    .map(|_| AggregateMeta::Partitioned {
                        bucket,
                        data: PartitionedData::Empty,
                    })
                    .collect()),
                PartitionedData::AggregatePayload(payloads) => {
                    let mut partitions = Vec::with_capacity(self.buckets);
                    partitions.resize_with(self.buckets, Vec::new);

                    for payload in payloads {
                        for (index, payload) in self
                            .scatter_payload(payload.bucket, payload.payload, 0)
                            .into_iter()
                            .enumerate()
                        {
                            partitions[index].push(payload);
                        }
                    }

                    Ok(partitions
                        .into_iter()
                        .map(|payloads| AggregateMeta::Partitioned {
                            bucket,
                            data: PartitionedData::AggregatePayload(payloads),
                        })
                        .collect())
                }
                PartitionedData::Serialized(payloads) => {
                    let mut partitions = Vec::with_capacity(self.buckets);
                    partitions.resize_with(self.buckets, Vec::new);

                    for payload in payloads {
                        for (index, payload) in self
                            .scatter_serialized_payload(payload, 0)?
                            .into_iter()
                            .enumerate()
                        {
                            partitions[index].push(payload);
                        }
                    }

                    Ok(partitions
                        .into_iter()
                        .map(|payloads| AggregateMeta::Partitioned {
                            bucket,
                            data: PartitionedData::AggregatePayload(payloads),
                        })
                        .collect())
                }
                PartitionedData::BucketSpilled(payloads) => {
                    let mut partitions = Vec::with_capacity(self.buckets);
                    partitions.resize_with(self.buckets, Vec::new);

                    for payload in payloads {
                        let bucket = payload.bucket as usize;
                        partitions[bucket % self.buckets].push(payload);
                    }

                    Ok(partitions
                        .into_iter()
                        .map(|payloads| AggregateMeta::Partitioned {
                            bucket,
                            data: PartitionedData::BucketSpilled(payloads),
                        })
                        .collect())
                }
                PartitionedData::Mixed(items) => {
                    let mut partitions = Vec::with_capacity(self.buckets);
                    partitions.resize_with(self.buckets, Vec::new);

                    for item in items {
                        match item {
                            PartitionItem::AggregatePayload(payload) => {
                                for (index, payload) in self
                                    .scatter_payload(payload.bucket, payload.payload, 0)
                                    .into_iter()
                                    .enumerate()
                                {
                                    partitions[index]
                                        .push(PartitionItem::AggregatePayload(payload));
                                }
                            }
                            PartitionItem::Serialized(payload) => {
                                for (index, payload) in self
                                    .scatter_serialized_payload(payload, 0)?
                                    .into_iter()
                                    .enumerate()
                                {
                                    partitions[index]
                                        .push(PartitionItem::AggregatePayload(payload));
                                }
                            }
                            PartitionItem::BucketSpilled(payload) => {
                                // we will restore it after sending to the target node
                                // so we just use bucket id to scatter here
                                let bucket = payload.bucket as usize;
                                partitions[bucket % self.buckets]
                                    .push(PartitionItem::BucketSpilled(payload));
                            }
                        }
                    }

                    Ok(partitions
                        .into_iter()
                        .map(|data| AggregateMeta::Partitioned {
                            bucket,
                            data: PartitionedData::Mixed(data),
                        })
                        .collect())
                }
            },
            AggregateMeta::AggregatePayload(p) => Ok(self
                .scatter_payload(p.bucket, p.payload, p.max_partition_count)
                .into_iter()
                .map(AggregateMeta::AggregatePayload)
                .collect()),
            _ => unreachable!(),
        }
    }

    fn scatter_payload(
        &self,
        bucket: isize,
        payload: Payload,
        max_partition_count: usize,
    ) -> Vec<AggregatePayload> {
        payload
            .scatter_into_buckets(self.buckets)
            .into_iter()
            .map(|payload| AggregatePayload {
                bucket,
                payload,
                max_partition_count,
            })
            .collect()
    }

    fn scatter_serialized_payload(
        &self,
        payload: SerializedPayload,
        max_partition_count: usize,
    ) -> Result<Vec<AggregatePayload>> {
        let bucket = payload.bucket;
        let params = &self.aggregate_params;
        let payload = payload.convert_to_single_payload(
            params.group_data_types.clone(),
            params.aggregate_functions.clone(),
            params.num_states(),
            Arc::new(Bump::new()),
        )?;
        Ok(self.scatter_payload(bucket, payload, max_partition_count))
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
    /// Send bucket `b` to output `b % buckets`.
    pub fn scatter(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        let buckets = self.buckets;
        Self::scatter_by(data_block, buckets, |bucket| bucket % buckets)
    }

    /// Send every bucket of a partitioned block to output `route(bucket)` of
    /// `outputs`, keeping its bucket id.
    pub fn scatter_by(
        mut data_block: DataBlock,
        outputs: usize,
        route: impl Fn(usize) -> usize,
    ) -> Result<Vec<DataBlock>> {
        let Some(AggregateMeta::Partitioned { data, .. }) = data_block
            .take_meta()
            .and_then(AggregateMeta::downcast_from)
        else {
            return Err(ErrorCode::Internal(
                "Internal, AggregateBucketScatter only recv Partitioned AggregateMeta",
            ));
        };

        fn split<T>(
            items: Vec<T>,
            outputs: usize,
            route: impl Fn(&T) -> usize,
            wrap: impl Fn(Vec<T>) -> PartitionedData,
        ) -> Vec<DataBlock> {
            let mut chunks = (0..outputs).map(|_| vec![]).collect::<Vec<_>>();
            for item in items {
                chunks[route(&item)].push(item);
            }
            chunks
                .into_iter()
                .map(|chunk| {
                    AggregateMeta::Partitioned {
                        bucket: None,
                        data: wrap(chunk),
                    }
                    .into_datablock()
                })
                .collect()
        }

        Ok(match data {
            PartitionedData::Empty => (0..outputs)
                .map(|_| {
                    AggregateMeta::Partitioned {
                        bucket: None,
                        data: PartitionedData::Empty,
                    }
                    .into_datablock()
                })
                .collect(),
            PartitionedData::Serialized(payloads) => split(
                payloads,
                outputs,
                |payload| route(payload.bucket as usize),
                PartitionedData::Serialized,
            ),
            PartitionedData::AggregatePayload(payloads) => split(
                payloads,
                outputs,
                |payload| route(payload.bucket as usize),
                PartitionedData::AggregatePayload,
            ),
            PartitionedData::BucketSpilled(payloads) => split(
                payloads,
                outputs,
                |payload| route(payload.bucket as usize),
                PartitionedData::BucketSpilled,
            ),
            PartitionedData::Mixed(items) => split(
                items,
                outputs,
                |item| {
                    route(match item {
                        PartitionItem::Serialized(payload) => payload.bucket as usize,
                        PartitionItem::AggregatePayload(payload) => payload.bucket as usize,
                        PartitionItem::BucketSpilled(payload) => payload.bucket as usize,
                    })
                },
                PartitionedData::Mixed,
            ),
        })
    }
}

impl LocalScatter for AggregateBucketScatter {
    fn name(&self) -> &'static str {
        "Bucket"
    }

    fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        self.scatter(data_block)
    }
}
