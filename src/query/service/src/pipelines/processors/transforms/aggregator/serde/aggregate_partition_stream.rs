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

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::PartitionedData;
use crate::servers::flight::v1::partition::PartitionStream;
use crate::servers::flight::v1::partition::PartitionedBlock;
use crate::servers::flight::v1::partition::pre_partitioned_blocks;

#[derive(Clone)]
pub struct AggregateRowPartitionStream {
    pub buckets: usize,
    pub(crate) aggregate_params: Arc<AggregatorParams>,
}

impl AggregateRowPartitionStream {
    fn spilled_destination(bucket: isize, buckets: usize) -> Result<usize> {
        if buckets == 0 {
            return Err(ErrorCode::Internal(
                "Aggregate row shuffle has no destination lanes",
            ));
        }
        let global_lane = usize::try_from(bucket).map_err(|_| {
            ErrorCode::Internal(format!(
                "Aggregate row shuffle received invalid spilled bucket {bucket}",
            ))
        })?;
        Ok(global_lane % buckets)
    }

    fn scatter(&self, mut data_block: DataBlock) -> Result<Vec<DataBlock>> {
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
                        })
                    })
                    .collect())
            }
            AggregateMeta::Partitioned { bucket, data } => match data {
                PartitionedData::AggregatePayload(payloads) => {
                    let mut partitions = Vec::with_capacity(self.buckets);
                    partitions.resize_with(self.buckets, Vec::new);

                    for payload in payloads {
                        for (index, payload) in self
                            .scatter_payload(payload.bucket, payload.payload)
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
                            .scatter_serialized_payload(payload)?
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
                        let destination = Self::spilled_destination(payload.bucket, self.buckets)?;
                        partitions[destination].push(payload);
                    }

                    Ok(partitions
                        .into_iter()
                        .map(|payloads| AggregateMeta::Partitioned {
                            bucket,
                            data: PartitionedData::BucketSpilled(payloads),
                        })
                        .collect())
                }
            },
            AggregateMeta::AggregatePayload(p) => Ok(self
                .scatter_payload(p.bucket, p.payload)
                .into_iter()
                .map(AggregateMeta::AggregatePayload)
                .collect()),
            _ => unreachable!(),
        }
    }

    fn scatter_payload(&self, bucket: isize, payload: Payload) -> Vec<AggregatePayload> {
        payload
            .scatter_into_buckets(self.buckets)
            .into_iter()
            .map(|payload| AggregatePayload { bucket, payload })
            .collect()
    }

    fn scatter_serialized_payload(
        &self,
        payload: SerializedPayload,
    ) -> Result<Vec<AggregatePayload>> {
        let bucket = payload.bucket;
        let params = &self.aggregate_params;
        let payload = payload.convert_to_single_payload(
            params.group_data_types.clone(),
            params.aggregate_functions.clone(),
            params.num_states(),
            Arc::new(Bump::new()),
        )?;
        Ok(self.scatter_payload(bucket, payload))
    }
}

impl PartitionStream for AggregateRowPartitionStream {
    fn push(&mut self, data_block: DataBlock) -> Result<Vec<PartitionedBlock>> {
        pre_partitioned_blocks(self.scatter(data_block)?, self.buckets)
    }
}

#[derive(Clone)]
pub struct AggregateBucketPartitionStream {
    pub buckets: usize,
}

impl AggregateBucketPartitionStream {
    fn scatter(&self, mut data_block: DataBlock) -> Result<Vec<DataBlock>> {
        let Some(block_meta) = data_block
            .take_meta()
            .and_then(AggregateMeta::downcast_from)
        else {
            return Err(ErrorCode::Internal(
                "Internal, AggregateBucketPartitionStream only recv AggregateMeta",
            ));
        };

        Ok(match block_meta {
            AggregateMeta::Partitioned { data, .. } => match data {
                PartitionedData::Serialized(payloads) => {
                    let mut chunks = (0..self.buckets).map(|_| vec![]).collect::<Vec<_>>();
                    for mut payload in payloads {
                        let bucket = payload.bucket as usize;
                        payload.bucket /= self.buckets as isize;
                        chunks[bucket % self.buckets].push(payload);
                    }
                    chunks
                        .into_iter()
                        .map(|payload| {
                            AggregateMeta::Partitioned {
                                bucket: None,
                                data: PartitionedData::Serialized(payload),
                            }
                            .into_datablock()
                        })
                        .collect()
                }
                PartitionedData::AggregatePayload(payloads) => {
                    let mut chunks = (0..self.buckets).map(|_| vec![]).collect::<Vec<_>>();
                    for mut payload in payloads {
                        let bucket = payload.bucket as usize;
                        payload.bucket /= self.buckets as isize;
                        chunks[bucket % self.buckets].push(payload);
                    }
                    chunks
                        .into_iter()
                        .map(|payload| {
                            AggregateMeta::Partitioned {
                                bucket: None,
                                data: PartitionedData::AggregatePayload(payload),
                            }
                            .into_datablock()
                        })
                        .collect()
                }
                PartitionedData::BucketSpilled(payloads) => {
                    let mut chunks = (0..self.buckets).map(|_| vec![]).collect::<Vec<_>>();
                    for mut spilled_payload in payloads {
                        let bucket = spilled_payload.bucket as usize;
                        spilled_payload.bucket /= self.buckets as isize;
                        chunks[bucket % self.buckets].push(spilled_payload);
                    }
                    chunks
                        .into_iter()
                        .map(|payload| {
                            AggregateMeta::Partitioned {
                                bucket: None,
                                data: PartitionedData::BucketSpilled(payload),
                            }
                            .into_datablock()
                        })
                        .collect()
                }
            },
            _ => {
                unreachable!(
                    "Internal, AggregateBucketPartitionStream only recv Partitioned AggregateMeta"
                )
            }
        })
    }
}

impl PartitionStream for AggregateBucketPartitionStream {
    fn push(&mut self, data_block: DataBlock) -> Result<Vec<PartitionedBlock>> {
        pre_partitioned_blocks(self.scatter(data_block)?, self.buckets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_spilled_bucket_routes_by_global_lane() {
        for global_lane in 0..12 {
            assert_eq!(
                AggregateRowPartitionStream::spilled_destination(global_lane, 12).unwrap(),
                global_lane as usize
            );
        }

        assert_eq!(
            AggregateRowPartitionStream::spilled_destination(10, 4).unwrap(),
            2
        );
        assert!(AggregateRowPartitionStream::spilled_destination(-1, 4).is_err());
    }

    #[test]
    fn test_bucket_partition_selects_worker_and_consumes_bucket_once() {
        let input = AggregateMeta::Partitioned {
            bucket: None,
            data: PartitionedData::Serialized(vec![
                SerializedPayload {
                    bucket: 10,
                    data_block: DataBlock::empty(),
                },
                SerializedPayload {
                    bucket: 3,
                    data_block: DataBlock::empty(),
                },
            ]),
        }
        .into_datablock();

        let mut blocks = AggregateBucketPartitionStream { buckets: 4 }
            .scatter(input)
            .unwrap();
        assert_eq!(blocks.len(), 4);

        let bucket_at = |block: &mut DataBlock| {
            let meta = AggregateMeta::downcast_from(block.take_meta().unwrap()).unwrap();
            let AggregateMeta::Partitioned {
                data: PartitionedData::Serialized(payloads),
                ..
            } = meta
            else {
                panic!("expected serialized aggregate partition")
            };
            payloads
                .into_iter()
                .map(|payload| payload.bucket)
                .collect::<Vec<_>>()
        };

        assert!(bucket_at(&mut blocks[0]).is_empty());
        assert!(bucket_at(&mut blocks[1]).is_empty());
        assert_eq!(bucket_at(&mut blocks[2]), vec![2]);
        assert_eq!(bucket_at(&mut blocks[3]), vec![0]);
    }
}
