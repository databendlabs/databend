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

use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggregatePayload;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::BlockProfileStatistics;
use databend_common_expression::BucketSpilledPayload;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::PartitionedPayload;
use databend_common_expression::Payload;
use databend_common_expression::SerializedPayload;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::StringType;
use databend_common_storages_parquet::serialize_row_group_meta_to_bytes;
use parquet::file::metadata::RowGroupMetaData;

use crate::pipelines::processors::transforms::aggregator::AggregateSerdeMeta;

pub struct NewSpilledPayload {
    pub bucket: isize,
    pub location: String,
    pub row_group: RowGroupMetaData,
}

pub enum AggregateMeta {
    Serialized(SerializedPayload),
    AggregatePayload(AggregatePayload),
    AggregateSpilling(PartitionedPayload),
    BucketSpilled(BucketSpilledPayload),
    Spilled(Vec<BucketSpilledPayload>),

    Partitioned {
        bucket: Option<isize>,
        data: PartitionedData,
    },

    NewBucketSpilled(NewSpilledPayload),
    NewSpilled(Vec<NewSpilledPayload>),
}

pub enum PartitionedData {
    Empty,
    Serialized(Vec<SerializedPayload>),
    AggregatePayload(Vec<AggregatePayload>),
    BucketSpilled(Vec<BucketSpilledPayload>),
    NewBucketSpilled(Vec<NewSpilledPayload>),
    Mixed(Vec<PartitionItem>),
}

impl Debug for PartitionedData {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            PartitionedData::Empty => f.debug_struct("PartitionedAggregateData::Empty").finish(),
            PartitionedData::Serialized(_) => f
                .debug_struct("PartitionedAggregateData::Serialized")
                .finish(),
            PartitionedData::AggregatePayload(_) => f
                .debug_struct("PartitionedAggregateData::AggregatePayload")
                .finish(),
            PartitionedData::BucketSpilled(_) => f
                .debug_struct("PartitionedAggregateData::BucketSpilled")
                .finish(),
            PartitionedData::NewBucketSpilled(_) => f
                .debug_struct("PartitionedAggregateData::NewBucketSpilled")
                .finish(),
            PartitionedData::Mixed(_) => f.debug_struct("PartitionedAggregateData::Mixed").finish(),
        }
    }
}

impl PartitionedData {
    fn output_stats(&self) -> Option<BlockProfileStatistics> {
        match self {
            PartitionedData::Empty => Some(BlockProfileStatistics { rows: 0, bytes: 0 }),
            PartitionedData::Serialized(payloads) => Some(BlockProfileStatistics {
                rows: payloads.iter().map(|p| p.data_block.num_rows()).sum(),
                bytes: payloads.iter().map(|p| p.data_block.memory_size()).sum(),
            }),
            PartitionedData::AggregatePayload(payloads) => Some(BlockProfileStatistics {
                rows: payloads.iter().map(|p| p.payload.len()).sum(),
                bytes: payloads.iter().map(|p| p.payload.memory_size()).sum(),
            }),
            PartitionedData::BucketSpilled(_) => None,
            PartitionedData::NewBucketSpilled(payloads) => Some(BlockProfileStatistics {
                rows: payloads
                    .iter()
                    .map(|p| p.row_group.num_rows() as usize)
                    .sum(),
                bytes: payloads
                    .iter()
                    .map(|p| p.row_group.total_byte_size() as usize)
                    .sum(),
            }),
            PartitionedData::Mixed(items) => {
                let mut rows = 0;
                let mut bytes = 0;
                for item in items {
                    let stats = item.output_stats()?;
                    rows += stats.rows;
                    bytes += stats.bytes;
                }
                Some(BlockProfileStatistics { rows, bytes })
            }
        }
    }
}

pub enum PartitionItem {
    Serialized(SerializedPayload),
    AggregatePayload(AggregatePayload),
    BucketSpilled(BucketSpilledPayload),
    NewBucketSpilled(NewSpilledPayload),
}

impl Debug for PartitionItem {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            PartitionItem::Serialized(_) => f
                .debug_struct("AggregatePartitionItem::Serialized")
                .finish(),
            PartitionItem::AggregatePayload(_) => f
                .debug_struct("AggregatePartitionItem::AggregatePayload")
                .finish(),
            PartitionItem::BucketSpilled(_) => f
                .debug_struct("AggregatePartitionItem::BucketSpilled")
                .finish(),
            PartitionItem::NewBucketSpilled(_) => f
                .debug_struct("AggregatePartitionItem::NewBucketSpilled")
                .finish(),
        }
    }
}

impl From<PartitionItem> for AggregateMeta {
    fn from(item: PartitionItem) -> Self {
        match item {
            PartitionItem::Serialized(payload) => AggregateMeta::Serialized(payload),
            PartitionItem::AggregatePayload(payload) => AggregateMeta::AggregatePayload(payload),
            PartitionItem::BucketSpilled(payload) => AggregateMeta::BucketSpilled(payload),
            PartitionItem::NewBucketSpilled(payload) => AggregateMeta::NewBucketSpilled(payload),
        }
    }
}

impl PartitionItem {
    pub fn into_datablock(self) -> DataBlock {
        DataBlock::empty_with_meta(Box::new(AggregateMeta::from(self)))
    }

    pub fn output_stats(&self) -> Option<BlockProfileStatistics> {
        match self {
            PartitionItem::Serialized(payload) => Some(BlockProfileStatistics {
                rows: payload.data_block.num_rows(),
                bytes: payload.data_block.memory_size(),
            }),
            PartitionItem::AggregatePayload(payload) => Some(BlockProfileStatistics {
                rows: payload.payload.len(),
                bytes: payload.payload.memory_size(),
            }),
            PartitionItem::BucketSpilled(_) => None,
            PartitionItem::NewBucketSpilled(payload) => Some(BlockProfileStatistics {
                rows: payload.row_group.num_rows() as usize,
                bytes: payload.row_group.total_byte_size() as usize,
            }),
        }
    }

    pub fn serialize_mixed(data: Vec<PartitionItem>) -> Result<DataBlock> {
        if data.is_empty() {
            return Ok(DataBlock::empty());
        }

        let has_new_spilled = data
            .iter()
            .any(|item| matches!(item, PartitionItem::NewBucketSpilled(_)));
        let has_payload = data.iter().any(|item| {
            matches!(
                item,
                PartitionItem::Serialized(_) | PartitionItem::AggregatePayload(_)
            )
        });

        if has_new_spilled && has_payload {
            return Err(ErrorCode::Internal(
                "Partitioned meta cannot serialize mixed payload and spilled batches.",
            ));
        }

        if has_new_spilled {
            let bucket_num = data.len();
            let mut bucket_column = Vec::with_capacity(bucket_num);
            let mut row_group_column = Vec::with_capacity(bucket_num);
            let mut location_column = Vec::with_capacity(bucket_num);

            for item in data {
                let PartitionItem::NewBucketSpilled(payload) = item else {
                    return Err(ErrorCode::Internal(
                        "Partitioned meta cannot serialize mixed spilled batches.",
                    ));
                };
                bucket_column.push(payload.bucket as i64);
                location_column.push(payload.location);
                row_group_column.push(serialize_row_group_meta_to_bytes(&payload.row_group)?);
            }
            let data_block = DataBlock::new_from_columns(vec![
                Int64Type::from_data(bucket_column),
                StringType::from_data(location_column),
                BinaryType::from_data(row_group_column),
            ]);

            return data_block.add_meta(Some(AggregateSerdeMeta::create_new_spilled(
                bucket_num as isize,
            )));
        }

        let mut buckets = Vec::with_capacity(data.len());
        let mut payload_row_counts = Vec::with_capacity(data.len());
        let mut payload_blocks = Vec::with_capacity(data.len());

        for item in data {
            let (bucket, block) = match item {
                PartitionItem::Serialized(payload) => (payload.bucket, payload.data_block),
                PartitionItem::AggregatePayload(payload) => {
                    (payload.bucket, payload.payload.aggregate_flush_all()?)
                }
                PartitionItem::BucketSpilled(_) => {
                    return Err(ErrorCode::Internal(
                        "Partitioned meta cannot serialize legacy spilled batches.",
                    ));
                }
                PartitionItem::NewBucketSpilled(_) => unreachable!(),
            };

            if block.num_rows() == 0 {
                continue;
            }
            buckets.push(bucket);
            payload_row_counts.push(block.num_rows());
            payload_blocks.push(block);
        }

        if payload_blocks.is_empty() {
            return Ok(DataBlock::empty());
        }

        let merged_block = DataBlock::concat(&payload_blocks)?;
        merged_block.add_meta(Some(AggregateSerdeMeta::create_partitioned_payload(
            buckets,
            payload_row_counts,
            false,
        )))
    }
}

pub enum AggregateBucketInput {
    Direct {
        bucket: isize,
        max_partition_count: usize,
        is_empty: bool,
        meta: PartitionItem,
    },
    Spilled {
        max_partition_count: usize,
        metas: Vec<(isize, PartitionItem)>,
    },
}

impl AggregateMeta {
    pub fn create_agg_payload(
        bucket: isize,
        payload: Payload,
        max_partition_count: usize,
    ) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::AggregatePayload(AggregatePayload {
            bucket,
            payload,
            max_partition_count,
        }))
    }

    pub fn create_agg_spilling(payload: PartitionedPayload) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::AggregateSpilling(payload))
    }

    pub fn create_serialized(
        bucket: isize,
        block: DataBlock,
        max_partition_count: usize,
    ) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::Serialized(SerializedPayload {
            bucket,
            data_block: block,
            max_partition_count,
        }))
    }

    pub fn create_spilled(buckets_payload: Vec<BucketSpilledPayload>) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::Spilled(buckets_payload))
    }

    pub fn create_bucket_spilled(payload: BucketSpilledPayload) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::BucketSpilled(payload))
    }

    pub fn create_partitioned(bucket: Option<isize>, data: PartitionedData) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::Partitioned { bucket, data })
    }

    pub fn create_new_bucket_spilled(payload: NewSpilledPayload) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::NewBucketSpilled(payload))
    }

    pub fn create_new_spilled(payloads: Vec<NewSpilledPayload>) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::NewSpilled(payloads))
    }

    pub fn into_bucket_input(self, default_max_partition_count: usize) -> AggregateBucketInput {
        match self {
            AggregateMeta::BucketSpilled(payload) => {
                let bucket = payload.bucket;
                let max_partition_count = payload.max_partition_count;
                AggregateBucketInput::Spilled {
                    max_partition_count,
                    metas: vec![(bucket, PartitionItem::BucketSpilled(payload))],
                }
            }
            AggregateMeta::Spilled(buckets_payload) => {
                let max_partition_count = buckets_payload
                    .first()
                    .map(|payload| payload.max_partition_count)
                    .unwrap_or(default_max_partition_count);
                let metas = buckets_payload
                    .into_iter()
                    .map(|payload| (payload.bucket, PartitionItem::BucketSpilled(payload)))
                    .collect();
                AggregateBucketInput::Spilled {
                    max_partition_count,
                    metas,
                }
            }
            AggregateMeta::Serialized(payload) => {
                let bucket = payload.bucket;
                let max_partition_count = payload.max_partition_count;
                let is_empty = payload.data_block.is_empty();
                AggregateBucketInput::Direct {
                    bucket,
                    max_partition_count,
                    is_empty,
                    meta: PartitionItem::Serialized(payload),
                }
            }
            AggregateMeta::AggregatePayload(payload) => {
                let bucket = payload.bucket;
                let max_partition_count = payload.max_partition_count;
                let is_empty = payload.payload.len() == 0;
                AggregateBucketInput::Direct {
                    bucket,
                    max_partition_count,
                    is_empty,
                    meta: PartitionItem::AggregatePayload(payload),
                }
            }
            _ => unreachable!(),
        }
    }

    pub fn bucket_spilled_payloads(&self) -> Vec<&BucketSpilledPayload> {
        match self {
            AggregateMeta::BucketSpilled(payload) => vec![payload],
            AggregateMeta::Partitioned {
                data: PartitionedData::BucketSpilled(payloads),
                ..
            } => payloads.iter().collect(),
            AggregateMeta::Partitioned {
                data: PartitionedData::Mixed(items),
                ..
            } => items
                .iter()
                .filter_map(|item| match item {
                    PartitionItem::BucketSpilled(payload) => Some(payload),
                    _ => None,
                })
                .collect(),
            _ => vec![],
        }
    }

    pub fn deserialize_bucket_spilled(
        self,
        mut read_data: VecDeque<Vec<u8>>,
    ) -> Result<AggregateMeta> {
        let mut take_read_data = || {
            read_data.pop_front().ok_or_else(|| {
                ErrorCode::Internal(
                    "Internal, missing spilled aggregate data for BucketSpilled meta.",
                )
            })
        };

        let meta = match self {
            AggregateMeta::BucketSpilled(payload) => {
                AggregateMeta::Serialized(payload.deserialize(take_read_data()?)?)
            }
            AggregateMeta::Partitioned {
                bucket,
                data: PartitionedData::BucketSpilled(payloads),
            } => {
                let mut new_data = Vec::with_capacity(payloads.len());
                for payload in payloads {
                    new_data.push(payload.deserialize(take_read_data()?)?);
                }

                AggregateMeta::Partitioned {
                    bucket,
                    data: PartitionedData::Serialized(new_data),
                }
            }
            AggregateMeta::Partitioned {
                bucket,
                data: PartitionedData::Mixed(items),
            } => {
                let mut new_items = Vec::with_capacity(items.len());
                for item in items {
                    new_items.push(match item {
                        PartitionItem::BucketSpilled(payload) => {
                            PartitionItem::Serialized(payload.deserialize(take_read_data()?)?)
                        }
                        item => item,
                    });
                }

                AggregateMeta::Partitioned {
                    bucket,
                    data: PartitionedData::Mixed(new_items),
                }
            }
            AggregateMeta::Partitioned { bucket, data } => {
                AggregateMeta::Partitioned { bucket, data }
            }
            _ => unreachable!(),
        };

        if !read_data.is_empty() {
            return Err(ErrorCode::Internal(
                "Internal, spilled aggregate read data exceeds BucketSpilled metas.",
            ));
        }

        Ok(meta)
    }

    pub fn into_datablock(self) -> DataBlock {
        DataBlock::empty_with_meta(Box::new(self))
    }
}

impl serde::Serialize for AggregateMeta {
    fn serialize<S>(&self, _: S) -> std::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unreachable!("AggregateMeta does not support exchanging between multiple nodes")
    }
}

impl<'de> serde::Deserialize<'de> for AggregateMeta {
    fn deserialize<D>(_: D) -> std::result::Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unreachable!("AggregateMeta does not support exchanging between multiple nodes")
    }
}

impl Debug for AggregateMeta {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            AggregateMeta::Partitioned { .. } => {
                f.debug_struct("AggregateMeta::Partitioned").finish()
            }
            AggregateMeta::Serialized { .. } => {
                f.debug_struct("AggregateMeta::Serialized").finish()
            }
            AggregateMeta::Spilled(_) => f.debug_struct("Aggregate::Spilled").finish(),
            AggregateMeta::BucketSpilled(_) => f.debug_struct("Aggregate::BucketSpilled").finish(),
            AggregateMeta::NewBucketSpilled(_) => {
                f.debug_struct("Aggregate::NewBucketSpilled").finish()
            }
            AggregateMeta::NewSpilled(_) => f.debug_struct("Aggregate::NewSpilled").finish(),
            AggregateMeta::AggregatePayload(_) => {
                f.debug_struct("AggregateMeta:AggregatePayload").finish()
            }
            AggregateMeta::AggregateSpilling(_) => {
                f.debug_struct("AggregateMeta:AggregateSpilling").finish()
            }
        }
    }
}

impl BlockMetaInfo for AggregateMeta {
    fn typetag_deserialize(&self) {
        unimplemented!("AggregateMeta does not support exchanging between multiple nodes")
    }

    fn typetag_name(&self) -> &'static str {
        unimplemented!("AggregateMeta does not support exchanging between multiple nodes")
    }

    fn output_stats(&self) -> Option<BlockProfileStatistics> {
        match self {
            AggregateMeta::Serialized(payload) => Some(BlockProfileStatistics {
                rows: payload.data_block.num_rows(),
                bytes: payload.data_block.memory_size(),
            }),
            AggregateMeta::AggregatePayload(payload) => Some(BlockProfileStatistics {
                rows: payload.payload.len(),
                bytes: payload.payload.memory_size(),
            }),
            AggregateMeta::AggregateSpilling(payload) => Some(BlockProfileStatistics {
                rows: payload.len(),
                bytes: payload.memory_size(),
            }),
            AggregateMeta::Spilled(_) | AggregateMeta::BucketSpilled(_) => None,
            AggregateMeta::Partitioned { data, .. } => data.output_stats(),
            AggregateMeta::NewBucketSpilled(payload) => Some(BlockProfileStatistics {
                rows: payload.row_group.num_rows() as usize,
                bytes: payload.row_group.total_byte_size() as usize,
            }),
            AggregateMeta::NewSpilled(payloads) => Some(BlockProfileStatistics {
                rows: payloads
                    .iter()
                    .map(|p| p.row_group.num_rows() as usize)
                    .sum(),
                bytes: payloads
                    .iter()
                    .map(|p| p.row_group.total_byte_size() as usize)
                    .sum(),
            }),
        }
    }
}
