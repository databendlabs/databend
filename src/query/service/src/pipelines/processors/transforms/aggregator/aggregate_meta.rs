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

use std::fmt::Debug;
use std::fmt::Formatter;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggregatePayload;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::BlockProfileStatistics;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Payload;
use databend_common_expression::SerializedPayload;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::StringType;
use databend_common_storages_parquet::serialize_row_group_meta_to_bytes;
use parquet::file::metadata::RowGroupMetaData;

use crate::pipelines::processors::transforms::aggregator::AggregateSerdeMeta;

pub struct SpilledPayload {
    pub bucket: isize,
    pub location: String,
    pub row_group: RowGroupMetaData,
}

pub enum AggregateMeta {
    Serialized(SerializedPayload),
    AggregatePayload(AggregatePayload),
    Partitioned {
        bucket: Option<isize>,
        data: PartitionedData,
    },
    BucketSpilled(SpilledPayload),
    Spilled(Vec<SpilledPayload>),
}

pub enum PartitionedData {
    Empty,
    Serialized(Vec<SerializedPayload>),
    AggregatePayload(Vec<AggregatePayload>),
    BucketSpilled(Vec<SpilledPayload>),
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
            PartitionedData::BucketSpilled(payloads) => Some(BlockProfileStatistics {
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
    BucketSpilled(SpilledPayload),
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
        }
    }
}

impl From<PartitionItem> for AggregateMeta {
    fn from(item: PartitionItem) -> Self {
        match item {
            PartitionItem::Serialized(payload) => AggregateMeta::Serialized(payload),
            PartitionItem::AggregatePayload(payload) => AggregateMeta::AggregatePayload(payload),
            PartitionItem::BucketSpilled(payload) => AggregateMeta::BucketSpilled(payload),
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
            PartitionItem::BucketSpilled(payload) => Some(BlockProfileStatistics {
                rows: payload.row_group.num_rows() as usize,
                bytes: payload.row_group.total_byte_size() as usize,
            }),
        }
    }

    pub fn serialize_mixed(data: Vec<PartitionItem>) -> Result<DataBlock> {
        if data.is_empty() {
            return Ok(DataBlock::empty());
        }

        let has_spilled = data
            .iter()
            .any(|item| matches!(item, PartitionItem::BucketSpilled(_)));
        let has_payload = data.iter().any(|item| {
            matches!(
                item,
                PartitionItem::Serialized(_) | PartitionItem::AggregatePayload(_)
            )
        });

        if has_spilled && has_payload {
            return Err(ErrorCode::Internal(
                "Partitioned meta cannot serialize mixed payload and spilled batches.",
            ));
        }

        if has_spilled {
            let bucket_num = data.len();
            let mut bucket_column = Vec::with_capacity(bucket_num);
            let mut row_group_column = Vec::with_capacity(bucket_num);
            let mut location_column = Vec::with_capacity(bucket_num);

            for item in data {
                let PartitionItem::BucketSpilled(payload) = item else {
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

            return data_block.add_meta(Some(AggregateSerdeMeta::create_spilled(
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
                PartitionItem::BucketSpilled(_) => unreachable!(),
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

    pub fn create_bucket_spilled(payload: SpilledPayload) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::BucketSpilled(payload))
    }

    pub fn create_spilled(payloads: Vec<SpilledPayload>) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::Spilled(payloads))
    }

    pub fn create_partitioned(bucket: Option<isize>, data: PartitionedData) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::Partitioned { bucket, data })
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
            AggregateMeta::AggregatePayload(_) => {
                f.debug_struct("AggregateMeta:AggregatePayload").finish()
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
            AggregateMeta::Partitioned { data, .. } => data.output_stats(),
            AggregateMeta::BucketSpilled(payload) => Some(BlockProfileStatistics {
                rows: payload.row_group.num_rows() as usize,
                bytes: payload.row_group.total_byte_size() as usize,
            }),
            AggregateMeta::Spilled(payloads) => Some(BlockProfileStatistics {
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
