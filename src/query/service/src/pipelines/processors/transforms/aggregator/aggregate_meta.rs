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

use databend_common_expression::AggregatePayload;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::BlockProfileStatistics;
use databend_common_expression::DataBlock;
use databend_common_expression::SerializedPayload;
use parquet::file::metadata::RowGroupMetaData;

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
    Serialized(Vec<SerializedPayload>),
    AggregatePayload(Vec<AggregatePayload>),
    BucketSpilled(Vec<SpilledPayload>),
}

impl Debug for PartitionedData {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            PartitionedData::Serialized(_) => f
                .debug_struct("PartitionedAggregateData::Serialized")
                .finish(),
            PartitionedData::AggregatePayload(_) => f
                .debug_struct("PartitionedAggregateData::AggregatePayload")
                .finish(),
            PartitionedData::BucketSpilled(_) => f
                .debug_struct("PartitionedAggregateData::BucketSpilled")
                .finish(),
        }
    }
}

impl PartitionedData {
    fn output_stats(&self) -> Option<BlockProfileStatistics> {
        match self {
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
        }
    }
}

impl AggregateMeta {
    pub fn create_serialized(bucket: isize, block: DataBlock) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::Serialized(SerializedPayload {
            bucket,
            data_block: block,
        }))
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
