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
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::BlockProfileStatistics;
use databend_common_expression::DataBlock;
use databend_common_expression::Payload;
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
    pub fn from_items(items: Vec<PartitionItem>) -> Self {
        if items.is_empty() {
            PartitionedData::Empty
        } else if items
            .iter()
            .all(|item| matches!(item, PartitionItem::AggregatePayload(_)))
        {
            PartitionedData::AggregatePayload(
                items
                    .into_iter()
                    .filter_map(|item| match item {
                        PartitionItem::AggregatePayload(payload) => Some(payload),
                        _ => None,
                    })
                    .collect(),
            )
        } else if items
            .iter()
            .all(|item| matches!(item, PartitionItem::Serialized(_)))
        {
            PartitionedData::Serialized(
                items
                    .into_iter()
                    .filter_map(|item| match item {
                        PartitionItem::Serialized(payload) => Some(payload),
                        _ => None,
                    })
                    .collect(),
            )
        } else if items
            .iter()
            .all(|item| matches!(item, PartitionItem::BucketSpilled(_)))
        {
            PartitionedData::BucketSpilled(
                items
                    .into_iter()
                    .filter_map(|item| match item {
                        PartitionItem::BucketSpilled(payload) => Some(payload),
                        _ => None,
                    })
                    .collect(),
            )
        } else {
            PartitionedData::Mixed(items)
        }
    }

    pub fn into_items(self) -> Vec<PartitionItem> {
        match self {
            PartitionedData::Empty => vec![],
            PartitionedData::Serialized(payloads) => payloads
                .into_iter()
                .map(PartitionItem::Serialized)
                .collect(),
            PartitionedData::AggregatePayload(payloads) => payloads
                .into_iter()
                .map(PartitionItem::AggregatePayload)
                .collect(),
            PartitionedData::BucketSpilled(payloads) => payloads
                .into_iter()
                .map(PartitionItem::BucketSpilled)
                .collect(),
            PartitionedData::Mixed(items) => items,
        }
    }

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

    /// Splits this item into parts of about `max_bytes` each without copying
    /// aggregate states: payloads move whole pages, serialized blocks are sliced.
    fn split(self, max_bytes: usize) -> Vec<PartitionItem> {
        match self {
            PartitionItem::AggregatePayload(payload) => {
                let AggregatePayload {
                    bucket,
                    payload,
                    max_partition_count,
                } = payload;
                payload
                    .split_by_pages(max_bytes)
                    .into_iter()
                    .map(|payload| {
                        PartitionItem::AggregatePayload(AggregatePayload {
                            bucket,
                            payload,
                            max_partition_count,
                        })
                    })
                    .collect()
            }
            PartitionItem::Serialized(payload) => {
                let rows = payload.data_block.num_rows();
                let bytes = payload.data_block.memory_size();
                if rows <= 1 || bytes <= max_bytes {
                    return vec![PartitionItem::Serialized(payload)];
                }
                let parts = bytes.div_ceil(max_bytes.max(1)).min(rows);
                let rows_per_part = rows.div_ceil(parts);
                payload
                    .data_block
                    .split_by_rows_no_tail(rows_per_part)
                    .into_iter()
                    .map(|data_block| {
                        PartitionItem::Serialized(SerializedPayload {
                            bucket: payload.bucket,
                            data_block,
                            max_partition_count: payload.max_partition_count,
                        })
                    })
                    .collect()
            }
            item @ PartitionItem::BucketSpilled(_) => vec![item],
        }
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

    fn into_partition_items(self) -> Vec<PartitionItem> {
        match self {
            AggregateMeta::Serialized(payload) => vec![PartitionItem::Serialized(payload)],
            AggregateMeta::AggregatePayload(payload) => {
                vec![PartitionItem::AggregatePayload(payload)]
            }
            AggregateMeta::BucketSpilled(payload) => vec![PartitionItem::BucketSpilled(payload)],
            AggregateMeta::Spilled(payloads) => payloads
                .into_iter()
                .map(PartitionItem::BucketSpilled)
                .collect(),
            AggregateMeta::Partitioned { data, .. } => data.into_items(),
        }
    }

    /// Takes all items out, leaving an empty `Partitioned` with the same bucket.
    fn take_partition_items(&mut self) -> (Option<isize>, Vec<PartitionItem>) {
        let bucket = match self {
            AggregateMeta::Partitioned { bucket, .. } => *bucket,
            _ => None,
        };
        let meta = std::mem::replace(self, AggregateMeta::Partitioned {
            bucket,
            data: PartitionedData::Empty,
        });
        (bucket, meta.into_partition_items())
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

    fn split(&mut self, max_bytes: usize) -> Result<Vec<BlockMetaInfoPtr>> {
        match self.output_stats() {
            Some(stats) if stats.bytes > max_bytes => {}
            _ => return Ok(vec![]),
        }

        let (bucket, items) = self.take_partition_items();
        let mut parts = vec![];
        let mut part = vec![];
        let mut part_bytes = 0;
        for item in items.into_iter().flat_map(|item| item.split(max_bytes)) {
            let bytes = item.output_stats().map_or(0, |stats| stats.bytes);
            if !part.is_empty() && part_bytes + bytes > max_bytes {
                parts.push(std::mem::take(&mut part));
                part_bytes = 0;
            }
            part_bytes += bytes;
            part.push(item);
        }
        parts.push(part);

        let mut parts = parts.into_iter().map(|items| AggregateMeta::Partitioned {
            bucket,
            data: PartitionedData::from_items(items),
        });
        // `parts` always holds at least one (possibly empty) part.
        *self = parts.next().unwrap();
        Ok(parts
            .map(|meta| Box::new(meta) as BlockMetaInfoPtr)
            .collect())
    }

    fn merge(&mut self, other: BlockMetaInfoPtr) -> Result<Option<BlockMetaInfoPtr>> {
        let other = match AggregateMeta::downcast_from_err(other) {
            Ok(other) => other,
            Err(other) => return Ok(Some(other)),
        };

        // Merge at the item level only: payloads of different partials own
        // different arenas, so they must not be combined into one payload.
        let (bucket, mut items) = self.take_partition_items();
        items.extend(other.into_partition_items());
        *self = AggregateMeta::Partitioned {
            bucket,
            data: PartitionedData::from_items(items),
        };
        Ok(None)
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
