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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::PartitionedData;
use crate::pipelines::processors::transforms::aggregator::SerializedPayload;

pub const BUCKET_TYPE: usize = 1;

// Cannot change to enum, because bincode cannot deserialize custom enum.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct AggregateSerdeMeta {
    pub typ: usize,
    pub bucket: isize,
    pub max_partition_count: usize,
    pub is_empty: bool,
}

impl AggregateSerdeMeta {
    pub fn create_agg_payload(
        bucket: isize,
        max_partition_count: usize,
        is_empty: bool,
    ) -> BlockMetaInfoPtr {
        Box::new(AggregateSerdeMeta {
            typ: BUCKET_TYPE,
            bucket,
            max_partition_count,
            is_empty,
        })
    }
}

#[typetag::serde(name = "aggregate_serde")]
impl BlockMetaInfo for AggregateSerdeMeta {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        AggregateSerdeMeta::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

/// Serialized partial aggregate states of several buckets, concatenated in one block, sent to
/// a remote node through the hash shuffle. The shuffle decodes blocks with the schema of the raw
/// rows by default, so the states carry their own schema.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct AggregateStatesMeta {
    pub schema: DataSchemaRef,
    pub buckets: Vec<isize>,
    pub payload_row_counts: Vec<usize>,
}

impl AggregateStatesMeta {
    pub fn create(
        schema: DataSchemaRef,
        buckets: Vec<isize>,
        payload_row_counts: Vec<usize>,
    ) -> BlockMetaInfoPtr {
        Box::new(AggregateStatesMeta {
            schema,
            buckets,
            payload_row_counts,
        })
    }

    /// Split `block` back into the serialized payload of each bucket.
    pub fn into_aggregate_meta(self, block: DataBlock) -> Result<AggregateMeta> {
        if self.buckets.len() != self.payload_row_counts.len() {
            return Err(ErrorCode::Internal(
                "Invalid aggregate states meta".to_string(),
            ));
        }

        let mut offset = 0;
        let mut payloads = Vec::with_capacity(self.buckets.len());
        for (bucket, rows) in self.buckets.into_iter().zip(self.payload_row_counts) {
            let start = offset;
            offset += rows;
            if offset > block.num_rows() {
                return Err(ErrorCode::Internal(
                    "Aggregate states rows exceed block rows".to_string(),
                ));
            }
            payloads.push(SerializedPayload {
                bucket,
                data_block: block.slice(start..offset),
                max_partition_count: 0,
            });
        }

        if offset != block.num_rows() {
            return Err(ErrorCode::Internal(
                "Aggregate states rows do not match block rows".to_string(),
            ));
        }
        Ok(AggregateMeta::Partitioned {
            bucket: None,
            data: PartitionedData::Serialized(payloads),
        })
    }
}

#[typetag::serde(name = "aggregate_states")]
impl BlockMetaInfo for AggregateStatesMeta {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        AggregateStatesMeta::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }

    fn override_block_schema(&self) -> Option<DataSchemaRef> {
        Some(self.schema.clone())
    }
}
