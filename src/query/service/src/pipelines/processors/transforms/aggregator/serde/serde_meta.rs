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

use std::ops::Range;

use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockMetaInfoPtr;

pub const BUCKET_TYPE: usize = 1;
pub const SPILLED_TYPE: usize = 2;
pub const NEW_SPILLED_TYPE: usize = 3;
pub const PARTITIONED_AGGREGATE_TYPE: usize = 4;

// Cannot change to enum, because bincode cannot deserialize custom enum
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct AggregateSerdeMeta {
    pub typ: usize,
    pub bucket: isize,
    pub location: Option<String>,
    pub data_range: Option<Range<u64>>,
    pub columns_layout: Vec<usize>,
    // use for new agg hashtable
    pub max_partition_count: usize,
    pub is_empty: bool,

    // used for PARTITIONED_AGGREGATE_TYPE
    pub buckets: Vec<isize>,
    pub payload_row_counts: Vec<usize>,

    // used for row/bucket shuffle
    // -1 for row shuffle
    pub shuffle_bucket: isize,
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
            location: None,
            data_range: None,
            columns_layout: vec![],
            max_partition_count,
            is_empty,
            buckets: vec![],
            payload_row_counts: vec![],
            shuffle_bucket: 0,
        })
    }

    pub fn create_spilled(
        bucket: isize,
        location: String,
        data_range: Range<u64>,
        columns_layout: Vec<usize>,
        is_empty: bool,
    ) -> BlockMetaInfoPtr {
        Box::new(AggregateSerdeMeta {
            typ: SPILLED_TYPE,
            bucket,
            columns_layout,
            location: Some(location),
            data_range: Some(data_range),
            max_partition_count: 0,
            is_empty,
            buckets: vec![],
            payload_row_counts: vec![],
            shuffle_bucket: 0,
        })
    }

    pub fn create_agg_spilled(
        bucket: isize,
        location: String,
        data_range: Range<u64>,
        columns_layout: Vec<usize>,
        max_partition_count: usize,
    ) -> BlockMetaInfoPtr {
        Box::new(AggregateSerdeMeta {
            typ: SPILLED_TYPE,
            bucket,
            columns_layout,
            location: Some(location),
            data_range: Some(data_range),
            max_partition_count,
            is_empty: false,
            buckets: vec![],
            payload_row_counts: vec![],
            shuffle_bucket: 0,
        })
    }

    pub fn create_new_spilled(shuffle_bucket: isize) -> BlockMetaInfoPtr {
        Box::new(AggregateSerdeMeta {
            typ: NEW_SPILLED_TYPE,
            bucket: 0,
            columns_layout: vec![],
            location: None,
            data_range: None,
            max_partition_count: 0,
            is_empty: false,
            buckets: vec![],
            payload_row_counts: vec![],
            shuffle_bucket,
        })
    }

    pub fn create_partitioned_payload(
        buckets: Vec<isize>,
        payload_row_counts: Vec<usize>,
    ) -> BlockMetaInfoPtr {
        Box::new(AggregateSerdeMeta {
            typ: PARTITIONED_AGGREGATE_TYPE,
            bucket: 0,
            columns_layout: vec![],
            location: None,
            data_range: None,
            max_partition_count: 0,
            is_empty: false,
            buckets,
            payload_row_counts,
            shuffle_bucket: 0,
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
