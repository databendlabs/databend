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

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::ops::Range;

use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;

pub struct WindowPayload {
    pub bucket: isize,
    pub data: DataBlock,
}

pub struct SpillingWindowPayloads {
    pub data: BTreeMap<usize, Vec<DataBlock>>,
}

pub struct BucketSpilledWindowPayload {
    pub bucket: isize,
    pub location: String,
    pub data_range: Range<u64>,
    pub columns_layout: Vec<u64>,
}

pub enum WindowPartitionMeta {
    Spilling(SpillingWindowPayloads),
    Spilled(Vec<BucketSpilledWindowPayload>),
    BucketSpilled(BucketSpilledWindowPayload),
    Payload(WindowPayload),

    Partitioned { bucket: isize, data: Vec<Self> },
}

impl WindowPartitionMeta {
    pub fn create_payload(bucket: isize, data: DataBlock) -> BlockMetaInfoPtr {
        Box::new(WindowPartitionMeta::Payload(WindowPayload { bucket, data }))
    }

    pub fn create_spilling(data: BTreeMap<usize, Vec<DataBlock>>) -> BlockMetaInfoPtr {
        Box::new(WindowPartitionMeta::Spilling(SpillingWindowPayloads {
            data,
        }))
    }

    pub fn create_spilled(buckets_payload: Vec<BucketSpilledWindowPayload>) -> BlockMetaInfoPtr {
        Box::new(WindowPartitionMeta::Spilled(buckets_payload))
    }

    pub fn create_bucket_spilled(payload: BucketSpilledWindowPayload) -> BlockMetaInfoPtr {
        Box::new(WindowPartitionMeta::BucketSpilled(payload))
    }

    pub fn create_partitioned(bucket: isize, data: Vec<Self>) -> BlockMetaInfoPtr {
        Box::new(WindowPartitionMeta::Partitioned { bucket, data })
    }
}

impl serde::Serialize for WindowPartitionMeta {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unreachable!("WindowPartitionMeta does not support exchanging between multiple nodes")
    }
}

impl<'de> serde::Deserialize<'de> for WindowPartitionMeta {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unreachable!("WindowPartitionMeta does not support exchanging between multiple nodes")
    }
}

impl Debug for WindowPartitionMeta {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            WindowPartitionMeta::Payload(_) => {
                f.debug_struct("WindowPartitionMeta::Bucket").finish()
            }
            WindowPartitionMeta::Spilling(_) => {
                f.debug_struct("WindowPartitionMeta::Spilling").finish()
            }
            WindowPartitionMeta::Spilled(_) => {
                f.debug_struct("WindowPartitionMeta::Spilled").finish()
            }
            WindowPartitionMeta::BucketSpilled(_) => f
                .debug_struct("WindowPartitionMeta::BucketSpilled")
                .finish(),
            WindowPartitionMeta::Partitioned { .. } => {
                f.debug_struct("WindowPartitionMeta::Partitioned").finish()
            }
        }
    }
}

impl BlockMetaInfo for WindowPartitionMeta {
    fn typetag_deserialize(&self) {
        unimplemented!("WindowPartitionMeta does not support exchanging between multiple nodes")
    }

    fn typetag_name(&self) -> &'static str {
        unimplemented!("WindowPartitionMeta does not support exchanging between multiple nodes")
    }

    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals for WindowPartitionMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone for WindowPartitionMeta")
    }
}
