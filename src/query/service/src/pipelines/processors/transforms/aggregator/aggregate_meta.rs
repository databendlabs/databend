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

use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::ops::Range;

use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoPtr;
use common_expression::Column;
use common_expression::DataBlock;

use crate::pipelines::processors::transforms::group_by::ArenaHolder;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::PartitionedHashMethod;
use crate::pipelines::processors::transforms::HashTableCell;

pub struct HashTablePayload<T: HashMethodBounds, V: Send + Sync + 'static> {
    pub bucket: isize,
    pub cell: HashTableCell<T, V>,
    pub arena_holder: ArenaHolder,
}

pub struct SerializedPayload {
    pub bucket: isize,
    pub data_block: DataBlock,
}

impl SerializedPayload {
    pub fn get_group_by_column(&self) -> &Column {
        let entry = self.data_block.columns().last().unwrap();
        entry.value.as_column().unwrap()
    }
}

pub struct BucketSpilledPayload {
    pub bucket: isize,
    pub location: String,
    pub data_range: Range<u64>,
    pub columns_layout: Vec<u64>,
}

pub enum AggregateMeta<Method: HashMethodBounds, V: Send + Sync + 'static> {
    Serialized(SerializedPayload),
    HashTable(HashTablePayload<Method, V>),
    BucketSpilled(BucketSpilledPayload),
    Spilled(Vec<BucketSpilledPayload>),
    Spilling(HashTablePayload<PartitionedHashMethod<Method>, V>),

    Partitioned { bucket: isize, data: Vec<Self> },
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static> AggregateMeta<Method, V> {
    pub fn create_hashtable(bucket: isize, cell: HashTableCell<Method, V>) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::<Method, V>::HashTable(HashTablePayload {
            cell,
            bucket,
            arena_holder: ArenaHolder::create(None),
        }))
    }

    pub fn create_serialized(bucket: isize, block: DataBlock) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::<Method, V>::Serialized(SerializedPayload {
            bucket,
            data_block: block,
        }))
    }

    pub fn create_spilling(
        cell: HashTableCell<PartitionedHashMethod<Method>, V>,
    ) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::<Method, V>::Spilling(HashTablePayload {
            cell,
            bucket: 0,
            arena_holder: ArenaHolder::create(None),
        }))
    }

    pub fn create_spilled(buckets_payload: Vec<BucketSpilledPayload>) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::<Method, V>::Spilled(buckets_payload))
    }

    pub fn create_bucket_spilled(payload: BucketSpilledPayload) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::<Method, V>::BucketSpilled(payload))
    }

    pub fn create_partitioned(bucket: isize, data: Vec<Self>) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::<Method, V>::Partitioned { data, bucket })
    }
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static> serde::Serialize
    for AggregateMeta<Method, V>
{
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unreachable!("AggregateMeta does not support exchanging between multiple nodes")
    }
}

impl<'de, Method: HashMethodBounds, V: Send + Sync + 'static> serde::Deserialize<'de>
    for AggregateMeta<Method, V>
{
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unreachable!("AggregateMeta does not support exchanging between multiple nodes")
    }
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static> Debug for AggregateMeta<Method, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregateMeta::HashTable(_) => f.debug_struct("AggregateMeta::HashTable").finish(),
            AggregateMeta::Partitioned { .. } => {
                f.debug_struct("AggregateMeta::Partitioned").finish()
            }
            AggregateMeta::Serialized { .. } => {
                f.debug_struct("AggregateMeta::Serialized").finish()
            }
            AggregateMeta::Spilling(_) => f.debug_struct("Aggregate::Spilling").finish(),
            AggregateMeta::Spilled(_) => f.debug_struct("Aggregate::Spilling").finish(),
            AggregateMeta::BucketSpilled(_) => f.debug_struct("Aggregate::BucketSpilled").finish(),
        }
    }
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static> BlockMetaInfo
    for AggregateMeta<Method, V>
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn typetag_deserialize(&self) {
        unimplemented!("AggregateMeta does not support exchanging between multiple nodes")
    }

    fn typetag_name(&self) -> &'static str {
        unimplemented!("AggregateMeta does not support exchanging between multiple nodes")
    }

    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals for AggregateMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone for AggregateMeta")
    }
}
