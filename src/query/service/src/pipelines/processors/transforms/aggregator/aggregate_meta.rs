use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;

use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoPtr;
use common_hashtable::HashtableLike;

use crate::pipelines::processors::transforms::group_by::ArenaHolder;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::PartitionedHashMethod;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;

pub struct HashTablePayload<T: HashtableLike> {
    inner: T,
    arena_holder: ArenaHolder,
}

pub enum AggregateMeta<Method: HashMethodBounds, V: Send + Sync + 'static> {
    HashTable(HashTablePayload<Method::HashTable<V>>),
    PartitionedHashTable(
        HashTablePayload<
            <PartitionedHashMethod<Method> as PolymorphicKeysHelper<
                PartitionedHashMethod<Method>,
            >>::HashTable<V>,
        >,
    ),
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static> AggregateMeta<Method, V> {
    pub fn create_hashtable(
        v: Method::HashTable<V>,
        arena_holder: ArenaHolder,
    ) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::<Method, V>::HashTable(HashTablePayload {
            inner: v,
            arena_holder,
        }))
    }

    pub fn create_partitioned_hashtable(
        hashtable: <PartitionedHashMethod<Method> as PolymorphicKeysHelper<
            PartitionedHashMethod<Method>,
        >>::HashTable<V>,
        arena_holder: ArenaHolder,
    ) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::<Method, V>::PartitionedHashTable(
            HashTablePayload {
                arena_holder,
                inner: hashtable,
            },
        ))
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
            AggregateMeta::PartitionedHashTable(_) => f
                .debug_struct("AggregateMeta::PartitionedHashTable")
                .finish(),
        }
    }
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static> BlockMetaInfo
    for AggregateMeta<Method, V>
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn typetag_deserialize(&self) {
        unimplemented!("AggregateMeta does not support exchanging between multiple nodes")
    }

    fn typetag_name(&self) -> &'static str {
        unimplemented!("AggregateMeta does not support exchanging between multiple nodes")
    }

    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        match self {
            AggregateMeta::HashTable(_) => {
                unimplemented!("Unimplemented equals for AggregateMeta::HashTable")
            }
            AggregateMeta::PartitionedHashTable(_) => {
                unimplemented!("Unimplemented equals for AggregateMeta::PartitionedHashTable")
            }
        }
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        match self {
            AggregateMeta::HashTable(_) => {
                unimplemented!("Unimplemented clone for AggregateMeta::HashTable")
            }
            AggregateMeta::PartitionedHashTable(_) => {
                unimplemented!("Unimplemented clone for AggregateMeta::PartitionedHashTable")
            }
        }
    }
}
