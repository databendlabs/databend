use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;

use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoPtr;
use common_expression::Column;
use common_expression::DataBlock;
use common_hashtable::HashtableLike;

use crate::pipelines::processors::transforms::group_by::ArenaHolder;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::PartitionedHashMethod;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;

pub struct HashTablePayload<T: HashtableLike> {
    pub hashtable: T,
    pub bucket: isize,
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

pub enum AggregateMeta<Method: HashMethodBounds, V: Send + Sync + 'static> {
    Serialized(SerializedPayload),
    HashTable(HashTablePayload<Method::HashTable<V>>),

    Partitioned { bucket: isize, data: Vec<Self> },
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static> AggregateMeta<Method, V> {
    pub fn create_hashtable(
        bucket: isize,
        hashtable: Method::HashTable<V>,
        arena_holder: ArenaHolder,
    ) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::<Method, V>::HashTable(HashTablePayload {
            bucket,
            hashtable,
            arena_holder,
        }))
    }

    pub fn create_serialized(bucket: isize, block: DataBlock) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::<Method, V>::Serialized(SerializedPayload {
            bucket,
            data_block: block,
        }))
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
