use common_datablocks::{HashMethod, HashMethodFixedKeys};
use common_datavalues::DFNumericType;
use std::fmt::Debug;
use std::hash::Hash;

use crate::common::{HashMap, HashTableEntity, KeyValueEntity, HashTableKeyable, HashTableIter, HashMapIterator};

pub trait AggregatorDataState<Method: HashMethod> where Method::HashKey: HashTableKeyable {
    fn len(&self) -> usize;

    fn iter(&self) -> HashMapIterator<Method::HashKey, usize>;

    fn insert_key(&mut self, key: &Method::HashKey, inserted: &mut bool) -> *mut KeyValueEntity<Method::HashKey, usize>;
}

// TODO: Optimize the type with length below 2
pub struct NativeAggregatorDataContainer<T> where
    T: DFNumericType,
    T::Native: std::cmp::Eq + Clone + Debug,
    HashMethodFixedKeys<T>: HashMethod<HashKey=T::Native>,
    <HashMethodFixedKeys<T> as HashMethod>::HashKey: HashTableKeyable
{
    pub data: HashMap<T::Native, usize>,
}

impl<T> AggregatorDataState<HashMethodFixedKeys<T>> for NativeAggregatorDataContainer<T> where
    T: DFNumericType,
    T::Native: std::cmp::Eq + Hash + Clone + Debug,
    HashMethodFixedKeys<T>: HashMethod<HashKey=T::Native>,
    <HashMethodFixedKeys<T> as HashMethod>::HashKey: HashTableKeyable
{
    #[inline]
    fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    fn iter(&self) -> HashMapIterator<<HashMethodFixedKeys<T> as HashMethod>::HashKey, usize> {
        self.data.iter()
    }

    #[inline]
    fn insert_key(&mut self, key: &<HashMethodFixedKeys<T> as HashMethod>::HashKey, inserted: &mut bool) -> *mut KeyValueEntity<<HashMethodFixedKeys<T> as HashMethod>::HashKey, usize> {
        self.data.insert_key(key, inserted)
    }
}
