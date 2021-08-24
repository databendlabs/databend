use common_datablocks::{HashMethod, HashMethodFixedKeys};
use common_datavalues::DFNumericType;
use std::fmt::Debug;
use std::hash::Hash;

use crate::common::{HashMap, HashTableEntity, FastHash, KeyValueEntity, HashTableKeyable, HashTableIter, HashMapIterator};

pub trait AggregatorDataContainer<Method: HashMethod> where Method::HashKey: HashTableKeyable {
    fn size(&self) -> usize;

    fn iter(&self) -> HashMapIterator<Method::HashKey, usize>;

    fn insert_key(&mut self, key: &Method::HashKey, inserted: &mut bool) -> *mut KeyValueEntity<Method::HashKey, usize>;
}

// TODO: Optimize the type with length below 2
pub struct NativeAggregatorDataContainer<T> where
    T: DFNumericType,
    T::Native: std::cmp::Eq + FastHash + Clone + Debug,
    HashMethodFixedKeys<T>: HashMethod<HashKey=T::Native>,
    <HashMethodFixedKeys<T> as HashMethod>::HashKey: HashTableKeyable
{
    pub data: HashMap<T::Native, usize>,
}

impl<T> AggregatorDataContainer<HashMethodFixedKeys<T>> for NativeAggregatorDataContainer<T> where
    T: DFNumericType,
    T::Native: std::cmp::Eq + Hash + Clone + Debug,
    HashMethodFixedKeys<T>: HashMethod<HashKey=T::Native>,
    <HashMethodFixedKeys<T> as HashMethod>::HashKey: HashTableKeyable
{
    #[inline(always)]
    fn size(&self) -> usize {
        self.data.len()
    }

    #[inline(always)]
    fn iter(&self) -> HashMapIterator<<HashMethodFixedKeys<T> as HashMethod>::HashKey, usize> {
        self.data.iter()
    }

    #[inline(always)]
    fn insert_key(&mut self, key: &<HashMethodFixedKeys<T> as HashMethod>::HashKey, inserted: &mut bool) -> *mut KeyValueEntity<<HashMethodFixedKeys<T> as HashMethod>::HashKey, usize> {
        self.data.insert_key(key, inserted)
    }
}
