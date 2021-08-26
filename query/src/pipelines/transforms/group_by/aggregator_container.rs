use common_datablocks::{HashMethod, HashMethodFixedKeys, HashMethodSerializer};
use common_datavalues::DFNumericType;
use std::fmt::Debug;
use std::hash::Hash;

use crate::common::{HashMap, HashTableEntity, KeyValueEntity, HashTableKeyable, HashTableIter, HashMapIterator};
use std::ptr::NonNull;
use std::alloc::Layout;
use bumpalo::Bump;

pub trait AggregatorDataState<Method: HashMethod> where Method::HashKey: HashTableKeyable {
    fn len(&self) -> usize;

    fn alloc_layout(&self, layout: Layout) -> NonNull<u8>;

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
    pub area: Bump,
    pub data: HashMap<T::Native, usize>,
}

impl<T> AggregatorDataState<HashMethodFixedKeys<T>> for NativeAggregatorDataContainer<T> where
    T: DFNumericType,
    T::Native: std::cmp::Eq + Hash + Clone + Debug,
    HashMethodFixedKeys<T>: HashMethod<HashKey=T::Native>,
    <HashMethodFixedKeys<T> as HashMethod>::HashKey: HashTableKeyable
{
    #[inline(always)]
    fn len(&self) -> usize {
        self.data.len()
    }

    #[inline(always)]
    fn alloc_layout(&self, layout: Layout) -> NonNull<u8> {
        self.area.alloc_layout(layout)
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

pub struct SerializedAggregatorDataContainer {}

impl AggregatorDataState<HashMethodSerializer> for SerializedAggregatorDataContainer {
    fn len(&self) -> usize {
        todo!()
    }

    fn alloc_layout(&self, layout: Layout) -> NonNull<u8> {
        todo!()
    }

    fn iter(&self) -> HashMapIterator<Vec<u8>, usize> {
        todo!()
    }

    fn insert_key(&mut self, key: &Vec<u8>, inserted: &mut bool) -> *mut KeyValueEntity<Vec<u8>, usize> {
        todo!()
    }
}

