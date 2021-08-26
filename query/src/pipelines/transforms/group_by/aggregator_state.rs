use common_datablocks::{HashMethod, HashMethodFixedKeys, HashMethodSerializer};
use common_datavalues::DFNumericType;
use std::fmt::Debug;
use std::hash::Hash;

use crate::common::{HashMap, HashTableEntity, KeyValueEntity, HashTableKeyable, HashTableIter, HashMapIterator};
use std::ptr::NonNull;
use std::alloc::Layout;
use bumpalo::Bump;
use crate::pipelines::transforms::group_by::keys_ref::KeysRef;

pub trait AggregatorState<Method: HashMethod> {
    type HashKeyState: HashTableKeyable;

    fn len(&self) -> usize;

    fn alloc_layout(&self, layout: Layout) -> NonNull<u8>;

    fn iter(&self) -> HashMapIterator<Self::HashKeyState, usize>;

    fn insert_key(&mut self, key: &Method::HashKey, inserted: &mut bool) -> *mut KeyValueEntity<Self::HashKeyState, usize>;
}

// TODO: Optimize the type with length below 2
pub struct FixedKeysAggregatorState<T> where
    T: DFNumericType,
    T::Native: std::cmp::Eq + Clone + Debug,
    HashMethodFixedKeys<T>: HashMethod<HashKey=T::Native>,
    <HashMethodFixedKeys<T> as HashMethod>::HashKey: HashTableKeyable
{
    pub area: Bump,
    pub data: HashMap<T::Native, usize>,
}

impl<T> AggregatorState<HashMethodFixedKeys<T>> for FixedKeysAggregatorState<T> where
    T: DFNumericType,
    T::Native: std::cmp::Eq + Hash + Clone + Debug,
    HashMethodFixedKeys<T>: HashMethod<HashKey=T::Native>,
    <HashMethodFixedKeys<T> as HashMethod>::HashKey: HashTableKeyable
{
    type HashKeyState = <HashMethodFixedKeys<T> as HashMethod>::HashKey;

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

pub struct SerializedKeysAggregatorState {
    pub area: Bump,
    pub data: HashMap<KeysRef, usize>,
}

impl AggregatorState<HashMethodSerializer> for SerializedKeysAggregatorState {
    type HashKeyState = KeysRef;

    fn len(&self) -> usize {
        self.data.len()
    }

    fn alloc_layout(&self, layout: Layout) -> NonNull<u8> {
        self.area.alloc_layout(layout)
    }

    fn iter(&self) -> HashMapIterator<KeysRef, usize> {
        self.data.iter()
    }

    fn insert_key(&mut self, key: &Vec<u8>, inserted: &mut bool) -> *mut KeyValueEntity<KeysRef, usize> {
        // TODO: move key to global area if inserted. Otherwise, do nothing
        unimplemented!()
    }
}

