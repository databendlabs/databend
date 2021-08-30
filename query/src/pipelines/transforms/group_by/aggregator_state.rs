// Copyright 2020 Datafuse Labs.
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

use std::alloc::Layout;
use std::fmt::Debug;
use std::hash::Hash;
use std::ptr::NonNull;

use bumpalo::Bump;
use common_datablocks::HashMethod;
use common_datablocks::HashMethodFixedKeys;
use common_datablocks::HashMethodSerializer;
use common_datavalues::DFPrimitiveType;

use crate::common::HashMap;
use crate::common::HashMapIterator;
use crate::common::HashTableEntity;
use crate::common::HashTableKeyable;
use crate::common::KeyValueEntity;
use crate::pipelines::transforms::group_by::keys_ref::KeysRef;

/// Aggregate state of the SELECT query, destroy when group by is completed.
///
/// It helps manage the following states:
///     - Aggregate data(HashMap or MergeSort set in future)
///     - Aggregate function state data memory pool
///     - Group by key data memory pool (if necessary)
pub trait AggregatorState<Method: HashMethod> {
    type HashKeyState: HashTableKeyable;

    fn len(&self) -> usize;

    fn alloc_layout(&self, layout: Layout) -> NonNull<u8>;

    fn iter(&self) -> HashMapIterator<Self::HashKeyState, usize>;

    fn insert_key(
        &mut self,
        key: &Method::HashKey,
        inserted: &mut bool,
    ) -> *mut KeyValueEntity<Self::HashKeyState, usize>;
}

// TODO: Optimize the type with length below 2
pub struct FixedKeysAggregatorState<T>
where
    T: DFPrimitiveType,
    T: std::cmp::Eq + Clone + Debug,
    HashMethodFixedKeys<T>: HashMethod<HashKey = T>,
    <HashMethodFixedKeys<T> as HashMethod>::HashKey: HashTableKeyable,
{
    pub area: Bump,
    pub data: HashMap<T, usize>,
}

impl<T> AggregatorState<HashMethodFixedKeys<T>> for FixedKeysAggregatorState<T>
where
    T: DFPrimitiveType,
    T: std::cmp::Eq + Hash + Clone + Debug,
    HashMethodFixedKeys<T>: HashMethod<HashKey = T>,
    <HashMethodFixedKeys<T> as HashMethod>::HashKey: HashTableKeyable,
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
    fn insert_key(
        &mut self,
        key: &<HashMethodFixedKeys<T> as HashMethod>::HashKey,
        inserted: &mut bool,
    ) -> *mut KeyValueEntity<<HashMethodFixedKeys<T> as HashMethod>::HashKey, usize> {
        self.data.insert_key(key, inserted)
    }
}

pub struct SerializedKeysAggregatorState {
    pub keys_area: Bump,
    pub state_area: Bump,
    pub data_state_map: HashMap<KeysRef, usize>,
}

impl AggregatorState<HashMethodSerializer> for SerializedKeysAggregatorState {
    type HashKeyState = KeysRef;

    fn len(&self) -> usize {
        self.data_state_map.len()
    }

    fn alloc_layout(&self, layout: Layout) -> NonNull<u8> {
        self.state_area.alloc_layout(layout)
    }

    fn iter(&self) -> HashMapIterator<KeysRef, usize> {
        self.data_state_map.iter()
    }

    fn insert_key(
        &mut self,
        keys: &Vec<u8>,
        inserted: &mut bool,
    ) -> *mut KeyValueEntity<KeysRef, usize> {
        let mut keys_ref = KeysRef::create(keys.as_ptr() as usize, keys.len());
        let state_entity = self.data_state_map.insert_key(&keys_ref, inserted);

        if *inserted {
            unsafe {
                // Keys will be destroyed after call we need copy the keys to the memory pool.
                let global_keys = self.keys_area.alloc_slice_copy(keys);
                let inserted_hash = state_entity.get_hash();
                keys_ref.address = global_keys.as_ptr() as usize;
                // TODO: maybe need set key method.
                state_entity.set_key_and_hash(&keys_ref, inserted_hash)
            }
        }

        state_entity
    }
}
