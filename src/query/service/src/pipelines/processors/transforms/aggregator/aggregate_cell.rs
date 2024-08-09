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
use std::marker::PhantomData;
use std::sync::Arc;

use databend_common_base::runtime::drop_guard;
use databend_common_functions::aggregates::StateAddr;
use databend_common_hashtable::HashtableEntryRefLike;
use databend_common_hashtable::HashtableLike;

use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::group_by::Area;
use crate::pipelines::processors::transforms::group_by::ArenaHolder;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::PartitionedHashMethod;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;

// Manage unsafe memory usage, free memory when the cell is destroyed.
pub struct HashTableCell<T: HashMethodBounds, V: Send + Sync + 'static> {
    pub hashtable: T::HashTable<V>,
    pub arena: Area,
    pub arena_holders: Vec<ArenaHolder>,
    pub _dropper: Option<Arc<dyn HashTableDropper<T, V>>>,
}

unsafe impl<T: HashMethodBounds, V: Send + Sync + 'static> Send for HashTableCell<T, V> {}

unsafe impl<T: HashMethodBounds, V: Send + Sync + 'static> Sync for HashTableCell<T, V> {}

impl<T: HashMethodBounds, V: Send + Sync + 'static> Drop for HashTableCell<T, V> {
    fn drop(&mut self) {
        drop_guard(move || {
            if let Some(dropper) = self._dropper.take() {
                dropper.destroy(&mut self.hashtable);
            }
        })
    }
}

impl<T: HashMethodBounds, V: Send + Sync + 'static> HashTableCell<T, V> {
    pub fn create(
        inner: T::HashTable<V>,
        _dropper: Arc<dyn HashTableDropper<T, V>>,
    ) -> HashTableCell<T, V> {
        HashTableCell::<T, V> {
            hashtable: inner,
            arena_holders: vec![],
            _dropper: Some(_dropper),
            arena: Area::create(),
        }
    }

    pub fn len(&self) -> usize {
        self.hashtable.len()
    }

    pub fn allocated_bytes(&self) -> usize {
        self.hashtable.bytes_len(false)
            + self.arena.allocated_bytes()
            + self
                .arena_holders
                .iter()
                .map(ArenaHolder::allocated_bytes)
                .sum::<usize>()
    }
}

pub trait HashTableDropper<T: HashMethodBounds, V: Send + Sync + 'static> {
    fn as_any(&self) -> &dyn Any;
    fn destroy(&self, hashtable: &mut T::HashTable<V>);
}

pub struct GroupByHashTableDropper<T: HashMethodBounds> {
    _phantom: PhantomData<T>,
}

impl<T: HashMethodBounds> GroupByHashTableDropper<T> {
    pub fn create() -> Arc<dyn HashTableDropper<T, ()>> {
        Arc::new(GroupByHashTableDropper::<T> {
            _phantom: Default::default(),
        })
    }
}

impl<T: HashMethodBounds> HashTableDropper<T, ()> for GroupByHashTableDropper<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn destroy(&self, _: &mut T::HashTable<()>) {
        // do nothing
    }
}

pub struct AggregateHashTableDropper<T: HashMethodBounds> {
    params: Arc<AggregatorParams>,
    _phantom: PhantomData<T>,
}

impl<T: HashMethodBounds> AggregateHashTableDropper<T> {
    pub fn create(params: Arc<AggregatorParams>) -> Arc<dyn HashTableDropper<T, usize>> {
        Arc::new(AggregateHashTableDropper::<T> {
            params,
            _phantom: Default::default(),
        })
    }
}

impl<T: HashMethodBounds> HashTableDropper<T, usize> for AggregateHashTableDropper<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn destroy(&self, hashtable: &mut T::HashTable<usize>) {
        let aggregator_params = self.params.as_ref();
        let aggregate_functions = &aggregator_params.aggregate_functions;
        let offsets_aggregate_states = &aggregator_params.offsets_aggregate_states;

        let functions = aggregate_functions
            .iter()
            .filter(|p| p.need_manual_drop_state())
            .collect::<Vec<_>>();

        let state_offsets = offsets_aggregate_states
            .iter()
            .enumerate()
            .filter(|(idx, _)| aggregate_functions[*idx].need_manual_drop_state())
            .map(|(_, s)| *s)
            .collect::<Vec<_>>();

        if !state_offsets.is_empty() {
            for group_entity in hashtable.iter() {
                let place = Into::<StateAddr>::into(*group_entity.get());

                for (function, state_offset) in functions.iter().zip(state_offsets.iter()) {
                    unsafe { function.drop_state(place.next(*state_offset)) }
                }
            }
        }
    }
}

pub struct PartitionedHashTableDropper<Method: HashMethodBounds, V: Send + Sync + 'static> {
    _inner_dropper: Arc<dyn HashTableDropper<Method, V>>,
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static> PartitionedHashTableDropper<Method, V> {
    pub fn create(
        _inner_dropper: Arc<dyn HashTableDropper<Method, V>>,
    ) -> Arc<dyn HashTableDropper<PartitionedHashMethod<Method>, V>> {
        Arc::new(Self { _inner_dropper })
    }

    pub fn split_cell(
        mut v: HashTableCell<PartitionedHashMethod<Method>, V>,
    ) -> Vec<HashTableCell<Method, V>> {
        unsafe {
            let arena = std::mem::replace(&mut v.arena, Area::create());
            v.arena_holders.push(ArenaHolder::create(Some(arena)));

            let dropper = v
                ._dropper
                .as_ref()
                .unwrap()
                .as_any()
                .downcast_ref::<PartitionedHashTableDropper<Method, V>>()
                .unwrap()
                ._inner_dropper
                .clone();

            let mut cells = Vec::with_capacity(256);
            while let Some(table) = v.hashtable.pop_first_inner_table() {
                let mut table_cell = HashTableCell::create(table, dropper.clone());
                table_cell.arena_holders = v.arena_holders.to_vec();
                cells.push(table_cell);
            }

            cells
        }
    }
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static>
    HashTableDropper<PartitionedHashMethod<Method>, V> for PartitionedHashTableDropper<Method, V>
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn destroy(
        &self,
        hashtable: &mut <PartitionedHashMethod<Method> as PolymorphicKeysHelper<
            PartitionedHashMethod<Method>,
        >>::HashTable<V>,
    ) {
        for inner_table in hashtable.iter_tables_mut() {
            self._inner_dropper.destroy(inner_table)
        }
    }
}
