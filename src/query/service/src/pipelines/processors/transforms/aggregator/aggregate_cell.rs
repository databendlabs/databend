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
use std::sync::Arc;

use databend_common_base::runtime::drop_guard;
use databend_common_hashtable::HashtableLike;

use crate::pipelines::processors::transforms::group_by::Area;
use crate::pipelines::processors::transforms::group_by::ArenaHolder;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;

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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
