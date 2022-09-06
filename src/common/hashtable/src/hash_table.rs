// Copyright 2021 Datafuse Labs.
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
//
// Reference the ClickHouse HashTable to implement the Databend HashTable

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::marker::PhantomData;
use std::mem;

use common_base::mem_allocator::Allocator as AllocatorTrait;
use common_base::mem_allocator::ALLOC;

use crate::hash_table_grower::HashTableGrower;
use crate::HashTableEntity;
use crate::HashTableIter;
use crate::HashTableIteratorKind;
use crate::HashTableKeyable;

pub struct HashTable<
    Key: HashTableKeyable,
    Entity: HashTableEntity<Key>,
    Grower: HashTableGrower,
    Allocator: AllocatorTrait,
> {
    size: usize,
    grower: Grower,
    allocator: Allocator,
    entities: *mut Entity,
    entities_raw: *mut u8,
    zero_entity: Option<*mut Entity>,
    zero_entity_raw: Option<*mut u8>,

    // set to true if the table is converted to other hash table
    pub(crate) entity_swapped: bool,
    /// Generics hold
    generics_hold: PhantomData<Key>,
}

unsafe impl<
    Key: HashTableKeyable + Send,
    Entity: HashTableEntity<Key> + Send,
    Grower: HashTableGrower,
    Allocator: AllocatorTrait,
> Send for HashTable<Key, Entity, Grower, Allocator>
{
}

unsafe impl<
    Key: HashTableKeyable + Sync,
    Entity: HashTableEntity<Key> + Sync,
    Grower: HashTableGrower,
    Allocator: AllocatorTrait,
> Sync for HashTable<Key, Entity, Grower, Allocator>
{
}

impl<
    Key: HashTableKeyable,
    Entity: HashTableEntity<Key>,
    Grower: HashTableGrower,
    Allocator: AllocatorTrait,
> Drop for HashTable<Key, Entity, Grower, Allocator>
{
    fn drop(&mut self) {
        unsafe {
            let item_size = self.grower.max_size() as usize;

            if std::mem::needs_drop::<Entity>() && !self.entity_swapped {
                for off in 0..item_size {
                    let entity = self.entities.add(off);

                    if !entity.is_zero() {
                        std::ptr::drop_in_place(entity);
                    }
                }
            }

            let size = item_size * mem::size_of::<Entity>();
            let layout = Layout::from_size_align_unchecked(size, std::mem::align_of::<Entity>());

            self.allocator.deallocx(self.entities_raw, layout);
            if let Some(zero_entity) = self.zero_entity_raw {
                if std::mem::needs_drop::<Entity>() && !self.entity_swapped {
                    let entity = self.zero_entity.unwrap();
                    std::ptr::drop_in_place(entity);
                }

                let zero_layout = Layout::from_size_align_unchecked(
                    mem::size_of::<Entity>(),
                    std::mem::align_of::<Entity>(),
                );
                ALLOC.dealloc(zero_entity, zero_layout);
            }
        }
    }
}

impl<
    Key: HashTableKeyable,
    Entity: HashTableEntity<Key>,
    Grower: HashTableGrower,
    Allocator: AllocatorTrait + Default,
> HashTable<Key, Entity, Grower, Allocator>
{
    pub fn create() -> HashTable<Key, Entity, Grower, Allocator> {
        Self::with_capacity(1 << 8)
    }

    pub fn with_capacity(capacity: usize) -> HashTable<Key, Entity, Grower, Allocator> {
        let mut grower = Grower::default();
        while (grower.max_size() as usize) < capacity {
            grower.increase_size();
        }

        let size = grower.max_size() as usize * mem::size_of::<Entity>();
        unsafe {
            let layout = Layout::from_size_align_unchecked(size, mem::align_of::<Entity>());
            let mut allocator = Allocator::default();
            let raw_ptr = allocator.allocx(layout, true);

            if raw_ptr.is_null() {
                panic!(
                    "Failed to have enough memory to alloc {size} bytes to initial the hashtable"
                );
            }

            let entities_ptr = raw_ptr as *mut Entity;
            HashTable {
                size: 0,
                grower,
                entities: entities_ptr,
                entities_raw: raw_ptr,
                zero_entity: None,
                zero_entity_raw: None,
                generics_hold: PhantomData::default(),
                entity_swapped: false,
                allocator,
            }
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.size
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    #[inline(always)]
    pub fn enum_iter(&self) -> HashTableIteratorKind<Key, Entity> {
        HashTableIteratorKind::create_hash_table_iter(
            self.grower.max_size(),
            self.entities,
            self.zero_entity,
        )
    }

    #[inline(always)]
    pub fn iter(&self) -> HashTableIter<Key, Entity> {
        HashTableIter::<Key, Entity>::create(
            self.grower.max_size(),
            self.entities,
            self.zero_entity,
        )
    }

    #[inline(always)]
    pub fn insert_key(&mut self, key: &Key, inserted: &mut bool) -> *mut Entity {
        let hash = key.fast_hash();
        match self.insert_if_zero_key(key, hash, inserted) {
            None => self.insert_non_zero_key(key, hash, inserted),
            Some(zero_hash_table_entity) => zero_hash_table_entity,
        }
    }

    #[inline(always)]
    pub fn insert_hash_key(&mut self, key: &Key, hash: u64, inserted: &mut bool) -> *mut Entity {
        match self.insert_if_zero_key(key, hash, inserted) {
            None => self.insert_non_zero_key(key, hash, inserted),
            Some(zero_hash_table_entity) => zero_hash_table_entity,
        }
    }

    #[inline(always)]
    pub fn find_key(&self, key: &Key) -> Option<*mut Entity> {
        if !key.is_zero() {
            let hash_value = key.fast_hash();
            let place_value = self.find_entity(key, hash_value);
            unsafe {
                let value = self.entities.offset(place_value);
                return match value.is_zero() {
                    true => None,
                    false => Some(value),
                };
            }
        }

        self.zero_entity
    }

    #[inline(always)]
    fn find_entity(&self, key: &Key, hash_value: u64) -> isize {
        unsafe {
            let grower = &self.grower;

            let mut place_value = grower.place(hash_value);

            while !self.entities.offset(place_value).is_zero()
                && !self
                    .entities
                    .offset(place_value)
                    .key_equals(key, hash_value)
            {
                place_value = grower.next_place(place_value);
            }

            place_value
        }
    }

    #[inline(always)]
    fn insert_non_zero_key(
        &mut self,
        key: &Key,
        hash_value: u64,
        inserted: &mut bool,
    ) -> *mut Entity {
        let place_value = self.find_entity(key, hash_value);
        self.insert_non_zero_key_impl(place_value, key, hash_value, inserted)
    }

    #[inline(always)]
    fn insert_non_zero_key_impl(
        &mut self,
        place_value: isize,
        key: &Key,
        hash_value: u64,
        inserted: &mut bool,
    ) -> *mut Entity {
        unsafe {
            let entity = self.entities.offset(place_value);

            if !entity.is_zero() {
                *inserted = false;
                return self.entities.offset(place_value);
            }

            self.size += 1;
            *inserted = true;
            entity.set_key_and_hash(key, hash_value);

            if std::intrinsics::unlikely(self.grower.overflow(self.size)) {
                self.resize();
                let new_place = self.find_entity(key, hash_value);
                return self.entities.offset(new_place);
            }

            self.entities.offset(place_value)
        }
    }

    #[inline(always)]
    fn insert_if_zero_key(
        &mut self,
        key: &Key,
        hash_value: u64,
        inserted: &mut bool,
    ) -> Option<*mut Entity> {
        if key.is_zero() {
            return match self.zero_entity {
                Some(zero_entity) => {
                    *inserted = false;
                    Some(zero_entity)
                }
                None => unsafe {
                    let layout = Layout::from_size_align_unchecked(
                        mem::size_of::<Entity>(),
                        mem::align_of::<Entity>(),
                    );

                    self.size += 1;
                    *inserted = true;

                    self.zero_entity_raw = Some(ALLOC.alloc_zeroed(layout));
                    self.zero_entity = Some(self.zero_entity_raw.unwrap() as *mut Entity);
                    self.zero_entity.unwrap().set_key_and_hash(key, hash_value);
                    self.zero_entity
                },
            };
        }

        Option::None
    }

    unsafe fn resize(&mut self) {
        let old_grow_size = self.grower.max_size();
        let mut new_grower = self.grower.clone();

        new_grower.increase_size();

        // Realloc memory
        if new_grower.max_size() > self.grower.max_size() {
            let old_size = (old_grow_size as usize) * std::mem::size_of::<Entity>();
            let new_size = (new_grower.max_size() as usize) * std::mem::size_of::<Entity>();
            let layout =
                Layout::from_size_align_unchecked(old_size, std::mem::align_of::<Entity>());

            self.entities_raw = self
                .allocator
                .reallocx(self.entities_raw, layout, new_size, true);

            if self.entities_raw.is_null() {
                panic!(
                    "Failed to have enough memory to realloc {new_size} bytes to resize the hashtable"
                );
            }

            self.entities = self.entities_raw as *mut Entity;
            self.grower = new_grower;
            for index in 0..old_grow_size {
                let entity_ptr = self.entities.offset(index);

                if !entity_ptr.is_zero() {
                    self.reinsert(entity_ptr, entity_ptr.get_hash());
                }
            }

            // There is also a special case:
            //      if the element was to be at the end of the old buffer,                  [        x]
            //      but is at the beginning because of the collision resolution chain,      [o       x]
            //      then after resizing, it will first be out of place again,               [        xo        ]
            //      and in order to transfer it where necessary,
            //      after transferring all the elements from the old halves you need to     [         o   x    ]
            //      process tail from the collision resolution chain immediately after it   [        o    x    ]
            for index in old_grow_size..self.grower.max_size() {
                let entity = self.entities.offset(index);

                if entity.is_zero() {
                    return;
                }

                self.reinsert(self.entities.offset(index), entity.get_hash());
            }
        }
    }

    #[inline(always)]
    unsafe fn reinsert(&self, entity: *mut Entity, hash_value: u64) {
        if entity != self.entities.offset(self.grower.place(hash_value)) {
            let place = self.find_entity(entity.get_key(), hash_value);
            let new_entity = self.entities.offset(place);

            if new_entity.is_zero() {
                entity.swap(new_entity);
            }
        }
    }
}
