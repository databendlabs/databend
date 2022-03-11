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

use std::marker::PhantomData;

use crate::HashTableEntity;

pub struct HashTableIter<Key, Entity: HashTableEntity<Key>> {
    idx: isize,
    capacity: isize,
    entities: *mut Entity,
    zero_entity: Option<*mut Entity>,

    phantom: PhantomData<Key>,
}

impl<Key, Entity: HashTableEntity<Key>> HashTableIter<Key, Entity> {
    pub fn create(
        capacity: isize,
        entities: *mut Entity,
        zero_entity: Option<*mut Entity>,
    ) -> Self {
        Self {
            idx: -2,
            capacity,
            entities,
            zero_entity,
            phantom: PhantomData::default(),
        }
    }
}

impl<Key, Entity: HashTableEntity<Key>> Iterator for HashTableIter<Key, Entity> {
    type Item = *mut Entity;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            if self.idx == -2 {
                self.idx = -1;
                if self.zero_entity.is_some() {
                    return self.zero_entity;
                }
            }

            self.idx += 1;
            while self.idx < self.capacity && self.entities.offset(self.idx).is_zero() {
                self.idx += 1;
            }

            match self.idx == self.capacity {
                true => None,
                false => Some(self.entities.offset(self.idx)),
            }
        }
    }
}
