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

use crate::pipelines::transforms::group_by::aggregator_state_entity::ShortFixedKeyable;
use crate::pipelines::transforms::group_by::aggregator_state_entity::ShortFixedKeysStateEntity;

type Entities<Key> = *mut ShortFixedKeysStateEntity<Key>;

pub struct ShortFixedKeysStateIterator<Key: ShortFixedKeyable> {
    index: isize,
    capacity: isize,
    entities: *mut ShortFixedKeysStateEntity<Key>,
}

impl<Key: ShortFixedKeyable> ShortFixedKeysStateIterator<Key> {
    pub fn create(entities: Entities<Key>, capacity: isize) -> Self {
        ShortFixedKeysStateIterator::<Key> {
            index: 0,
            capacity,
            entities,
        }
    }
}

impl<Key: ShortFixedKeyable> Iterator for ShortFixedKeysStateIterator<Key> {
    type Item = *mut ShortFixedKeysStateEntity<Key>;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            while self.index < self.capacity {
                let entity = self.entities.offset(self.index);
                self.index += 1;

                if (*entity).fill {
                    return Some(entity);
                }
            }

            None
        }
    }
}
