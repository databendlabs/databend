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

use crate::pipelines::processors::transforms::group_by::aggregator_state_entity::ShortFixedKeyable;
use crate::pipelines::processors::transforms::group_by::aggregator_state_entity::ShortFixedKeysStateEntity;

pub struct ShortFixedKeysStateIterator<'a, Key: ShortFixedKeyable> {
    index: usize,
    entities: &'a [ShortFixedKeysStateEntity<Key>],
}

impl<'a, Key: ShortFixedKeyable> ShortFixedKeysStateIterator<'a, Key> {
    pub fn create(entities: &'a [ShortFixedKeysStateEntity<Key>]) -> Self {
        Self { index: 0, entities }
    }
}

impl<'a, Key: ShortFixedKeyable> Iterator for ShortFixedKeysStateIterator<'a, Key> {
    type Item = &'a ShortFixedKeysStateEntity<Key>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < self.entities.len() {
            let entity = &self.entities[self.index];
            self.index += 1;

            if entity.fill {
                return Some(entity);
            }
        }

        None
    }
}
