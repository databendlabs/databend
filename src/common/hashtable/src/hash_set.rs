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

use common_base::mem_allocator::Allocator as AllocatorTrait;

use crate::HashSet;
use crate::HashTableEntity;
use crate::HashTableGrower;
use crate::HashTableKeyable;

impl<Key: HashTableKeyable, Grower: HashTableGrower, Allocator: AllocatorTrait + Default>
    HashSet<Key, Grower, Allocator>
{
    pub fn merge(&mut self, other: &Self) {
        let mut inserted = false;
        for value in other.iter() {
            self.insert_key(value.get_key(), &mut inserted);
        }
    }
}
