// Copyright 2022 Datafuse Labs.
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

use crate::common::HashTable;
use crate::common::HashTableEntity;
use crate::common::HashTableGrower;
use crate::common::HashTableIteratorKind;
use crate::common::HashTableKeyable;

static BITS_FOR_BUCKET: u8 = 8;
static NUM_BUCKETS: usize = 1 << BITS_FOR_BUCKET;
static MAX_BUCKECT: usize = NUM_BUCKETS - 1;

pub enum HashTableKind<
    Key: HashTableKeyable,
    Entity: HashTableEntity<Key>,
    SingleLevelGrower: HashTableGrower,
    TwoLevelGrower: HashTableGrower,
> {
    HashTable(HashTable<Key, Entity, SingleLevelGrower>),
    TwoLevelHashTable(TwoLevelHashTable<Key, Entity, TwoLevelGrower>),
}

impl<
        Key: HashTableKeyable,
        Entity: HashTableEntity<Key>,
        SingleLevelGrower: HashTableGrower,
        TwoLevelGrower: HashTableGrower,
    > HashTableKind<Key, Entity, SingleLevelGrower, TwoLevelGrower>
{
    pub fn create_hash_table() -> Self {
        Self::HashTable(HashTable::create())
    }

    pub fn create_two_level_hash_table() -> Self {
        Self::TwoLevelHashTable(TwoLevelHashTable::create())
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        match self {
            HashTableKind::HashTable(data) => data.len(),
            HashTableKind::TwoLevelHashTable(data) => data.len(),
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn iter(&self) -> HashTableIteratorKind<Key, Entity> {
        match self {
            HashTableKind::HashTable(data) => data.iter(),
            HashTableKind::TwoLevelHashTable(data) => data.iter(),
        }
    }

    #[inline(always)]
    pub fn insert_key(&mut self, key: &Key, inserted: &mut bool) -> *mut Entity {
        match self {
            HashTableKind::HashTable(data) => data.insert_key(key, inserted),
            HashTableKind::TwoLevelHashTable(data) => data.insert_key(key, inserted),
        }
    }

    #[inline(always)]
    pub fn insert_hash_key(&mut self, key: &Key, hash: u64, inserted: &mut bool) -> *mut Entity {
        match self {
            HashTableKind::HashTable(data) => data.insert_hash_key(key, hash, inserted),
            HashTableKind::TwoLevelHashTable(data) => data.insert_hash_key(key, hash, inserted),
        }
    }

    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn convert_to_two_level(&mut self) {
        let mut two_level_hash_table = Self::create_two_level_hash_table();

        if !self.is_empty() {
            let mut inserted = true;
            for old_entity in self.iter() {
                let new_entity =
                    two_level_hash_table.insert_key(old_entity.get_key(), &mut inserted);
                if inserted {
                    new_entity.swap(old_entity);
                }
            }
        }
        std::mem::swap(self, &mut two_level_hash_table);
    }
}

pub struct TwoLevelHashTable<
    Key: HashTableKeyable,
    Entity: HashTableEntity<Key>,
    Grower: HashTableGrower,
> {
    hash_tables: Vec<HashTable<Key, Entity, Grower>>,
}

impl<Key: HashTableKeyable, Entity: HashTableEntity<Key>, Grower: HashTableGrower>
    TwoLevelHashTable<Key, Entity, Grower>
{
    pub fn create() -> TwoLevelHashTable<Key, Entity, Grower> {
        let mut hash_tables: Vec<HashTable<Key, Entity, Grower>> = Vec::with_capacity(NUM_BUCKETS);

        for _ in 0..NUM_BUCKETS {
            hash_tables.push(HashTable::<Key, Entity, Grower>::create());
        }

        TwoLevelHashTable { hash_tables }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.hash_tables
            .iter()
            .map(|hash_table| hash_table.len())
            .sum()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn iter(&self) -> HashTableIteratorKind<Key, Entity> {
        let mut iters = Vec::with_capacity(NUM_BUCKETS);
        for i in 0..NUM_BUCKETS {
            iters.push(self.hash_tables[i].iter())
        }
        HashTableIteratorKind::<Key, Entity>::create_two_level_hash_table_iter(iters)
    }

    #[inline(always)]
    pub fn insert_key(&mut self, key: &Key, inserted: &mut bool) -> *mut Entity {
        let hash = key.fast_hash();
        let bucket = self.get_bucket_from_hash(&hash);
        self.hash_tables[bucket].insert_hash_key(key, hash, inserted)
    }

    #[inline(always)]
    pub fn insert_hash_key(&mut self, key: &Key, hash: u64, inserted: &mut bool) -> *mut Entity {
        let bucket = self.get_bucket_from_hash(&hash);
        self.hash_tables[bucket].insert_hash_key(key, hash, inserted)
    }

    #[inline(always)]
    pub fn find_key(&self, key: &Key) -> Option<*mut Entity> {
        let hash = key.fast_hash();
        let bucket = self.get_bucket_from_hash(&hash);
        self.hash_tables[bucket].find_key(key)
    }

    #[inline(always)]
    fn get_bucket_from_hash(&self, hash_value: &u64) -> usize {
        ((hash_value >> (64 - BITS_FOR_BUCKET)) & (MAX_BUCKECT as u64)) as usize
    }
}
