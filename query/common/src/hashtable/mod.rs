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

pub use hash_table::HashTable;
pub use hash_table_entity::HashTableEntity;
pub use hash_table_entity::KeyValueEntity;
pub use hash_table_grower::Grower;
pub use hash_table_iter::HashTableIter;
pub use hash_table_key::HashTableKeyable;

mod hash_table;
#[allow(clippy::missing_safety_doc, clippy::not_unsafe_ptr_arg_deref)]
mod hash_table_entity;
mod hash_table_grower;
mod hash_table_iter;
mod hash_table_key;

pub type HashMap<Key, Value> = HashTable<Key, KeyValueEntity<Key, Value>>;
pub type HashMapIterator<Key, Value> = HashTableIter<Key, KeyValueEntity<Key, Value>>;
