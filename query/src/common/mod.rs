// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod hash_table_grower_test;

mod hash_table;
mod hash_table_iter;
mod hash_table_entity;
mod hash_table_grower;
mod hash_table_hasher;

pub use hash_table::HashTable;
pub use hash_table_hasher::KeyHasher;
pub use hash_table_hasher::DefaultHasher;
pub use hash_table_entity::HashTableEntity;
pub use hash_table_entity::DefaultHashTableEntity;

pub type HashMap<Key, Value> = HashTable<Key, DefaultHashTableEntity<Key, Value>, DefaultHasher<Key>>;

