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

#![allow(internal_features)]
#![allow(clippy::ptr_arg)]
#![feature(core_intrinsics)]
#![feature(allocator_api)]
#![feature(arbitrary_self_types)]
#![feature(ptr_metadata)]
#![feature(trusted_len)]

extern crate core;

mod container;

mod hashtable;
mod keys_ref;
mod lookup_hashtable;
mod partitioned_hashtable;
mod row_ptr;
mod short_string_hashtable;
mod stack_hashtable;
mod string_hashtable;
mod table0;
#[allow(dead_code)]
mod table1;
mod table_empty;
pub mod traits;
mod utils;

pub use table0::Entry as HashtableEntry;
pub use traits::BloomHash;
pub use traits::EntryMutRefLike as HashtableEntryMutRefLike;
pub use traits::EntryRefLike as HashtableEntryRefLike;
pub use traits::FastHash;
pub use traits::HashtableLike;
pub use traits::Keyable as HashtableKeyable;
pub use traits::UnsizedKeyable as HashtableUnsizedKeyable;
pub use traits::hash_join_fast_string_hash;

use crate::lookup_hashtable::LookupHashtable;
use crate::lookup_hashtable::LookupTableIter;
use crate::lookup_hashtable::LookupTableIterMut;

pub type Hashed<K> = utils::Hashed<K>;

pub type HashMap<K, V> = hashtable::Hashtable<K, V>;
pub type HashMapIter<'a, K, V> = hashtable::HashtableIter<'a, K, V>;
pub type HashMapIterMut<'a, K, V> = hashtable::HashtableIter<'a, K, V>;
pub type HashSet<K> = hashtable::Hashtable<K, ()>;
pub type HashSetIter<'a, K> = hashtable::HashtableIter<'a, K, ()>;
pub type HashSetIterMut<'a, K> = hashtable::HashtableIter<'a, K, ()>;

pub type StackHashMap<K, V, const N: usize = 16> = stack_hashtable::StackHashtable<K, V, N>;
pub type StackHashMapIter<'a, K, V> = stack_hashtable::StackHashtableIter<'a, K, V>;
pub type StackHashMapIterMut<'a, K, V> = stack_hashtable::StackHashtableIter<'a, K, V>;
pub type StackHashSet<K, const N: usize = 16> = stack_hashtable::StackHashtable<K, (), N>;
pub type StackHashSetIter<'a, K> = stack_hashtable::StackHashtableIter<'a, K, ()>;
pub type StackHashSetIterMut<'a, K> = stack_hashtable::StackHashtableIter<'a, K, ()>;

pub type PartitionedHashMap<Inner, const BUCKETS_LG2: u32, const HIGH_BIT: bool = true> =
    partitioned_hashtable::PartitionedHashtable<Inner, BUCKETS_LG2, HIGH_BIT>;
pub type PartitionedHashSet<K, const BUCKETS_LG2: u32, const HIGH_BIT: bool = true> =
    partitioned_hashtable::PartitionedHashtable<HashSet<K>, BUCKETS_LG2, HIGH_BIT>;

pub type PartitionedHashMapIter<Inner> = partitioned_hashtable::PartitionedHashtableIter<Inner>;

pub type ShortStringHashMap<K, V> = short_string_hashtable::ShortStringHashtable<K, V>;
pub type ShortStringHashMapIter<'a, K, V> =
    short_string_hashtable::ShortStringHashtableIter<'a, K, V>;
pub type ShortStringHashMapIterMut<'a, K, V> =
    short_string_hashtable::ShortStringHashtableIterMut<'a, K, V>;
pub type ShortStringHashSet<K> = short_string_hashtable::ShortStringHashtable<K, ()>;
pub type ShortStringHashtableEntryRef<'a, K, V> =
    short_string_hashtable::ShortStringHashtableEntryRef<'a, K, V>;
pub type ShortStringHashtableEntryMutRef<'a, K, V> =
    short_string_hashtable::ShortStringHashtableEntryMutRef<'a, K, V>;

pub type StringHashMap<K, V> = string_hashtable::StringHashtable<K, V>;
pub type StringHashMapIter<'a, K, V> = string_hashtable::StringHashtableIter<'a, K, V>;
pub type StringHashMapIterMut<'a, K, V> = string_hashtable::StringHashtableIterMut<'a, K, V>;
pub type StringHashSet<K> = string_hashtable::StringHashtable<K, ()>;
pub type StringHashtableEntryRef<'a, K, V> = string_hashtable::StringHashtableEntryRef<'a, K, V>;
pub type StringHashtableEntryMutRef<'a, K, V> =
    string_hashtable::StringHashtableEntryMutRef<'a, K, V>;

pub type LookupHashMap<K, const CAPACITY: usize, V> = LookupHashtable<K, CAPACITY, V>;
pub type LookupHashMapIter<'a, K, const CAPACITY: usize, V> = LookupTableIter<'a, CAPACITY, K, V>;
pub type LookupHashMapIterMut<'a, K, const CAPACITY: usize, V> =
    LookupTableIterMut<'a, CAPACITY, K, V>;

pub use keys_ref::KeysRef;
pub use partitioned_hashtable::hash2bucket;
pub use row_ptr::RowPtr;
pub use utils::Interval;
pub use utils::MergeIntoBlockInfoIndex;
pub use utils::fast_memcmp;
