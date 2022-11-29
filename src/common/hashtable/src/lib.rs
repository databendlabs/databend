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

#![feature(core_intrinsics)]
#![feature(allocator_api)]
#![feature(arbitrary_self_types)]
#![feature(new_uninit)]
#![feature(ptr_metadata)]
#![feature(maybe_uninit_slice)]

mod container;
mod hashtable;
mod keys_ref;
mod lookup_hashtable;
mod stack_hashtable;
mod table0;

#[allow(dead_code)]
mod table1;
mod table_empty;
mod traits;
mod twolevel_hashtable;
mod unsized_hashtable;
mod utils;

pub use table0::Entry as HashtableEntry;
pub use traits::EntryMutRefLike as HashtableEntryMutRefLike;
pub use traits::EntryRefLike as HashtableEntryRefLike;
pub use traits::FastHash;
pub use traits::HashtableLike;
pub use traits::Keyable as HashtableKeyable;
pub use traits::UnsizedKeyable as HashtableUnsizedKeyable;

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

pub type TwoLevelHashMap<Inner> = twolevel_hashtable::TwoLevelHashtable<Inner>;
pub type TwoLevelHashSet<K> = twolevel_hashtable::TwoLevelHashtable<HashSet<K>>;
pub type TwoLevelHashMapIter<Inner> = twolevel_hashtable::TwoLevelHashtableIter<Inner>;

pub type UnsizedHashMap<K, V> = unsized_hashtable::UnsizedHashtable<K, V>;
pub type UnsizedHashMapIter<'a, K, V> = unsized_hashtable::UnsizedHashtableIter<'a, K, V>;
pub type UnsizedHashMapIterMut<'a, K, V> = unsized_hashtable::UnsizedHashtableIterMut<'a, K, V>;
pub type UnsizedHashSet<K> = unsized_hashtable::UnsizedHashtable<K, ()>;
pub type UnsizedHashSetIter<'a, K> = unsized_hashtable::UnsizedHashtableIter<'a, K, ()>;
pub type UnsizedHashSetIterMut<'a, K> = unsized_hashtable::UnsizedHashtableIterMut<'a, K, ()>;
pub type UnsizedHashtableEntryRef<'a, K, V> = unsized_hashtable::UnsizedHashtableEntryRef<'a, K, V>;
pub type UnsizedHashtableEntryMutRef<'a, K, V> =
    unsized_hashtable::UnsizedHashtableEntryMutRef<'a, K, V>;

pub type LookupHashMap<K, const CAPACITY: usize, V> = LookupHashtable<K, CAPACITY, V>;
pub type LookupHashMapIter<'a, K, const CAPACITY: usize, V> = LookupTableIter<'a, CAPACITY, K, V>;
pub type LookupHashMapIterMut<'a, K, const CAPACITY: usize, V> =
    LookupTableIterMut<'a, CAPACITY, K, V>;

pub use keys_ref::KeysRef;
