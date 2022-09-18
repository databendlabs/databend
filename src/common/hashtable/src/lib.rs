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

mod hashtable;
mod stack_hashtable;
mod traits;
mod twolevel_hashtable;
mod unsized_hashtable;

mod allocator;
mod container;
mod table0;
mod table1;
mod utils;

pub use table0::Entry as HashtableEntry;
pub use traits::FastHash;
pub use traits::Keyable as HashtableKeyable;
pub use traits::UnsizedKeyable as HashtableUnsizedKeyable;

pub type Hashed<K> = utils::Hashed<K>;

pub type HashMap<K, V> = hashtable::Hashtable<K, V>;
pub type HashMapIter<'a, K, V> = hashtable::HashtableIter<'a, K, V>;
pub type HashMapIterMut<'a, K, V> = hashtable::HashtableIter<'a, K, V>;
pub type HashMapIterPtr<K, V> = hashtable::HashtableIterPtr<K, V>;
pub type HashMapIterPtrMut<K, V> = hashtable::HashtableIterMutPtr<K, V>;
pub type HashSet<K> = hashtable::Hashtable<K, ()>;
pub type HashSetIter<'a, K> = hashtable::HashtableIter<'a, K, ()>;
pub type HashSetIterMut<'a, K> = hashtable::HashtableIter<'a, K, ()>;
pub type HashSetIterPtr<K> = hashtable::HashtableIterPtr<K, ()>;
pub type HashSetIterPtrMut<K> = hashtable::HashtableIterMutPtr<K, ()>;

pub type StackHashMap<K, V, const N: usize = 16> = stack_hashtable::StackHashtable<K, V, N>;
pub type StackHashMapIter<'a, K, V> = stack_hashtable::StackHashtableIter<'a, K, V>;
pub type StackHashMapIterMut<'a, K, V> = stack_hashtable::StackHashtableIter<'a, K, V>;
pub type StackHashMapIterPtr<K, V> = stack_hashtable::StackHashtableIterPtr<K, V>;
pub type StackHashMapIterMutPtr<K, V> = stack_hashtable::StackHashtableIterMutPtr<K, V>;
pub type StackHashSet<K, const N: usize = 16> = stack_hashtable::StackHashtable<K, (), N>;
pub type StackHashSetIter<'a, K> = stack_hashtable::StackHashtableIter<'a, K, ()>;
pub type StackHashSetIterMut<'a, K> = stack_hashtable::StackHashtableIter<'a, K, ()>;
pub type StackHashSetIterPtr<K> = stack_hashtable::StackHashtableIterPtr<K, ()>;
pub type StackHashSetIterMutPtr<K> = stack_hashtable::StackHashtableIterMutPtr<K, ()>;

pub type TwolevelHashMap<K, V> = twolevel_hashtable::TwolevelHashtable<K, V>;
pub type TwolevelHashMapIter<'a, K, V> = twolevel_hashtable::TwolevelHashtableIter<'a, K, V>;
pub type TwolevelHashMapIterMut<'a, K, V> = twolevel_hashtable::TwolevelHashtableIterMut<'a, K, V>;
pub type TwolevelHashMapIterPtr<K, V> = twolevel_hashtable::TwolevelHashtableIterPtr<K, V>;
pub type TwolevelHashMapIterMutPtr<K, V> = twolevel_hashtable::TwolevelHashtableIterMutPtr<K, V>;
pub type TwolevelHashSet<K> = twolevel_hashtable::TwolevelHashtable<K, ()>;
pub type TwolevelHashSetIter<'a, K> = twolevel_hashtable::TwolevelHashtableIter<'a, K, ()>;
pub type TwolevelHashSetIterMut<'a, K> = twolevel_hashtable::TwolevelHashtableIterMut<'a, K, ()>;
pub type TwolevelHashSetIterPtr<K> = twolevel_hashtable::TwolevelHashtableIterPtr<K, ()>;
pub type TwolevelHashSetIterMutPtr<K> = twolevel_hashtable::TwolevelHashtableIterMutPtr<K, ()>;

pub type HashMapKind<K, V> = twolevel_hashtable::HashtableKind<K, V>;
pub type HashMapKindIter<'a, K, V> = twolevel_hashtable::HashtableKindIter<'a, K, V>;
pub type HashMapKindIterMut<'a, K, V> = twolevel_hashtable::HashtableKindIterMut<'a, K, V>;
pub type HashMapKindIterPtr<K, V> = twolevel_hashtable::HashtableKindIterPtr<K, V>;
pub type HashMapKindIterMutPtr<K, V> = twolevel_hashtable::HashtableKindIterMutPtr<K, V>;
pub type HashSetKind<K> = twolevel_hashtable::HashtableKind<K, ()>;
pub type HashSetKindIter<'a, K> = twolevel_hashtable::HashtableKindIter<'a, K, ()>;
pub type HashSetKindIterMut<'a, K> = twolevel_hashtable::HashtableKindIterMut<'a, K, ()>;
pub type HashSetKindIterPtr<K> = twolevel_hashtable::HashtableKindIterPtr<K, ()>;
pub type HashSetKindIterMutPtr<K> = twolevel_hashtable::HashtableKindIterMutPtr<K, ()>;

pub type UnsizedHashMap<K, V> = unsized_hashtable::UnsizedHashtable<K, V>;
pub type UnsizedHashSet<K> = unsized_hashtable::UnsizedHashtable<K, ()>;
