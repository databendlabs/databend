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

use std::alloc::Allocator;
use std::intrinsics::unlikely;
use std::iter::TrustedLen;
use std::mem::MaybeUninit;

use databend_common_base::mem_allocator::MmapAllocator;

use super::container::HeapContainer;
use super::table0::Entry;
use super::table0::Table0;
use super::table0::Table0Iter;
use super::table0::Table0IterMut;
use super::traits::HashtableLike;
use super::traits::Keyable;
use super::utils::ZeroEntry;
use crate::FastHash;

pub struct Hashtable<K, V, A = MmapAllocator>
where
    K: Keyable,
    A: Allocator + Clone,
{
    pub(crate) zero: ZeroEntry<K, V>,
    pub(crate) table: Table0<K, V, HeapContainer<Entry<K, V>, A>, A>,
}

unsafe impl<K: Keyable + Send, V: Send, A: Allocator + Clone + Send> Send for Hashtable<K, V, A> {}

unsafe impl<K: Keyable + Sync, V: Sync, A: Allocator + Clone + Sync> Sync for Hashtable<K, V, A> {}

impl<K, V, A> Hashtable<K, V, A>
where
    K: Keyable,
    A: Allocator + Clone + Default,
{
    pub fn new() -> Self {
        Self::new_in(Default::default())
    }
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_in(capacity, Default::default())
    }
}

impl<K, V, A> Default for Hashtable<K, V, A>
where
    K: Keyable,
    A: Allocator + Clone + Default,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, A> Hashtable<K, V, A>
where
    K: Keyable,
    A: Allocator + Clone,
{
    pub fn new_in(allocator: A) -> Self {
        Self::with_capacity_in(256, allocator)
    }
    pub fn with_capacity_in(capacity: usize, allocator: A) -> Self {
        Self {
            table: Table0::with_capacity_in(capacity, allocator),
            zero: ZeroEntry(None),
        }
    }
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.zero.is_some() as usize + self.table.len()
    }
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.zero.is_some() as usize + self.table.capacity()
    }
    #[inline(always)]
    pub fn entry(&self, key: &K) -> Option<&Entry<K, V>> {
        if unlikely(K::equals_zero(key)) {
            if let Some(entry) = self.zero.as_ref() {
                return Some(entry);
            } else {
                return None;
            }
        }
        unsafe { self.table.get(key) }
    }
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<&V> {
        unsafe { self.entry(key).map(|e| e.val.assume_init_ref()) }
    }
    #[inline(always)]
    pub fn entry_mut(&mut self, key: &K) -> Option<&mut Entry<K, V>> {
        if unlikely(K::equals_zero(key)) {
            if let Some(entry) = self.zero.as_mut() {
                return Some(entry);
            } else {
                return None;
            }
        }
        unsafe { self.table.get_mut(key) }
    }
    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        unsafe { self.entry_mut(key).map(|e| e.val.assume_init_mut()) }
    }
    #[inline(always)]
    pub fn contains(&self, key: &K) -> bool {
        self.get(key).is_some()
    }
    /// # Safety
    ///
    /// The uninitialized value of returned entry should be written immediately.
    #[inline(always)]
    pub unsafe fn insert_and_entry(
        &mut self,
        key: K,
    ) -> Result<&mut Entry<K, V>, &mut Entry<K, V>> {
        if unlikely(K::equals_zero(&key)) {
            let res = self.zero.is_some();
            if !res {
                *self.zero = Some(MaybeUninit::zeroed().assume_init());
            }
            let zero = self.zero.as_mut().unwrap();
            if res {
                return Err(zero);
            } else {
                return Ok(zero);
            }
        }
        self.table.check_grow();
        self.table.insert(key)
    }
    /// # Safety
    ///
    /// The returned uninitialized value should be written immediately.
    #[inline(always)]
    pub unsafe fn insert(&mut self, key: K) -> Result<&mut MaybeUninit<V>, &mut V> {
        match self.insert_and_entry(key) {
            Ok(e) => Ok(&mut e.val),
            Err(e) => Err(e.val.assume_init_mut()),
        }
    }
    pub fn iter(&self) -> HashtableIter<'_, K, V> {
        HashtableIter {
            inner: self.zero.iter().chain(self.table.iter()),
        }
    }
}

impl<K, A> Hashtable<K, (), A>
where
    K: Keyable,
    A: Allocator + Clone,
{
    #[inline(always)]
    pub fn set_insert(&mut self, key: K) -> Result<&mut MaybeUninit<()>, &mut ()> {
        unsafe { self.insert(key) }
    }
    #[inline(always)]
    pub fn set_merge(&mut self, other: &Self) {
        if let Some(entry) = other.zero.0.as_ref() {
            self.zero = ZeroEntry(Some(Entry {
                key: entry.key,
                val: MaybeUninit::uninit(),
                _alignment: [0; 0],
            }));
        }

        unsafe {
            self.table.set_merge(&other.table);
        }
    }
}

pub struct HashtableIter<'a, K, V> {
    pub inner: std::iter::Chain<std::option::Iter<'a, Entry<K, V>>, Table0Iter<'a, K, V>>,
}

impl<'a, K, V> Iterator for HashtableIter<'a, K, V>
where K: Keyable
{
    type Item = &'a Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

unsafe impl<'a, K, V> TrustedLen for HashtableIter<'a, K, V> where K: Keyable {}

pub struct HashtableIterMut<'a, K, V> {
    inner: std::iter::Chain<std::option::IterMut<'a, Entry<K, V>>, Table0IterMut<'a, K, V>>,
}

impl<'a, K, V> Iterator for HashtableIterMut<'a, K, V>
where K: Keyable
{
    type Item = &'a mut Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<K, V, A> HashtableLike for Hashtable<K, V, A>
where
    K: Keyable + FastHash,
    A: Allocator + Clone + 'static,
{
    type Key = K;
    type Value = V;

    type EntryRef<'a> = &'a Entry<K, V> where Self: 'a, K:'a, V: 'a;
    type EntryMutRef<'a> = &'a mut Entry<K, V> where Self: 'a, K:'a, V: 'a;

    type Iterator<'a> = HashtableIter<'a, K, V> where Self: 'a, K:'a, V: 'a;
    type IteratorMut<'a> = HashtableIterMut<'a, K, V> where Self: 'a, K:'a, V: 'a;

    fn len(&self) -> usize {
        self.len()
    }

    fn bytes_len(&self, _without_arena: bool) -> usize {
        std::mem::size_of::<Self>() + self.table.heap_bytes()
    }

    fn entry(&self, key_ref: &Self::Key) -> Option<Self::EntryRef<'_>> {
        self.entry(key_ref)
    }

    fn entry_mut(&mut self, key_ref: &Self::Key) -> Option<Self::EntryMutRef<'_>> {
        self.entry_mut(key_ref)
    }

    fn get(&self, key_ref: &Self::Key) -> Option<&Self::Value> {
        self.get(key_ref)
    }

    fn get_mut(&mut self, key_ref: &Self::Key) -> Option<&mut Self::Value> {
        self.get_mut(key_ref)
    }

    unsafe fn insert(
        &mut self,
        key: &Self::Key,
    ) -> Result<&mut MaybeUninit<Self::Value>, &mut Self::Value> {
        self.insert(*key)
    }

    #[inline(always)]
    unsafe fn insert_and_entry(
        &mut self,
        key: &Self::Key,
    ) -> Result<Self::EntryMutRef<'_>, Self::EntryMutRef<'_>> {
        if unlikely(K::equals_zero(key)) {
            let res = self.zero.is_some();
            if !res {
                *self.zero = Some(MaybeUninit::zeroed().assume_init());
            }
            let zero = self.zero.as_mut().unwrap();
            if res {
                return Err(zero);
            } else {
                return Ok(zero);
            }
        }
        self.table.check_grow();
        self.table.insert(*key)
    }

    #[inline(always)]
    unsafe fn insert_and_entry_with_hash(
        &mut self,
        key: &Self::Key,
        hash: u64,
    ) -> Result<Self::EntryMutRef<'_>, Self::EntryMutRef<'_>> {
        if unlikely(K::equals_zero(key)) {
            let res = self.zero.is_some();
            if !res {
                *self.zero = Some(MaybeUninit::zeroed().assume_init());
            }
            let zero = self.zero.as_mut().unwrap();
            if res {
                return Err(zero);
            } else {
                return Ok(zero);
            }
        }

        self.table.check_grow();

        self.table.insert_with_hash(*key, hash)
    }

    fn iter(&self) -> Self::Iterator<'_> {
        HashtableIter {
            inner: self.zero.iter().chain(self.table.iter()),
        }
    }

    fn clear(&mut self) {
        self.zero.0.take();
        self.table.clear();
    }
}
