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

use std::alloc::Allocator;
use std::intrinsics::unlikely;
use std::mem::MaybeUninit;

use super::container::StackContainer;
use super::table0::Entry;
use super::table0::Table0;
use super::table0::Table0Iter;
use super::table0::Table0IterMut;
use super::table0::Table0IterMutPtr;
use super::table0::Table0IterPtr;
use super::traits::Keyable;
use super::utils::ZeroEntry;

pub struct StackHashtable<K, V, const N: usize = 16, A = super::allocator::Default>
where
    K: Keyable,
    A: Allocator + Clone,
{
    zero: ZeroEntry<K, V>,
    table: Table0<K, V, StackContainer<Entry<K, V>, N, A>, A>,
}

unsafe impl<K: Keyable + Send, V: Send, const N: usize, A: Allocator + Clone + Send> Send
    for StackHashtable<K, V, N, A>
{
}

unsafe impl<K: Keyable + Sync, V: Sync, const N: usize, A: Allocator + Clone + Sync> Sync
    for StackHashtable<K, V, N, A>
{
}

impl<K, V, A, const N: usize> StackHashtable<K, V, N, A>
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

impl<K, V, A, const N: usize> StackHashtable<K, V, N, A>
where
    K: Keyable,
    A: Allocator + Clone,
{
    pub fn new_in(allocator: A) -> Self {
        Self::with_capacity_in(N, allocator)
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
        if unlikely((self.table.len() + 1) * 2 > self.table.capacity()) {
            if (self.table.entries.len() >> 22) == 0 {
                self.table.grow(2);
            } else {
                self.table.grow(1);
            }
        }
        self.table.insert(key)
    }
    #[inline(always)]
    pub unsafe fn insert(&mut self, key: K) -> Result<&mut MaybeUninit<V>, &mut V> {
        match self.insert_and_entry(key) {
            Ok(e) => Ok(&mut e.val),
            Err(e) => Err(e.val.assume_init_mut()),
        }
    }
    pub fn iter(&self) -> StackHashtableIter<'_, K, V> {
        StackHashtableIter {
            inner: self.zero.iter().chain(self.table.iter()),
        }
    }
    pub fn iter_mut(&mut self) -> StackHashtableIterMut<'_, K, V> {
        StackHashtableIterMut {
            inner: self.zero.iter_mut().chain(self.table.iter_mut()),
        }
    }
    pub fn iter_ptr(&self) -> StackHashtableIterPtr<K, V> {
        StackHashtableIterPtr {
            inner: self
                .zero
                .as_ref()
                .map(|x| x as *const _)
                .into_iter()
                .chain(self.table.iter_ptr()),
        }
    }
    pub fn iter_mut_ptr(&self) -> StackHashtableIterMutPtr<K, V> {
        StackHashtableIterMutPtr {
            inner: self
                .zero
                .as_ref()
                .map(|x| x as *const _ as *mut _)
                .into_iter()
                .chain(self.table.iter_mut_ptr()),
        }
    }
}

impl<K, A, const N: usize> StackHashtable<K, (), N, A>
where
    K: Keyable,
    A: Allocator + Clone,
{
    #[inline(always)]
    pub fn set_insert(&mut self, key: K) -> Result<&mut MaybeUninit<()>, &mut ()> {
        unsafe { self.insert(key) }
    }
    #[inline(always)]
    pub fn set_merge(&mut self, mut other: Self) {
        if let Some(entry) = other.zero.take() {
            self.zero = ZeroEntry(Some(entry));
        }
        while (self.table.len() + other.table.len()) * 2 > self.table.capacity() {
            if (self.table.entries.len() >> 22) == 0 {
                self.table.grow(2);
            } else {
                self.table.grow(1);
            }
        }
        unsafe {
            self.table.merge(other.table);
        }
    }
}

pub struct StackHashtableIter<'a, K, V> {
    inner: std::iter::Chain<std::option::Iter<'a, Entry<K, V>>, Table0Iter<'a, K, V>>,
}

impl<'a, K, V> Iterator for StackHashtableIter<'a, K, V>
where K: Keyable
{
    type Item = &'a Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub struct StackHashtableIterMut<'a, K, V> {
    inner: std::iter::Chain<std::option::IterMut<'a, Entry<K, V>>, Table0IterMut<'a, K, V>>,
}

impl<'a, K, V> Iterator for StackHashtableIterMut<'a, K, V>
where K: Keyable
{
    type Item = &'a mut Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub struct StackHashtableIterPtr<K, V> {
    inner: std::iter::Chain<std::option::IntoIter<*const Entry<K, V>>, Table0IterPtr<K, V>>,
}

impl<K, V> Iterator for StackHashtableIterPtr<K, V>
where K: Keyable
{
    type Item = *const Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub struct StackHashtableIterMutPtr<K, V> {
    inner: std::iter::Chain<std::option::IntoIter<*mut Entry<K, V>>, Table0IterMutPtr<K, V>>,
}

impl<K, V> Iterator for StackHashtableIterMutPtr<K, V>
where K: Keyable
{
    type Item = *mut Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
