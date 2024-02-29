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
use std::intrinsics::assume;
use std::mem::MaybeUninit;

use databend_common_base::runtime::drop_guard;

use super::container::Container;
use super::traits::EntryMutRefLike;
use super::traits::EntryRefLike;
use super::traits::Keyable;

pub struct Entry<K, V> {
    pub(crate) _alignment: [u64; 0],
    pub(crate) key: MaybeUninit<K>,
    pub(crate) val: MaybeUninit<V>,
}

impl<K: Keyable, V> Entry<K, V> {
    #[inline(always)]
    pub(crate) fn is_zero(&self) -> bool {
        K::is_zero(&self.key)
    }
    // this function can only be used in external crates
    #[inline(always)]
    pub fn key(&self) -> &K {
        unsafe { self.key.assume_init_ref() }
    }
    // this function can only be used in external crates
    /// # Safety
    ///
    /// The new key should be equals the old key.
    #[inline(always)]
    pub unsafe fn set_key(&mut self, key: K) {
        self.key.write(key);
    }
    // this function can only be used in external crates
    #[inline(always)]
    pub fn get(&self) -> &V {
        unsafe { self.val.assume_init_ref() }
    }
    // this function can only be used in external crates
    #[inline(always)]
    pub fn get_mut(&mut self) -> &mut V {
        unsafe { self.val.assume_init_mut() }
    }
    // this function can only be used in external crates
    #[inline(always)]
    pub fn write(&mut self, val: V) {
        self.val.write(val);
    }
}

pub struct Table0<K, V, C, A>
where
    K: Keyable,
    C: Container<T = Entry<K, V>, A = A>,
    A: Allocator + Clone,
{
    pub(crate) len: usize,
    #[allow(dead_code)]
    pub(crate) allocator: A,
    pub(crate) entries: C,
    pub(crate) dropped: bool,
}

impl<K, V, C, A> Table0<K, V, C, A>
where
    K: Keyable,
    C: Container<T = Entry<K, V>, A = A>,
    A: Allocator + Clone,
{
    pub fn with_capacity_in(capacity: usize, allocator: A) -> Self {
        Self {
            entries: unsafe {
                C::new_zeroed(
                    std::cmp::max(8, capacity.next_power_of_two()),
                    allocator.clone(),
                )
            },
            len: 0,
            allocator,
            dropped: false,
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub fn heap_bytes(&self) -> usize {
        self.entries.heap_bytes()
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.entries.len()
    }
    /// # Safety
    ///
    /// `key` doesn't equal to zero.
    #[inline(always)]
    pub unsafe fn get(&self, key: &K) -> Option<&Entry<K, V>> {
        self.get_with_hash(key, key.hash())
    }
    /// # Safety
    ///
    /// `key` doesn't equal to zero.
    /// Provided hash is correct.
    #[inline(always)]
    pub unsafe fn get_with_hash(&self, key: &K, hash: u64) -> Option<&Entry<K, V>> {
        assume(!K::equals_zero(key));
        let index = (hash as usize) & (self.entries.len() - 1);
        for i in (index..self.entries.len()).chain(0..index) {
            assume(i < self.entries.len());
            if self.entries[i].is_zero() {
                return None;
            }

            if self.entries[i].key.assume_init_ref() == key {
                return Some(&self.entries[i]);
            }
        }
        None
    }
    /// # Safety
    ///
    /// `key` doesn't equal to zero.
    #[inline(always)]
    pub unsafe fn get_mut(&mut self, key: &K) -> Option<&mut Entry<K, V>> {
        self.get_with_hash_mut(key, key.hash())
    }
    /// # Safety
    ///
    /// `key` doesn't equal to zero.
    /// Provided hash is correct.
    #[inline(always)]
    pub unsafe fn get_with_hash_mut(&mut self, key: &K, hash: u64) -> Option<&mut Entry<K, V>> {
        assume(!K::equals_zero(key));
        let index = (hash as usize) & (self.entries.len() - 1);
        for i in (index..self.entries.len()).chain(0..index) {
            assume(i < self.entries.len());
            if self.entries[i].is_zero() {
                return None;
            }
            if self.entries[i].key.assume_init_ref() == key {
                return Some(&mut self.entries[i]);
            }
        }
        None
    }

    pub unsafe fn get_slot_index(&self, key: &K) -> Option<usize> {
        assume(!K::equals_zero(key));

        let index = (key.hash() as usize) & (self.entries.len() - 1);
        for i in (index..self.entries.len()).chain(0..index) {
            assume(i < self.entries.len());
            if self.entries[i].is_zero() {
                return None;
            }
            if self.entries[i].key.assume_init_ref() == key {
                return Some(i);
            }
        }
        None
    }

    /// # Safety
    ///
    /// `key` doesn't equal to zero.
    /// The resulted `MaybeUninit` should be initialized immediately.
    ///
    /// # Panics
    ///
    /// Panics if the hash table overflows.
    #[inline(always)]
    pub unsafe fn insert(&mut self, key: K) -> Result<&mut Entry<K, V>, &mut Entry<K, V>> {
        self.insert_with_hash(key, key.hash())
    }
    /// # Safety
    ///
    /// `key` doesn't equal to zero.
    /// The resulted `MaybeUninit` should be initialized immediately.
    /// Provided hash is correct.
    ///
    /// # Panics
    /// The hashtable is full.
    #[inline(always)]
    pub unsafe fn insert_with_hash(
        &mut self,
        key: K,
        hash: u64,
    ) -> Result<&mut Entry<K, V>, &mut Entry<K, V>> {
        assume(!K::equals_zero(&key));
        let index = (hash as usize) & (self.entries.len() - 1);
        for i in (index..self.entries.len()).chain(0..index) {
            assume(i < self.entries.len());
            if self.entries[i].is_zero() {
                self.len += 1;
                self.entries[i].key.write(key);
                return Ok(&mut self.entries[i]);
            }
            if self.entries[i].key.assume_init_ref() == &key {
                return Err(&mut self.entries[i]);
            }
        }
        panic!("the hash table overflows")
    }
    pub fn iter(&self) -> Table0Iter<'_, K, V> {
        Table0Iter {
            slice: self.entries.as_ref(),
            i: 0,
        }
    }

    pub fn clear(&mut self) {
        unsafe {
            self.len = 0;

            if std::mem::needs_drop::<V>() {
                for entry in self.entries.as_mut() {
                    if !entry.is_zero() {
                        std::ptr::drop_in_place(entry.get_mut());
                    }
                }
            }

            self.entries = C::new_zeroed(0, self.allocator.clone());
        }
    }

    #[inline]
    pub fn check_grow(&mut self) {
        if std::intrinsics::unlikely((self.len() + 1) * 2 > self.capacity()) {
            if (self.entries.len() >> 22) == 0 {
                self.grow(2);
            } else {
                self.grow(1);
            }
        }
    }

    pub fn grow(&mut self, shift: u8) {
        let old_capacity = self.entries.len();
        let new_capacity = self.entries.len() << shift;
        unsafe {
            self.entries.grow_zeroed(new_capacity);
        }
        for i in 0..old_capacity {
            unsafe {
                assume(i < self.entries.len());
            }
            if K::is_zero(&self.entries[i].key) {
                continue;
            }
            let key = unsafe { self.entries[i].key.assume_init_ref() };
            let hash = K::hash(key);
            let index = (hash as usize) & (self.entries.len() - 1);
            for j in (index..self.entries.len()).chain(0..index) {
                unsafe {
                    assume(j < self.entries.len());
                }
                if j == i {
                    break;
                }
                if self.entries[j].is_zero() {
                    unsafe {
                        self.entries[j] = std::ptr::read(&self.entries[i]);
                        self.entries[i].key = MaybeUninit::zeroed();
                    }
                    break;
                }
            }
        }
        for i in old_capacity..new_capacity {
            unsafe {
                assume(i < self.entries.len());
            }
            if K::is_zero(&self.entries[i].key) {
                break;
            }
            let key = unsafe { self.entries[i].key.assume_init_ref() };
            let hash = K::hash(key);
            let index = (hash as usize) & (self.entries.len() - 1);
            for j in (index..self.entries.len()).chain(0..index) {
                unsafe {
                    assume(j < self.entries.len());
                }
                if j == i {
                    break;
                }
                if self.entries[j].is_zero() {
                    unsafe {
                        self.entries[j] = std::ptr::read(&self.entries[i]);
                        self.entries[i].key = MaybeUninit::zeroed();
                    }
                    break;
                }
            }
        }
    }
}

impl<K, C, A> Table0<K, (), C, A>
where
    K: Keyable,
    C: Container<T = Entry<K, ()>, A = A>,
    A: Allocator + Clone,
{
    pub unsafe fn set_merge(&mut self, other: &Self) {
        while (self.len() + other.len()) * 2 > self.capacity() {
            if (self.entries.len() >> 22) == 0 {
                self.grow(2);
            } else {
                self.grow(1);
            }
        }
        for entry in other.iter() {
            let key = entry.key.assume_init();
            let _ = self.insert(key);
        }
    }
}

impl<K, V, C, A> Drop for Table0<K, V, C, A>
where
    K: Keyable,
    C: Container<T = Entry<K, V>, A = A>,
    A: Allocator + Clone,
{
    fn drop(&mut self) {
        drop_guard(move || {
            if std::mem::needs_drop::<V>() && !self.dropped {
                unsafe {
                    for entry in self.entries.as_mut() {
                        if !entry.is_zero() {
                            std::ptr::drop_in_place(entry.get_mut());
                        }
                    }
                }
            }
        })
    }
}

pub struct Table0Iter<'a, K, V> {
    slice: &'a [Entry<K, V>],
    i: usize,
}

impl<'a, K, V> Iterator for Table0Iter<'a, K, V>
where K: Keyable
{
    type Item = &'a Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.i < self.slice.len() && self.slice[self.i].is_zero() {
            self.i += 1;
        }
        if self.i == self.slice.len() {
            None
        } else {
            let res = unsafe { &*(self.slice.as_ptr().add(self.i) as *const _) };
            self.i += 1;
            Some(res)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remain = self.slice.len() - self.i;
        (remain, Some(remain))
    }
}

pub struct Table0IterMut<'a, K, V> {
    slice: &'a mut [Entry<K, V>],
    i: usize,
}

impl<'a, K, V> Iterator for Table0IterMut<'a, K, V>
where K: Keyable
{
    type Item = &'a mut Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.i < self.slice.len() && self.slice[self.i].is_zero() {
            self.i += 1;
        }
        if self.i == self.slice.len() {
            None
        } else {
            let res = unsafe { &mut *(self.slice.as_ptr().add(self.i) as *mut _) };
            self.i += 1;
            Some(res)
        }
    }
}

impl<'a, K: Keyable, V: 'a> EntryRefLike for &'a Entry<K, V> {
    type KeyRef = &'a K;
    type ValueRef = &'a V;

    fn key(&self) -> Self::KeyRef {
        unsafe { self.key.assume_init_ref() }
    }
    fn get(&self) -> Self::ValueRef {
        (*self).get()
    }
}

impl<'a, K: Keyable, V> EntryMutRefLike for &'a mut Entry<K, V> {
    type Key = K;
    type Value = V;

    fn key(&self) -> &Self::Key {
        unsafe { self.key.assume_init_ref() }
    }
    fn get(&self) -> &Self::Value {
        unsafe { self.val.assume_init_ref() }
    }
    fn get_mut(&mut self) -> &mut Self::Value {
        unsafe { self.val.assume_init_mut() }
    }
    fn write(&mut self, value: Self::Value) {
        self.val.write(value);
    }
}
