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
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::num::NonZeroU64;
use std::ptr::NonNull;

use bumpalo::Bump;

use super::container::HeapContainer;
use super::table0::Entry;
use super::table0::Table0;
use super::table1::Table1;
use super::traits::FastHash;
use super::traits::Keyable;
use super::traits::UnsizedKeyable;
use super::utils::read_le;
use crate::table0::Table0Iter;
use crate::table0::Table0IterMut;
use crate::table0::Table0IterMutPtr;
use crate::table0::Table0IterPtr;
use crate::table1::Table1Iter;
use crate::table1::Table1IterMut;
use crate::table1::Table1IterMutPtr;
use crate::table1::Table1IterPtr;

pub struct UnsizedHashtable<K, V, A = super::allocator::Default>
where
    K: UnsizedKeyable + ?Sized,
    A: Allocator + Clone,
{
    pub(crate) arena: Bump,
    pub(crate) table0: Table1<V, A>,
    pub(crate) table1: Table0<InlineKey<0>, V, HeapContainer<Entry<InlineKey<0>, V>, A>, A>,
    pub(crate) table2: Table0<InlineKey<1>, V, HeapContainer<Entry<InlineKey<1>, V>, A>, A>,
    pub(crate) table3: Table0<InlineKey<2>, V, HeapContainer<Entry<InlineKey<2>, V>, A>, A>,
    pub(crate) table4: Table0<FallbackKey, V, HeapContainer<Entry<FallbackKey, V>, A>, A>,
    pub(crate) _phantom: PhantomData<K>,
}

unsafe impl<K: UnsizedKeyable + ?Sized + Send, V: Send, A: Allocator + Clone + Send> Send
    for UnsizedHashtable<K, V, A>
{
}

unsafe impl<K: UnsizedKeyable + ?Sized + Sync, V: Sync, A: Allocator + Clone + Sync> Sync
    for UnsizedHashtable<K, V, A>
{
}

impl<K, V, A> UnsizedHashtable<K, V, A>
where
    K: UnsizedKeyable + ?Sized,
    A: Allocator + Clone + Default,
{
    pub fn new() -> Self {
        Self::new_in(Default::default())
    }
}

impl<K, V, A> UnsizedHashtable<K, V, A>
where
    K: UnsizedKeyable + ?Sized,
    A: Allocator + Clone + Default,
{
    /// The bump for strings doesn't allocate memory by `A`.
    pub fn new_in(allocator: A) -> Self {
        Self {
            arena: Bump::new(),
            table0: Table1::new_in(allocator.clone()),
            table1: Table0::with_capacity_in(128, allocator.clone()),
            table2: Table0::with_capacity_in(128, allocator.clone()),
            table3: Table0::with_capacity_in(128, allocator.clone()),
            table4: Table0::with_capacity_in(128, allocator),
            _phantom: PhantomData,
        }
    }
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.table0.len()
            + self.table1.len()
            + self.table2.len()
            + self.table3.len()
            + self.table4.len()
    }
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.table0.capacity()
            + self.table1.capacity()
            + self.table2.capacity()
            + self.table3.capacity()
            + self.table4.capacity()
    }
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<&V> {
        let key = key.as_bytes();
        match key.len() {
            _ if key.last().copied() == Some(0) => unsafe {
                self.table4
                    .get(&FallbackKey::new(key))
                    .map(|e| e.val.assume_init_ref())
            },
            0 => unsafe { self.table0.get([0, 0]).map(|e| e.val.assume_init_ref()) },
            1 => unsafe {
                self.table0
                    .get([key[0], 0])
                    .map(|e| e.val.assume_init_ref())
            },
            2 => unsafe {
                self.table0
                    .get([key[0], key[1]])
                    .map(|e| e.val.assume_init_ref())
            },
            3..=8 => unsafe {
                let mut t = [0u64; 1];
                t[0] = read_le(key.as_ptr(), key.len());
                let t = std::mem::transmute::<_, InlineKey<0>>(t);
                self.table1.get(&t).map(|e| e.val.assume_init_ref())
            },
            9..=16 => unsafe {
                let mut t = [0u64; 2];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = read_le(key.as_ptr().offset(8), key.len() - 8);
                let t = std::mem::transmute::<_, InlineKey<1>>(t);
                self.table2.get(&t).map(|e| e.val.assume_init_ref())
            },
            17..=24 => unsafe {
                let mut t = [0u64; 3];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = (key.as_ptr() as *const u64).offset(1).read_unaligned();
                t[2] = read_le(key.as_ptr().offset(16), key.len() - 16);
                let t = std::mem::transmute::<_, InlineKey<2>>(t);
                self.table3.get(&t).map(|e| e.val.assume_init_ref())
            },
            _ => unsafe {
                self.table4
                    .get(&FallbackKey::new(key))
                    .map(|e| e.val.assume_init_ref())
            },
        }
    }
    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let key = key.as_bytes();
        match key.len() {
            _ if key.last().copied() == Some(0) => unsafe {
                self.table4
                    .get_mut(&FallbackKey::new(key))
                    .map(|e| e.val.assume_init_mut())
            },
            0 => unsafe { self.table0.get_mut([0, 0]).map(|e| e.val.assume_init_mut()) },
            1 => unsafe {
                self.table0
                    .get_mut([key[0], 0])
                    .map(|e| e.val.assume_init_mut())
            },
            2 => unsafe {
                self.table0
                    .get_mut([key[0], key[1]])
                    .map(|e| e.val.assume_init_mut())
            },
            3..=8 => unsafe {
                let mut t = [0u64; 1];
                t[0] = read_le(key.as_ptr(), key.len());
                let t = std::mem::transmute::<_, InlineKey<0>>(t);
                self.table1.get_mut(&t).map(|e| e.val.assume_init_mut())
            },
            9..=16 => unsafe {
                let mut t = [0u64; 2];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = read_le(key.as_ptr().offset(8), key.len() - 8);
                let t = std::mem::transmute::<_, InlineKey<1>>(t);
                self.table2.get_mut(&t).map(|e| e.val.assume_init_mut())
            },
            17..=24 => unsafe {
                let mut t = [0u64; 3];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = (key.as_ptr() as *const u64).offset(1).read_unaligned();
                t[2] = read_le(key.as_ptr().offset(16), key.len() - 16);
                let t = std::mem::transmute::<_, InlineKey<2>>(t);
                self.table3.get_mut(&t).map(|e| e.val.assume_init_mut())
            },
            _ => unsafe {
                self.table4
                    .get_mut(&FallbackKey::new(key))
                    .map(|e| e.val.assume_init_mut())
            },
        }
    }
    #[inline(always)]
    pub unsafe fn insert_and_entry(
        &mut self,
        key: &K,
    ) -> Result<*mut UnsizedHashtableFakeEntry<K, V>, *mut UnsizedHashtableFakeEntry<K, V>> {
        let key = key.as_bytes();
        match key.len() {
            _ if key.last().copied() == Some(0) => {
                if unlikely((self.table4.len() + 1) * 2 > self.table4.capacity()) {
                    if (self.table4.entries.len() >> 22) == 0 {
                        self.table4.grow(2);
                    } else {
                        self.table4.grow(1);
                    }
                }
                let s = self.arena.alloc_slice_copy(key);
                translate::<0b000, _, K, V>(self.table4.insert(FallbackKey::new(s)))
            }
            0 => translate::<0b100, _, K, V>(self.table0.insert([0, 0])),
            1 => translate::<0b100, _, K, V>(self.table0.insert([key[0], 0])),
            2 => translate::<0b100, _, K, V>(self.table0.insert([key[0], key[1]])),
            3..=8 => {
                if unlikely((self.table1.len() + 1) * 2 > self.table1.capacity()) {
                    if (self.table1.entries.len() >> 22) == 0 {
                        self.table1.grow(2);
                    } else {
                        self.table1.grow(1);
                    }
                }
                let mut t = [0u64; 1];
                t[0] = read_le(key.as_ptr(), key.len());
                let t = std::mem::transmute::<_, InlineKey<0>>(t);
                translate::<0b101, _, K, V>(self.table1.insert(t))
            }
            9..=16 => {
                if unlikely((self.table2.len() + 1) * 2 > self.table2.capacity()) {
                    if (self.table2.entries.len() >> 22) == 0 {
                        self.table2.grow(2);
                    } else {
                        self.table2.grow(1);
                    }
                }
                let mut t = [0u64; 2];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = read_le(key.as_ptr().offset(8), key.len() - 8);
                let t = std::mem::transmute::<_, InlineKey<1>>(t);
                translate::<0b110, _, K, V>(self.table2.insert(t))
            }
            17..=24 => {
                if unlikely((self.table3.len() + 1) * 2 > self.table3.capacity()) {
                    if (self.table3.entries.len() >> 22) == 0 {
                        self.table3.grow(2);
                    } else {
                        self.table3.grow(1);
                    }
                }
                let mut t = [0u64; 3];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = (key.as_ptr() as *const u64).offset(1).read_unaligned();
                t[2] = read_le(key.as_ptr().offset(16), key.len() - 16);
                let t = std::mem::transmute::<_, InlineKey<2>>(t);
                translate::<0b111, _, K, V>(self.table3.insert(t))
            }
            _ => {
                if unlikely((self.table4.len() + 1) * 2 > self.table4.capacity()) {
                    if (self.table4.entries.len() >> 22) == 0 {
                        self.table4.grow(2);
                    } else {
                        self.table4.grow(1);
                    }
                }
                let s = self.arena.alloc_slice_copy(key);
                translate::<0b000, _, K, V>(self.table4.insert(FallbackKey::new(s)))
            }
        }
    }
    #[inline(always)]
    pub unsafe fn insert_and_entry_borrowing(
        &mut self,
        key: *const K,
    ) -> Result<*mut UnsizedHashtableFakeEntry<K, V>, *mut UnsizedHashtableFakeEntry<K, V>> {
        let key = (*key).as_bytes();
        match key.len() {
            _ if key.last().copied() == Some(0) => {
                if unlikely((self.table4.len() + 1) * 2 > self.table4.capacity()) {
                    if (self.table4.entries.len() >> 22) == 0 {
                        self.table4.grow(2);
                    } else {
                        self.table4.grow(1);
                    }
                }
                translate::<0b000, _, K, V>(self.table4.insert(FallbackKey::new(key)))
            }
            0 => translate::<0b100, _, K, V>(self.table0.insert([0, 0])),
            1 => translate::<0b100, _, K, V>(self.table0.insert([key[0], 0])),
            2 => translate::<0b100, _, K, V>(self.table0.insert([key[0], key[1]])),
            3..=8 => {
                if unlikely((self.table1.len() + 1) * 2 > self.table1.capacity()) {
                    if (self.table1.entries.len() >> 22) == 0 {
                        self.table1.grow(2);
                    } else {
                        self.table1.grow(1);
                    }
                }
                let mut t = [0u64; 1];
                t[0] = read_le(key.as_ptr(), key.len());
                let t = std::mem::transmute::<_, InlineKey<0>>(t);
                translate::<0b101, _, K, V>(self.table1.insert(t))
            }
            9..=16 => {
                if unlikely((self.table2.len() + 1) * 2 > self.table2.capacity()) {
                    if (self.table2.entries.len() >> 22) == 0 {
                        self.table2.grow(2);
                    } else {
                        self.table2.grow(1);
                    }
                }
                let mut t = [0u64; 2];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = read_le(key.as_ptr().offset(8), key.len() - 8);
                let t = std::mem::transmute::<_, InlineKey<1>>(t);
                translate::<0b110, _, K, V>(self.table2.insert(t))
            }
            17..=24 => {
                if unlikely((self.table3.len() + 1) * 2 > self.table3.capacity()) {
                    if (self.table3.entries.len() >> 22) == 0 {
                        self.table3.grow(2);
                    } else {
                        self.table3.grow(1);
                    }
                }
                let mut t = [0u64; 3];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = (key.as_ptr() as *const u64).offset(1).read_unaligned();
                t[2] = read_le(key.as_ptr().offset(16), key.len() - 16);
                let t = std::mem::transmute::<_, InlineKey<2>>(t);
                translate::<0b111, _, K, V>(self.table3.insert(t))
            }
            _ => {
                if unlikely((self.table4.len() + 1) * 2 > self.table4.capacity()) {
                    if (self.table4.entries.len() >> 22) == 0 {
                        self.table4.grow(2);
                    } else {
                        self.table4.grow(1);
                    }
                }
                translate::<0b000, _, K, V>(self.table4.insert(FallbackKey::new(key)))
            }
        }
    }
    #[inline(always)]
    pub unsafe fn insert(&mut self, key: &K) -> Result<&mut MaybeUninit<V>, &mut V> {
        let key = key.as_bytes();
        match key.len() {
            _ if key.last().copied() == Some(0) => {
                if unlikely((self.table4.len() + 1) * 2 > self.table4.capacity()) {
                    if (self.table4.entries.len() >> 22) == 0 {
                        self.table4.grow(2);
                    } else {
                        self.table4.grow(1);
                    }
                }
                let s = self.arena.alloc_slice_copy(key);
                match self.table4.insert(FallbackKey::new(s)) {
                    Ok(e) => Ok(&mut e.val),
                    Err(e) => Err(e.val.assume_init_mut()),
                }
            }
            0 => match self.table0.insert([0, 0]) {
                Ok(e) => Ok(&mut e.val),
                Err(e) => Err(e.val.assume_init_mut()),
            },
            1 => match self.table0.insert([key[0], 0]) {
                Ok(e) => Ok(&mut e.val),
                Err(e) => Err(e.val.assume_init_mut()),
            },
            2 => match self.table0.insert([key[0], key[1]]) {
                Ok(e) => Ok(&mut e.val),
                Err(e) => Err(e.val.assume_init_mut()),
            },
            3..=8 => {
                if unlikely((self.table1.len() + 1) * 2 > self.table1.capacity()) {
                    if (self.table1.entries.len() >> 22) == 0 {
                        self.table1.grow(2);
                    } else {
                        self.table1.grow(1);
                    }
                }
                let mut t = [0u64; 1];
                t[0] = read_le(key.as_ptr(), key.len());
                let t = std::mem::transmute::<_, InlineKey<0>>(t);
                match self.table1.insert(t) {
                    Ok(e) => Ok(&mut e.val),
                    Err(e) => Err(e.val.assume_init_mut()),
                }
            }
            9..=16 => {
                if unlikely((self.table2.len() + 1) * 2 > self.table2.capacity()) {
                    if (self.table2.entries.len() >> 22) == 0 {
                        self.table2.grow(2);
                    } else {
                        self.table2.grow(1);
                    }
                }
                let mut t = [0u64; 2];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = read_le(key.as_ptr().offset(8), key.len() - 8);
                let t = std::mem::transmute::<_, InlineKey<1>>(t);
                match self.table2.insert(t) {
                    Ok(e) => Ok(&mut e.val),
                    Err(e) => Err(e.val.assume_init_mut()),
                }
            }
            17..=24 => {
                if unlikely((self.table3.len() + 1) * 2 > self.table3.capacity()) {
                    if (self.table3.entries.len() >> 22) == 0 {
                        self.table3.grow(2);
                    } else {
                        self.table3.grow(1);
                    }
                }
                let mut t = [0u64; 3];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = (key.as_ptr() as *const u64).offset(1).read_unaligned();
                t[2] = read_le(key.as_ptr().offset(16), key.len() - 16);
                let t = std::mem::transmute::<_, InlineKey<2>>(t);
                match self.table3.insert(t) {
                    Ok(e) => Ok(&mut e.val),
                    Err(e) => Err(e.val.assume_init_mut()),
                }
            }
            _ => {
                if unlikely((self.table4.len() + 1) * 2 > self.table4.capacity()) {
                    if (self.table4.entries.len() >> 22) == 0 {
                        self.table4.grow(2);
                    } else {
                        self.table4.grow(1);
                    }
                }
                let s = self.arena.alloc_slice_copy(key);
                match self.table4.insert(FallbackKey::new(s)) {
                    Ok(e) => Ok(&mut e.val),
                    Err(e) => Err(e.val.assume_init_mut()),
                }
            }
        }
    }
    #[inline(always)]
    pub unsafe fn insert_borrowing(&mut self, key: &K) -> Result<&mut MaybeUninit<V>, &mut V> {
        let key = key.as_bytes();
        match key.len() {
            _ if key.last().copied() == Some(0) => {
                if unlikely((self.table4.len() + 1) * 2 > self.table4.capacity()) {
                    if (self.table4.entries.len() >> 22) == 0 {
                        self.table4.grow(2);
                    } else {
                        self.table4.grow(1);
                    }
                }
                match self.table4.insert(FallbackKey::new(key)) {
                    Ok(e) => Ok(&mut e.val),
                    Err(e) => Err(e.val.assume_init_mut()),
                }
            }
            0 => match self.table0.insert([0, 0]) {
                Ok(e) => Ok(&mut e.val),
                Err(e) => Err(e.val.assume_init_mut()),
            },
            1 => match self.table0.insert([key[0], 0]) {
                Ok(e) => Ok(&mut e.val),
                Err(e) => Err(e.val.assume_init_mut()),
            },
            2 => match self.table0.insert([key[0], key[1]]) {
                Ok(e) => Ok(&mut e.val),
                Err(e) => Err(e.val.assume_init_mut()),
            },
            3..=8 => {
                if unlikely((self.table1.len() + 1) * 2 > self.table1.capacity()) {
                    if (self.table1.entries.len() >> 22) == 0 {
                        self.table1.grow(2);
                    } else {
                        self.table1.grow(1);
                    }
                }
                let mut t = [0u64; 1];
                t[0] = read_le(key.as_ptr(), key.len());
                let t = std::mem::transmute::<_, InlineKey<0>>(t);
                match self.table1.insert(t) {
                    Ok(e) => Ok(&mut e.val),
                    Err(e) => Err(e.val.assume_init_mut()),
                }
            }
            9..=16 => {
                if unlikely((self.table2.len() + 1) * 2 > self.table2.capacity()) {
                    if (self.table2.entries.len() >> 22) == 0 {
                        self.table2.grow(2);
                    } else {
                        self.table2.grow(1);
                    }
                }
                let mut t = [0u64; 2];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = read_le(key.as_ptr().offset(8), key.len() - 8);
                let t = std::mem::transmute::<_, InlineKey<1>>(t);
                match self.table2.insert(t) {
                    Ok(e) => Ok(&mut e.val),
                    Err(e) => Err(e.val.assume_init_mut()),
                }
            }
            17..=24 => {
                if unlikely((self.table3.len() + 1) * 2 > self.table3.capacity()) {
                    if (self.table3.entries.len() >> 22) == 0 {
                        self.table3.grow(2);
                    } else {
                        self.table3.grow(1);
                    }
                }
                let mut t = [0u64; 3];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = (key.as_ptr() as *const u64).offset(1).read_unaligned();
                t[2] = read_le(key.as_ptr().offset(16), key.len() - 16);
                let t = std::mem::transmute::<_, InlineKey<2>>(t);
                match self.table3.insert(t) {
                    Ok(e) => Ok(&mut e.val),
                    Err(e) => Err(e.val.assume_init_mut()),
                }
            }
            _ => {
                if unlikely((self.table4.len() + 1) * 2 > self.table4.capacity()) {
                    if (self.table4.entries.len() >> 22) == 0 {
                        self.table4.grow(2);
                    } else {
                        self.table4.grow(1);
                    }
                }
                let s = self.arena.alloc_slice_copy(key);
                match self.table4.insert(FallbackKey::new(s)) {
                    Ok(e) => Ok(&mut e.val),
                    Err(e) => Err(e.val.assume_init_mut()),
                }
            }
        }
    }
    pub fn iter(&self) -> UnsizedHashtableIter<'_, K, V> {
        UnsizedHashtableIter {
            it_0: Some(self.table0.iter()),
            it_1: Some(self.table1.iter()),
            it_2: Some(self.table2.iter()),
            it_3: Some(self.table3.iter()),
            it_4: Some(self.table4.iter()),
            _phantom: PhantomData,
        }
    }
    pub fn iter_mut(&mut self) -> UnsizedHashtableIterMut<'_, K, V> {
        UnsizedHashtableIterMut {
            it_0: Some(self.table0.iter_mut()),
            it_1: Some(self.table1.iter_mut()),
            it_2: Some(self.table2.iter_mut()),
            it_3: Some(self.table3.iter_mut()),
            it_4: Some(self.table4.iter_mut()),
            _phantom: PhantomData,
        }
    }
    pub fn iter_ptr(&self) -> UnsizedHashtableIterPtr<K, V> {
        UnsizedHashtableIterPtr {
            it_0: Some(self.table0.iter_ptr()),
            it_1: Some(self.table1.iter_ptr()),
            it_2: Some(self.table2.iter_ptr()),
            it_3: Some(self.table3.iter_ptr()),
            it_4: Some(self.table4.iter_ptr()),
            _phantom: PhantomData,
        }
    }
    pub fn iter_mut_ptr(&self) -> UnsizedHashtableIterMutPtr<K, V> {
        UnsizedHashtableIterMutPtr {
            it_0: Some(self.table0.iter_mut_ptr()),
            it_1: Some(self.table1.iter_mut_ptr()),
            it_2: Some(self.table2.iter_mut_ptr()),
            it_3: Some(self.table3.iter_mut_ptr()),
            it_4: Some(self.table4.iter_mut_ptr()),
            _phantom: PhantomData,
        }
    }
}

pub struct UnsizedHashtableIter<'a, K, V>
where K: UnsizedKeyable + ?Sized
{
    it_0: Option<Table1Iter<'a, V>>,
    it_1: Option<Table0Iter<'a, InlineKey<0>, V>>,
    it_2: Option<Table0Iter<'a, InlineKey<1>, V>>,
    it_3: Option<Table0Iter<'a, InlineKey<2>, V>>,
    it_4: Option<Table0Iter<'a, FallbackKey, V>>,
    _phantom: PhantomData<&'a mut K>,
}

impl<'a, K, V> Iterator for UnsizedHashtableIter<'a, K, V>
where K: UnsizedKeyable + ?Sized
{
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(it) = self.it_0.as_mut() {
            if let Some(e) = it.next() {
                unsafe {
                    let key = e.key.assume_init_ref();
                    let val = e.val.assume_init_ref();
                    if key[1] != 0 {
                        return Some((UnsizedKeyable::from_bytes(&key[..2]), val));
                    } else if key[0] != 0 {
                        return Some((UnsizedKeyable::from_bytes(&key[..1]), val));
                    } else {
                        return Some((UnsizedKeyable::from_bytes(&key[..0]), val));
                    }
                }
            }
            self.it_0 = None;
        }
        if let Some(it) = self.it_1.as_mut() {
            if let Some(e) = it.next() {
                let bytes = e.key().1.get().to_le_bytes();
                unsafe {
                    for i in (0..=7).rev() {
                        if bytes[i] != 0 {
                            let key = UnsizedKeyable::from_bytes(std::slice::from_raw_parts(
                                e.key() as *const _ as *const u8,
                                i + 1,
                            ));
                            let val = e.get();
                            return Some((key, val));
                        }
                    }
                }
                unreachable!()
            }
            self.it_1 = None;
        }
        if let Some(it) = self.it_2.as_mut() {
            if let Some(e) = it.next() {
                let bytes = e.key().1.get().to_le_bytes();
                unsafe {
                    for i in (0..=7).rev() {
                        if bytes[i] != 0 {
                            let key = UnsizedKeyable::from_bytes(std::slice::from_raw_parts(
                                e.key() as *const _ as *const u8,
                                i + 9,
                            ));
                            let val = e.get();
                            return Some((key, val));
                        }
                    }
                }
                unreachable!()
            }
            self.it_2 = None;
        }
        if let Some(it) = self.it_3.as_mut() {
            if let Some(e) = it.next() {
                let bytes = e.key().1.get().to_le_bytes();
                unsafe {
                    for i in (0..=7).rev() {
                        if bytes[i] != 0 {
                            let key = UnsizedKeyable::from_bytes(std::slice::from_raw_parts(
                                e.key() as *const _ as *const u8,
                                i + 17,
                            ));
                            let val = e.get();
                            return Some((key, val));
                        }
                    }
                };
                unreachable!();
            }
            self.it_3 = None;
        }
        if let Some(it) = self.it_4.as_mut() {
            if let Some(e) = it.next() {
                let key = unsafe { UnsizedKeyable::from_bytes(e.key().key.unwrap().as_ref()) };
                let val = e.get();
                return Some((key, val));
            }
            self.it_4 = None;
        }
        None
    }
}

pub struct UnsizedHashtableIterMut<'a, K, V>
where K: UnsizedKeyable + ?Sized
{
    it_0: Option<Table1IterMut<'a, V>>,
    it_1: Option<Table0IterMut<'a, InlineKey<0>, V>>,
    it_2: Option<Table0IterMut<'a, InlineKey<1>, V>>,
    it_3: Option<Table0IterMut<'a, InlineKey<2>, V>>,
    it_4: Option<Table0IterMut<'a, FallbackKey, V>>,
    _phantom: PhantomData<&'a mut K>,
}

impl<'a, K, V> Iterator for UnsizedHashtableIterMut<'a, K, V>
where K: UnsizedKeyable + ?Sized
{
    type Item = (&'a K, &'a mut V);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(it) = self.it_0.as_mut() {
            if let Some(e) = it.next() {
                unsafe {
                    let key = e.key.assume_init_ref();
                    let val = e.val.assume_init_mut();
                    if key[1] != 0 {
                        return Some((UnsizedKeyable::from_bytes(&key[..2]), val));
                    } else if key[0] != 0 {
                        return Some((UnsizedKeyable::from_bytes(&key[..1]), val));
                    } else {
                        return Some((UnsizedKeyable::from_bytes(&key[..0]), val));
                    }
                }
            }
            self.it_0 = None;
        }
        if let Some(it) = self.it_1.as_mut() {
            if let Some(e) = it.next() {
                let bytes = e.key().1.get().to_le_bytes();
                unsafe {
                    for i in (0..=7).rev() {
                        if bytes[i] != 0 {
                            let key = UnsizedKeyable::from_bytes(std::slice::from_raw_parts(
                                e.key() as *const _ as *const u8,
                                i + 1,
                            ));
                            let val = e.get_mut();
                            return Some((key, val));
                        }
                    }
                }
                unreachable!()
            }
            self.it_1 = None;
        }
        if let Some(it) = self.it_2.as_mut() {
            if let Some(e) = it.next() {
                let bytes = e.key().1.get().to_le_bytes();
                unsafe {
                    for i in (0..=7).rev() {
                        if bytes[i] != 0 {
                            let key = UnsizedKeyable::from_bytes(std::slice::from_raw_parts(
                                e.key() as *const _ as *const u8,
                                i + 9,
                            ));
                            let val = e.get_mut();
                            return Some((key, val));
                        }
                    }
                }
                unreachable!()
            }
            self.it_2 = None;
        }
        if let Some(it) = self.it_3.as_mut() {
            if let Some(e) = it.next() {
                let bytes = e.key().1.get().to_le_bytes();
                unsafe {
                    for i in (0..=7).rev() {
                        if bytes[i] != 0 {
                            let key = UnsizedKeyable::from_bytes(std::slice::from_raw_parts(
                                e.key() as *const _ as *const u8,
                                i + 17,
                            ));
                            let val = e.get_mut();
                            return Some((key, val));
                        }
                    }
                };
                unreachable!();
            }
            self.it_3 = None;
        }
        if let Some(it) = self.it_4.as_mut() {
            if let Some(e) = it.next() {
                let key = unsafe { UnsizedKeyable::from_bytes(e.key().key.unwrap().as_ref()) };
                let val = e.get_mut();
                return Some((key, val));
            }
            self.it_4 = None;
        }
        None
    }
}

pub struct UnsizedHashtableIterPtr<K, V>
where K: UnsizedKeyable + ?Sized
{
    it_0: Option<Table1IterPtr<V>>,
    it_1: Option<Table0IterPtr<InlineKey<0>, V>>,
    it_2: Option<Table0IterPtr<InlineKey<1>, V>>,
    it_3: Option<Table0IterPtr<InlineKey<2>, V>>,
    it_4: Option<Table0IterPtr<FallbackKey, V>>,
    _phantom: PhantomData<*mut K>,
}

impl<K, V> Iterator for UnsizedHashtableIterPtr<K, V>
where K: UnsizedKeyable + ?Sized
{
    type Item = *const UnsizedHashtableFakeEntry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(it) = self.it_0.as_mut() {
            if let Some(e) = it.next() {
                return Some(
                    (e as usize | (0b100 << 61)) as *const UnsizedHashtableFakeEntry<K, V>,
                );
            }
            self.it_0 = None;
        }
        if let Some(it) = self.it_1.as_mut() {
            if let Some(e) = it.next() {
                return Some(
                    (e as usize | (0b101 << 61)) as *const UnsizedHashtableFakeEntry<K, V>,
                );
            }
            self.it_1 = None;
        }
        if let Some(it) = self.it_2.as_mut() {
            if let Some(e) = it.next() {
                return Some(
                    (e as usize | (0b110 << 61)) as *const UnsizedHashtableFakeEntry<K, V>,
                );
            }
            self.it_2 = None;
        }
        if let Some(it) = self.it_3.as_mut() {
            if let Some(e) = it.next() {
                return Some(
                    (e as usize | (0b111 << 61)) as *const UnsizedHashtableFakeEntry<K, V>,
                );
            }
            self.it_3 = None;
        }
        if let Some(it) = self.it_4.as_mut() {
            if let Some(e) = it.next() {
                return Some(
                    (e as usize | (0b000 << 61)) as *const UnsizedHashtableFakeEntry<K, V>,
                );
            }
            self.it_4 = None;
        }
        None
    }
}

pub struct UnsizedHashtableIterMutPtr<K, V>
where K: UnsizedKeyable + ?Sized
{
    it_0: Option<Table1IterMutPtr<V>>,
    it_1: Option<Table0IterMutPtr<InlineKey<0>, V>>,
    it_2: Option<Table0IterMutPtr<InlineKey<1>, V>>,
    it_3: Option<Table0IterMutPtr<InlineKey<2>, V>>,
    it_4: Option<Table0IterMutPtr<FallbackKey, V>>,
    _phantom: PhantomData<*mut K>,
}

impl<K, V> Iterator for UnsizedHashtableIterMutPtr<K, V>
where K: UnsizedKeyable + ?Sized
{
    type Item = *mut UnsizedHashtableFakeEntry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(it) = self.it_0.as_mut() {
            if let Some(e) = it.next() {
                return Some((e as usize | (0b100 << 61)) as *mut UnsizedHashtableFakeEntry<K, V>);
            }
            self.it_0 = None;
        }
        if let Some(it) = self.it_1.as_mut() {
            if let Some(e) = it.next() {
                return Some((e as usize | (0b101 << 61)) as *mut UnsizedHashtableFakeEntry<K, V>);
            }
            self.it_1 = None;
        }
        if let Some(it) = self.it_2.as_mut() {
            if let Some(e) = it.next() {
                return Some((e as usize | (0b110 << 61)) as *mut UnsizedHashtableFakeEntry<K, V>);
            }
            self.it_2 = None;
        }
        if let Some(it) = self.it_3.as_mut() {
            if let Some(e) = it.next() {
                return Some((e as usize | (0b111 << 61)) as *mut UnsizedHashtableFakeEntry<K, V>);
            }
            self.it_3 = None;
        }
        if let Some(it) = self.it_4.as_mut() {
            if let Some(e) = it.next() {
                return Some((e as usize | (0b000 << 61)) as *mut UnsizedHashtableFakeEntry<K, V>);
            }
            self.it_4 = None;
        }
        None
    }
}

pub struct UnsizedHashtableFakeEntry<K, V>(PhantomData<K>, PhantomData<V>)
where K: UnsizedKeyable + ?Sized;
// 63 62 61 60 ... 0
//  1  0  0     addr => address of table0 entry
//  1  0  1     addr => address of table1 entry
//  1  1  0     addr => address of table2 entry
//  1  1  1     addr => address of table3 entry
//  0  0  0     addr => address of table4 entry

impl<K, V> UnsizedHashtableFakeEntry<K, V>
where K: UnsizedKeyable + ?Sized
{
    pub unsafe fn key(self: *const Self) -> &'static K {
        match self as usize >> 61 {
            0b100 => {
                let e = &(*((self as usize & ((1 << 61) - 1)) as *const Entry<[u8; 2], V>));
                let key = e.key.assume_init_ref();
                if key[1] != 0 {
                    UnsizedKeyable::from_bytes(&key[..2])
                } else if key[0] != 0 {
                    UnsizedKeyable::from_bytes(&key[..1])
                } else {
                    UnsizedKeyable::from_bytes(&key[..0])
                }
            }
            0b101 => {
                let e = &(*((self as usize & ((1 << 61) - 1)) as *const Entry<InlineKey<0>, V>));
                let bytes = e.key().1.get().to_le_bytes();
                for i in (0..=7).rev() {
                    if bytes[i] != 0 {
                        return UnsizedKeyable::from_bytes(std::slice::from_raw_parts(
                            e.key() as *const _ as *const u8,
                            i + 1,
                        ));
                    }
                }
                unreachable!()
            }
            0b110 => {
                let e = &(*((self as usize & ((1 << 61) - 1)) as *const Entry<InlineKey<1>, V>));
                let bytes = e.key().1.get().to_le_bytes();
                for i in (0..=7).rev() {
                    if bytes[i] != 0 {
                        return UnsizedKeyable::from_bytes(std::slice::from_raw_parts(
                            e.key() as *const _ as *const u8,
                            i + 9,
                        ));
                    }
                }
                unreachable!()
            }
            0b111 => {
                let e = &(*((self as usize & ((1 << 61) - 1)) as *const Entry<InlineKey<2>, V>));
                let bytes = e.key().1.get().to_le_bytes();
                for i in (0..=7).rev() {
                    if bytes[i] != 0 {
                        return UnsizedKeyable::from_bytes(std::slice::from_raw_parts(
                            e.key() as *const _ as *const u8,
                            i + 17,
                        ));
                    }
                }
                unreachable!()
            }
            0b000 => {
                let e = &(*((self as usize & ((1 << 61) - 1)) as *const Entry<FallbackKey, V>));
                UnsizedKeyable::from_bytes(e.key.assume_init().key.unwrap().as_ref())
            }
            _ => unreachable!(),
        }
    }
    pub unsafe fn get(self: *const Self) -> &'static V {
        const MASK: usize = (1 << 61) - 1;
        match self as usize >> 61 {
            0b100 => (*((self as usize & MASK) as *const Entry<[u8; 2], V>)).get(),
            0b101 => (*((self as usize & MASK) as *const Entry<InlineKey<0>, V>)).get(),
            0b110 => (*((self as usize & MASK) as *const Entry<InlineKey<1>, V>)).get(),
            0b111 => (*((self as usize & MASK) as *const Entry<InlineKey<2>, V>)).get(),
            0b000 => (*((self as usize & MASK) as *const Entry<FallbackKey, V>)).get(),
            _ => unreachable!(),
        }
    }
    pub unsafe fn get_mut(self: *mut Self) -> &'static mut V {
        const MASK: usize = (1 << 61) - 1;
        match self as usize >> 61 {
            0b100 => (*((self as usize & MASK) as *mut Entry<[u8; 2], V>)).get_mut(),
            0b101 => (*((self as usize & MASK) as *mut Entry<InlineKey<0>, V>)).get_mut(),
            0b110 => (*((self as usize & MASK) as *mut Entry<InlineKey<1>, V>)).get_mut(),
            0b111 => (*((self as usize & MASK) as *mut Entry<InlineKey<2>, V>)).get_mut(),
            0b000 => (*((self as usize & MASK) as *mut Entry<FallbackKey, V>)).get_mut(),
            _ => unreachable!(),
        }
    }
    pub unsafe fn write(self: *mut Self, val: V) {
        const MASK: usize = (1 << 61) - 1;
        match self as usize >> 61 {
            0b100 => {
                (*((self as usize & MASK) as *mut Entry<[u8; 2], V>))
                    .val
                    .write(val);
            }
            0b101 => {
                (*((self as usize & MASK) as *mut Entry<InlineKey<0>, V>))
                    .val
                    .write(val);
            }
            0b110 => {
                (*((self as usize & MASK) as *mut Entry<InlineKey<1>, V>))
                    .val
                    .write(val);
            }
            0b111 => {
                (*((self as usize & MASK) as *mut Entry<InlineKey<2>, V>))
                    .val
                    .write(val);
            }
            0b000 => {
                (*((self as usize & MASK) as *mut Entry<FallbackKey, V>))
                    .val
                    .write(val);
            }
            _ => unreachable!(),
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct InlineKey<const N: usize>(pub [u64; N], pub NonZeroU64);

unsafe impl<const N: usize> Keyable for InlineKey<N> {
    #[inline(always)]
    fn equals_zero(_: &Self) -> bool {
        false
    }

    #[inline(always)]
    fn is_zero(this: &MaybeUninit<Self>) -> bool {
        unsafe { *(this as *const _ as *const u64).add(N) == 0 }
    }

    #[inline(always)]
    fn hash(&self) -> u64 {
        (self.0, self.1).fast_hash()
    }
}

#[derive(Copy, Clone)]
pub(crate) struct FallbackKey {
    key: Option<NonNull<[u8]>>,
    hash: u64,
}

impl FallbackKey {
    unsafe fn new(key: &[u8]) -> Self {
        Self {
            key: Some(NonNull::from(key)),
            hash: key.fast_hash(),
        }
    }
}

impl PartialEq for FallbackKey {
    fn eq(&self, other: &Self) -> bool {
        if self.hash == other.hash {
            unsafe { self.key.map(|x| x.as_ref()) == other.key.map(|x| x.as_ref()) }
        } else {
            false
        }
    }
}

impl Eq for FallbackKey {}

unsafe impl Keyable for FallbackKey {
    #[inline(always)]
    fn equals_zero(_: &Self) -> bool {
        false
    }

    #[inline(always)]
    fn is_zero(this: &MaybeUninit<Self>) -> bool {
        unsafe { this.assume_init_ref().key.is_none() }
    }

    #[inline(always)]
    fn hash(&self) -> u64 {
        self.hash
    }
}

fn translate<const X: usize, I, O, V>(
    r: Result<&mut Entry<I, V>, &mut Entry<I, V>>,
) -> Result<*mut UnsizedHashtableFakeEntry<O, V>, *mut UnsizedHashtableFakeEntry<O, V>>
where O: UnsizedKeyable + ?Sized {
    match r {
        Ok(x) => Ok((x as *mut _ as usize | (X << 61)) as *mut UnsizedHashtableFakeEntry<O, V>),
        Err(x) => Err((x as *mut _ as usize | (X << 61)) as *mut UnsizedHashtableFakeEntry<O, V>),
    }
}
