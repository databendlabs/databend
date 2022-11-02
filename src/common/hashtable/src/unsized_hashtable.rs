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
use common_base::mem_allocator::GlobalAllocator;
use common_base::mem_allocator::MmapAllocator;

use super::container::HeapContainer;
use super::table0::Entry;
use super::table0::Table0;
use super::table1::Table1;
use super::traits::EntryMutRefLike;
use super::traits::EntryRefLike;
use super::traits::FastHash;
use super::traits::HashtableLike;
use super::traits::Keyable;
use super::traits::UnsizedKeyable;
use super::utils::read_le;
use crate::table0::Table0Iter;
use crate::table0::Table0IterMut;
use crate::table1::Table1Iter;
use crate::table1::Table1IterMut;

pub struct UnsizedHashtable<K, V, A = MmapAllocator<GlobalAllocator>>
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

impl<K, V, A> Default for UnsizedHashtable<K, V, A>
where
    K: UnsizedKeyable + ?Sized,
    A: Allocator + Clone + Default,
{
    fn default() -> Self {
        Self::new()
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
    pub fn entry(&self, key: &K) -> Option<UnsizedHashtableEntryRef<'_, K, V>> {
        let key = key.as_bytes();
        match key.len() {
            _ if key.last().copied() == Some(0) => unsafe {
                self.table4
                    .get(&FallbackKey::new(key))
                    .map(|x| UnsizedHashtableEntryRef(UnsizedHashtableEntryRefInner::Table4(x)))
            },
            0 => self.table0.get([0, 0]).map(|x| {
                UnsizedHashtableEntryRef(UnsizedHashtableEntryRefInner::Table0(x, PhantomData))
            }),
            1 => self.table0.get([key[0], 0]).map(|x| {
                UnsizedHashtableEntryRef(UnsizedHashtableEntryRefInner::Table0(x, PhantomData))
            }),
            2 => self.table0.get([key[0], key[1]]).map(|x| {
                UnsizedHashtableEntryRef(UnsizedHashtableEntryRefInner::Table0(x, PhantomData))
            }),
            3..=8 => unsafe {
                let mut t = [0u64; 1];
                t[0] = read_le(key.as_ptr(), key.len());
                let t = std::mem::transmute::<_, InlineKey<0>>(t);
                self.table1
                    .get(&t)
                    .map(|x| UnsizedHashtableEntryRef(UnsizedHashtableEntryRefInner::Table1(x)))
            },
            9..=16 => unsafe {
                let mut t = [0u64; 2];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = read_le(key.as_ptr().offset(8), key.len() - 8);
                let t = std::mem::transmute::<_, InlineKey<1>>(t);
                self.table2
                    .get(&t)
                    .map(|x| UnsizedHashtableEntryRef(UnsizedHashtableEntryRefInner::Table2(x)))
            },
            17..=24 => unsafe {
                let mut t = [0u64; 3];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = (key.as_ptr() as *const u64).offset(1).read_unaligned();
                t[2] = read_le(key.as_ptr().offset(16), key.len() - 16);
                let t = std::mem::transmute::<_, InlineKey<2>>(t);
                self.table3
                    .get(&t)
                    .map(|x| UnsizedHashtableEntryRef(UnsizedHashtableEntryRefInner::Table3(x)))
            },
            _ => unsafe {
                self.table4
                    .get(&FallbackKey::new(key))
                    .map(|x| UnsizedHashtableEntryRef(UnsizedHashtableEntryRefInner::Table4(x)))
            },
        }
    }
    #[inline(always)]
    pub fn entry_mut(&mut self, key: &K) -> Option<UnsizedHashtableEntryMutRef<'_, K, V>> {
        let key = key.as_bytes();
        match key.len() {
            _ if key.last().copied() == Some(0) => unsafe {
                self.table4.get_mut(&FallbackKey::new(key)).map(|x| {
                    UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table4(x))
                })
            },
            0 => self.table0.get_mut([0, 0]).map(|x| {
                UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table0(
                    x,
                    PhantomData,
                ))
            }),
            1 => self.table0.get_mut([key[0], 0]).map(|x| {
                UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table0(
                    x,
                    PhantomData,
                ))
            }),
            2 => self.table0.get_mut([key[0], key[1]]).map(|x| {
                UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table0(
                    x,
                    PhantomData,
                ))
            }),
            3..=8 => unsafe {
                let mut t = [0u64; 1];
                t[0] = read_le(key.as_ptr(), key.len());
                let t = std::mem::transmute::<_, InlineKey<0>>(t);
                self.table1.get_mut(&t).map(|x| {
                    UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table1(x))
                })
            },
            9..=16 => unsafe {
                let mut t = [0u64; 2];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = read_le(key.as_ptr().offset(8), key.len() - 8);
                let t = std::mem::transmute::<_, InlineKey<1>>(t);
                self.table2.get_mut(&t).map(|x| {
                    UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table2(x))
                })
            },
            17..=24 => unsafe {
                let mut t = [0u64; 3];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = (key.as_ptr() as *const u64).offset(1).read_unaligned();
                t[2] = read_le(key.as_ptr().offset(16), key.len() - 16);
                let t = std::mem::transmute::<_, InlineKey<2>>(t);
                self.table3.get_mut(&t).map(|x| {
                    UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table3(x))
                })
            },
            _ => unsafe {
                self.table4.get_mut(&FallbackKey::new(key)).map(|x| {
                    UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table4(x))
                })
            },
        }
    }
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<&V> {
        self.entry(key).map(|e| e.get())
    }
    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.entry_mut(key)
            .map(|e| unsafe { &mut *(e.get_mut_ptr() as *mut V) })
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
        key: &K,
    ) -> Result<UnsizedHashtableEntryMutRef<'_, K, V>, UnsizedHashtableEntryMutRef<'_, K, V>> {
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
                self.table4
                    .insert(FallbackKey::new(s))
                    .map(|x| {
                        UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table4(x))
                    })
                    .map_err(|x| {
                        UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table4(x))
                    })
            }
            0 => self
                .table0
                .insert([0, 0])
                .map(|x| {
                    UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table0(
                        x,
                        PhantomData,
                    ))
                })
                .map_err(|x| {
                    UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table0(
                        x,
                        PhantomData,
                    ))
                }),
            1 => self
                .table0
                .insert([key[0], 0])
                .map(|x| {
                    UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table0(
                        x,
                        PhantomData,
                    ))
                })
                .map_err(|x| {
                    UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table0(
                        x,
                        PhantomData,
                    ))
                }),
            2 => self
                .table0
                .insert([key[0], key[1]])
                .map(|x| {
                    UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table0(
                        x,
                        PhantomData,
                    ))
                })
                .map_err(|x| {
                    UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table0(
                        x,
                        PhantomData,
                    ))
                }),
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
                self.table1
                    .insert(t)
                    .map(|x| {
                        UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table1(x))
                    })
                    .map_err(|x| {
                        UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table1(x))
                    })
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
                self.table2
                    .insert(t)
                    .map(|x| {
                        UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table2(x))
                    })
                    .map_err(|x| {
                        UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table2(x))
                    })
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
                self.table3
                    .insert(t)
                    .map(|x| {
                        UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table3(x))
                    })
                    .map_err(|x| {
                        UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table3(x))
                    })
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
                self.table4
                    .insert(FallbackKey::new(s))
                    .map(|x| {
                        UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table4(x))
                    })
                    .map_err(|x| {
                        UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table4(x))
                    })
            }
        }
    }
    /// # Safety
    ///
    /// The uninitialized value of returned entry should be written immediately.
    #[inline(always)]
    pub unsafe fn insert(&mut self, key: &K) -> Result<&mut MaybeUninit<V>, &mut V> {
        match self.insert_and_entry(key) {
            Ok(e) => Ok(&mut *(e.get_mut_ptr() as *mut MaybeUninit<V>)),
            Err(e) => Err(&mut *e.get_mut_ptr()),
        }
    }
    /// # Safety
    ///
    /// * The uninitialized value of returned entry should be written immediately.
    /// * The lifetime of key lives longer than the hashtable.
    #[inline(always)]
    pub unsafe fn insert_and_entry_borrowing(
        &mut self,
        key: *const K,
    ) -> Result<UnsizedHashtableEntryMutRef<'_, K, V>, UnsizedHashtableEntryMutRef<'_, K, V>> {
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
                self.table4
                    .insert(FallbackKey::new(key))
                    .map(|x| {
                        UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table4(x))
                    })
                    .map_err(|x| {
                        UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table4(x))
                    })
            }
            0 => self
                .table0
                .insert([0, 0])
                .map(|x| {
                    UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table0(
                        x,
                        PhantomData,
                    ))
                })
                .map_err(|x| {
                    UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table0(
                        x,
                        PhantomData,
                    ))
                }),
            1 => self
                .table0
                .insert([key[0], 0])
                .map(|x| {
                    UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table0(
                        x,
                        PhantomData,
                    ))
                })
                .map_err(|x| {
                    UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table0(
                        x,
                        PhantomData,
                    ))
                }),
            2 => self
                .table0
                .insert([key[0], key[1]])
                .map(|x| {
                    UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table0(
                        x,
                        PhantomData,
                    ))
                })
                .map_err(|x| {
                    UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table0(
                        x,
                        PhantomData,
                    ))
                }),
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
                self.table1
                    .insert(t)
                    .map(|x| {
                        UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table1(x))
                    })
                    .map_err(|x| {
                        UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table1(x))
                    })
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
                self.table2
                    .insert(t)
                    .map(|x| {
                        UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table2(x))
                    })
                    .map_err(|x| {
                        UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table2(x))
                    })
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
                self.table3
                    .insert(t)
                    .map(|x| {
                        UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table3(x))
                    })
                    .map_err(|x| {
                        UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table3(x))
                    })
            }
            _ => {
                if unlikely((self.table4.len() + 1) * 2 > self.table4.capacity()) {
                    if (self.table4.entries.len() >> 22) == 0 {
                        self.table4.grow(2);
                    } else {
                        self.table4.grow(1);
                    }
                }
                self.table4
                    .insert(FallbackKey::new(key))
                    .map(|x| {
                        UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table4(x))
                    })
                    .map_err(|x| {
                        UnsizedHashtableEntryMutRef(UnsizedHashtableEntryMutRefInner::Table4(x))
                    })
            }
        }
    }
    /// # Safety
    ///
    /// * The uninitialized value of returned entry should be written immediately.
    /// * The lifetime of key lives longer than the hashtable.
    #[inline(always)]
    pub unsafe fn insert_borrowing(&mut self, key: &K) -> Result<&mut MaybeUninit<V>, &mut V> {
        match self.insert_and_entry_borrowing(key) {
            Ok(e) => Ok(&mut *(e.get_mut_ptr() as *mut MaybeUninit<V>)),
            Err(e) => Err(&mut *e.get_mut_ptr()),
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
    type Item = UnsizedHashtableEntryRef<'a, K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(it) = self.it_0.as_mut() {
            if let Some(e) = it.next() {
                return Some(UnsizedHashtableEntryRef(
                    UnsizedHashtableEntryRefInner::Table0(e, PhantomData),
                ));
            }
            self.it_0 = None;
        }
        if let Some(it) = self.it_1.as_mut() {
            if let Some(e) = it.next() {
                return Some(UnsizedHashtableEntryRef(
                    UnsizedHashtableEntryRefInner::Table1(e),
                ));
            }
            self.it_1 = None;
        }
        if let Some(it) = self.it_2.as_mut() {
            if let Some(e) = it.next() {
                return Some(UnsizedHashtableEntryRef(
                    UnsizedHashtableEntryRefInner::Table2(e),
                ));
            }
            self.it_2 = None;
        }
        if let Some(it) = self.it_3.as_mut() {
            if let Some(e) = it.next() {
                return Some(UnsizedHashtableEntryRef(
                    UnsizedHashtableEntryRefInner::Table3(e),
                ));
            }
            self.it_3 = None;
        }
        if let Some(it) = self.it_4.as_mut() {
            if let Some(e) = it.next() {
                return Some(UnsizedHashtableEntryRef(
                    UnsizedHashtableEntryRefInner::Table4(e),
                ));
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
    type Item = UnsizedHashtableEntryMutRef<'a, K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(it) = self.it_0.as_mut() {
            if let Some(e) = it.next() {
                return Some(UnsizedHashtableEntryMutRef(
                    UnsizedHashtableEntryMutRefInner::Table0(e, PhantomData),
                ));
            }
            self.it_0 = None;
        }
        if let Some(it) = self.it_1.as_mut() {
            if let Some(e) = it.next() {
                return Some(UnsizedHashtableEntryMutRef(
                    UnsizedHashtableEntryMutRefInner::Table1(e),
                ));
            }
            self.it_1 = None;
        }
        if let Some(it) = self.it_2.as_mut() {
            if let Some(e) = it.next() {
                return Some(UnsizedHashtableEntryMutRef(
                    UnsizedHashtableEntryMutRefInner::Table2(e),
                ));
            }
            self.it_2 = None;
        }
        if let Some(it) = self.it_3.as_mut() {
            if let Some(e) = it.next() {
                return Some(UnsizedHashtableEntryMutRef(
                    UnsizedHashtableEntryMutRefInner::Table3(e),
                ));
            }
            self.it_3 = None;
        }
        if let Some(it) = self.it_4.as_mut() {
            if let Some(e) = it.next() {
                return Some(UnsizedHashtableEntryMutRef(
                    UnsizedHashtableEntryMutRefInner::Table4(e),
                ));
            }
            self.it_4 = None;
        }
        None
    }
}

enum UnsizedHashtableEntryRefInner<'a, K: ?Sized, V> {
    Table0(&'a Entry<[u8; 2], V>, PhantomData<K>),
    Table1(&'a Entry<InlineKey<0>, V>),
    Table2(&'a Entry<InlineKey<1>, V>),
    Table3(&'a Entry<InlineKey<2>, V>),
    Table4(&'a Entry<FallbackKey, V>),
}

impl<'a, K: ?Sized, V> Copy for UnsizedHashtableEntryRefInner<'a, K, V> {}

impl<'a, K: ?Sized, V> Clone for UnsizedHashtableEntryRefInner<'a, K, V> {
    fn clone(&self) -> Self {
        use UnsizedHashtableEntryRefInner::*;
        match self {
            Table0(a, b) => Table0(a, *b),
            Table1(a) => Table1(a),
            Table2(a) => Table2(a),
            Table3(a) => Table3(a),
            Table4(a) => Table4(a),
        }
    }
}

impl<'a, K: ?Sized + UnsizedKeyable, V> UnsizedHashtableEntryRefInner<'a, K, V> {
    fn key(self) -> &'a K {
        use UnsizedHashtableEntryRefInner::*;
        match self {
            Table0(e, _) => unsafe {
                let key = e.key.assume_init_ref();
                if key[1] != 0 {
                    UnsizedKeyable::from_bytes(&key[..2])
                } else if key[0] != 0 {
                    UnsizedKeyable::from_bytes(&key[..1])
                } else {
                    UnsizedKeyable::from_bytes(&key[..0])
                }
            },
            Table1(e) => unsafe {
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
            },
            Table2(e) => unsafe {
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
            },
            Table3(e) => unsafe {
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
            },
            Table4(e) => unsafe {
                UnsizedKeyable::from_bytes(e.key.assume_init().key.unwrap().as_ref())
            },
        }
    }
    fn get(self) -> &'a V {
        use UnsizedHashtableEntryRefInner::*;
        match self {
            Table0(e, _) => e.get(),
            Table1(e) => e.get(),
            Table2(e) => e.get(),
            Table3(e) => e.get(),
            Table4(e) => e.get(),
        }
    }
    fn get_ptr(self) -> *const V {
        use UnsizedHashtableEntryRefInner::*;
        match self {
            Table0(e, _) => e.val.as_ptr(),
            Table1(e) => e.val.as_ptr(),
            Table2(e) => e.val.as_ptr(),
            Table3(e) => e.val.as_ptr(),
            Table4(e) => e.val.as_ptr(),
        }
    }
}

pub struct UnsizedHashtableEntryRef<'a, K: ?Sized, V>(UnsizedHashtableEntryRefInner<'a, K, V>);

impl<'a, K: ?Sized, V> Copy for UnsizedHashtableEntryRef<'a, K, V> {}

impl<'a, K: ?Sized, V> Clone for UnsizedHashtableEntryRef<'a, K, V> {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

impl<'a, K: ?Sized + UnsizedKeyable, V> UnsizedHashtableEntryRef<'a, K, V> {
    pub fn key(self) -> &'a K {
        self.0.key()
    }
    pub fn get(self) -> &'a V {
        self.0.get()
    }
    pub fn get_ptr(self) -> *const V {
        self.0.get_ptr()
    }
}

enum UnsizedHashtableEntryMutRefInner<'a, K: ?Sized, V> {
    Table0(&'a mut Entry<[u8; 2], V>, PhantomData<K>),
    Table1(&'a mut Entry<InlineKey<0>, V>),
    Table2(&'a mut Entry<InlineKey<1>, V>),
    Table3(&'a mut Entry<InlineKey<2>, V>),
    Table4(&'a mut Entry<FallbackKey, V>),
}

impl<'a, K: ?Sized + UnsizedKeyable, V> UnsizedHashtableEntryMutRefInner<'a, K, V> {
    fn key(&self) -> &'a K {
        use UnsizedHashtableEntryMutRefInner::*;
        match self {
            Table0(e, _) => unsafe {
                let key = e.key.assume_init_ref();
                if key[1] != 0 {
                    &*(UnsizedKeyable::from_bytes(&key[..2]) as *const K)
                } else if key[0] != 0 {
                    &*(UnsizedKeyable::from_bytes(&key[..1]) as *const K)
                } else {
                    &*(UnsizedKeyable::from_bytes(&key[..0]) as *const K)
                }
            },
            Table1(e) => unsafe {
                let bytes = e.key().1.get().to_le_bytes();
                for i in (0..=7).rev() {
                    if bytes[i] != 0 {
                        return UnsizedKeyable::from_bytes(std::slice::from_raw_parts(
                            e.key.assume_init_ref() as *const _ as *const u8,
                            i + 1,
                        ));
                    }
                }
                unreachable!()
            },
            Table2(e) => unsafe {
                let bytes = e.key().1.get().to_le_bytes();
                for i in (0..=7).rev() {
                    if bytes[i] != 0 {
                        return UnsizedKeyable::from_bytes(std::slice::from_raw_parts(
                            e.key.assume_init_ref() as *const _ as *const u8,
                            i + 9,
                        ));
                    }
                }
                unreachable!()
            },
            Table3(e) => unsafe {
                let bytes = e.key().1.get().to_le_bytes();
                for i in (0..=7).rev() {
                    if bytes[i] != 0 {
                        return UnsizedKeyable::from_bytes(std::slice::from_raw_parts(
                            e.key.assume_init_ref() as *const _ as *const u8,
                            i + 17,
                        ));
                    }
                }
                unreachable!()
            },
            Table4(e) => unsafe {
                UnsizedKeyable::from_bytes(e.key.assume_init().key.unwrap().as_ref())
            },
        }
    }
    fn get(&self) -> &V {
        use UnsizedHashtableEntryMutRefInner::*;
        match self {
            Table0(e, _) => e.get(),
            Table1(e) => e.get(),
            Table2(e) => e.get(),
            Table3(e) => e.get(),
            Table4(e) => e.get(),
        }
    }
    fn get_ptr(&self) -> *const V {
        use UnsizedHashtableEntryMutRefInner::*;
        match self {
            Table0(e, _) => e.val.as_ptr(),
            Table1(e) => e.val.as_ptr(),
            Table2(e) => e.val.as_ptr(),
            Table3(e) => e.val.as_ptr(),
            Table4(e) => e.val.as_ptr(),
        }
    }
    fn get_mut(&mut self) -> &mut V {
        use UnsizedHashtableEntryMutRefInner::*;
        match self {
            Table0(e, _) => e.get_mut(),
            Table1(e) => e.get_mut(),
            Table2(e) => e.get_mut(),
            Table3(e) => e.get_mut(),
            Table4(e) => e.get_mut(),
        }
    }
    fn write(&mut self, val: V) {
        use UnsizedHashtableEntryMutRefInner::*;
        match self {
            Table0(e, _) => e.write(val),
            Table1(e) => e.write(val),
            Table2(e) => e.write(val),
            Table3(e) => e.write(val),
            Table4(e) => e.write(val),
        }
    }
}

pub struct UnsizedHashtableEntryMutRef<'a, K: ?Sized, V>(
    UnsizedHashtableEntryMutRefInner<'a, K, V>,
);

impl<'a, K: ?Sized + UnsizedKeyable, V> UnsizedHashtableEntryMutRef<'a, K, V> {
    pub fn key(&self) -> &'a K {
        self.0.key()
    }
    pub fn get(&self) -> &V {
        self.0.get()
    }
    pub fn get_ptr(&self) -> *const V {
        self.0.get_ptr()
    }
    pub fn get_mut_ptr(&self) -> *mut V {
        self.get_ptr() as *mut V
    }
    pub fn get_mut(&mut self) -> &mut V {
        self.0.get_mut()
    }
    pub fn write(&mut self, val: V) {
        self.0.write(val)
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

impl<'a, K: UnsizedKeyable + ?Sized + 'a, V: 'a> EntryRefLike
    for UnsizedHashtableEntryRef<'a, K, V>
{
    type KeyRef = &'a K;
    type ValueRef = &'a V;

    fn key(&self) -> Self::KeyRef {
        (*self).key()
    }
    fn get(&self) -> Self::ValueRef {
        (*self).get()
    }
}

impl<'a, K: UnsizedKeyable + ?Sized + 'a, V: 'a> EntryMutRefLike
    for UnsizedHashtableEntryMutRef<'a, K, V>
{
    type KeyRef = &'a K;
    type Value = V;

    fn key(&self) -> Self::KeyRef {
        self.key()
    }
    fn get(&self) -> &Self::Value {
        self.get()
    }
    fn get_mut(&mut self) -> &mut Self::Value {
        self.get_mut()
    }
    fn write(&mut self, value: Self::Value) {
        self.write(value);
    }
}

impl<V, A> HashtableLike for UnsizedHashtable<[u8], V, A>
where A: Allocator + Clone + Default
{
    type Key = [u8];
    type KeyRef<'a> = &'a [u8] where Self::Key:'a;
    type Value = V;

    type EntryRef<'a> = UnsizedHashtableEntryRef<'a, [u8], V> where Self:'a, V: 'a;
    type EntryMutRef<'a> = UnsizedHashtableEntryMutRef<'a, [u8], V> where Self:'a, V: 'a;

    type Iterator<'a> = UnsizedHashtableIter<'a, [u8], V> where Self:'a, V: 'a;
    type IteratorMut<'a> = UnsizedHashtableIterMut<'a, [u8], V> where Self:'a, V: 'a;

    fn entry<'a>(&self, key_ref: Self::KeyRef<'a>) -> Option<Self::EntryRef<'_>>
    where Self::Key: 'a {
        self.entry(key_ref)
    }
    fn entry_mut<'a>(&mut self, key_ref: Self::KeyRef<'a>) -> Option<Self::EntryMutRef<'_>>
    where Self::Key: 'a {
        self.entry_mut(key_ref)
    }

    fn get<'a>(&self, key_ref: Self::KeyRef<'a>) -> Option<&Self::Value>
    where Self::Key: 'a {
        self.get(key_ref)
    }
    fn get_mut<'a>(&mut self, key_ref: Self::KeyRef<'a>) -> Option<&mut Self::Value>
    where Self::Key: 'a {
        self.get_mut(key_ref)
    }

    unsafe fn insert<'a>(
        &mut self,
        key_ref: Self::KeyRef<'a>,
    ) -> Result<&mut MaybeUninit<Self::Value>, &mut Self::Value>
    where
        Self::Key: 'a,
    {
        self.insert(key_ref)
    }
    unsafe fn insert_and_entry<'a>(
        &mut self,
        key_ref: Self::KeyRef<'a>,
    ) -> Result<Self::EntryMutRef<'_>, Self::EntryMutRef<'_>>
    where
        Self::Key: 'a,
    {
        self.insert_and_entry(key_ref)
    }

    fn iter(&self) -> Self::Iterator<'_> {
        self.iter()
    }
    fn iter_mut(&mut self) -> Self::IteratorMut<'_> {
        self.iter_mut()
    }
}
