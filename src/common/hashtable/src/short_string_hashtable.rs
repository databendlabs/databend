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
use std::iter::TrustedLen;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::num::NonZeroU64;
use std::ptr::NonNull;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_base::mem_allocator::MmapAllocator;

use super::container::HeapContainer;
use super::table0::Entry;
use super::table0::Table0;
use super::traits::EntryMutRefLike;
use super::traits::EntryRefLike;
use super::traits::FastHash;
use super::traits::HashtableLike;
use super::traits::Keyable;
use super::traits::UnsizedKeyable;
use super::utils::read_le;
use crate::table0::Table0Iter;
use crate::table0::Table0IterMut;
use crate::table_empty::TableEmpty;
use crate::table_empty::TableEmptyIter;
use crate::table_empty::TableEmptyIterMut;

pub struct ShortStringHashtable<K, V, A = MmapAllocator>
where
    K: UnsizedKeyable + ?Sized,
    A: Allocator + Clone,
{
    pub(crate) arena: Arc<Bump>,
    pub(crate) key_size: usize,
    pub(crate) table0: TableEmpty<V, A>,
    pub(crate) table1: Table0<InlineKey<0>, V, HeapContainer<Entry<InlineKey<0>, V>, A>, A>,
    pub(crate) table2: Table0<InlineKey<1>, V, HeapContainer<Entry<InlineKey<1>, V>, A>, A>,
    pub(crate) table3: Table0<InlineKey<2>, V, HeapContainer<Entry<InlineKey<2>, V>, A>, A>,
    pub(crate) table4: Table0<FallbackKey, V, HeapContainer<Entry<FallbackKey, V>, A>, A>,
    pub(crate) _phantom: PhantomData<K>,
}

unsafe impl<K: UnsizedKeyable + ?Sized + Send, V: Send, A: Allocator + Clone + Send> Send
    for ShortStringHashtable<K, V, A>
{
}

unsafe impl<K: UnsizedKeyable + ?Sized + Sync, V: Sync, A: Allocator + Clone + Sync> Sync
    for ShortStringHashtable<K, V, A>
{
}

impl<K, V, A> ShortStringHashtable<K, V, A>
where
    K: UnsizedKeyable + ?Sized,
    A: Allocator + Clone + Default,
{
    pub fn new(arena: Arc<Bump>) -> Self {
        Self::with_capacity(128, arena)
    }
}

impl<K, A> ShortStringHashtable<K, (), A>
where
    K: UnsizedKeyable + ?Sized,
    A: Allocator + Clone + Default,
{
    #[inline(always)]
    pub fn set_insert(&mut self, key: &K) -> Result<&mut MaybeUninit<()>, &mut ()> {
        unsafe { self.insert_borrowing(key) }
    }

    #[inline(always)]
    pub fn set_merge(&mut self, other: &Self) {
        unsafe {
            for _ in other.table0.iter() {
                let _ = self.table0.insert();
            }
            self.table1.set_merge(&other.table1);
            self.table2.set_merge(&other.table2);
            self.table3.set_merge(&other.table3);
            self.table4.set_merge(&other.table4);
        }
    }
}

impl<K, V, A> ShortStringHashtable<K, V, A>
where
    K: UnsizedKeyable + ?Sized,
    A: Allocator + Clone + Default,
{
    /// The bump for strings doesn't allocate memory by `A`.
    pub fn with_capacity(capacity: usize, arena: Arc<Bump>) -> Self {
        let allocator = A::default();
        Self {
            arena,
            key_size: 0,
            table0: TableEmpty::new_in(allocator.clone()),
            table1: Table0::with_capacity_in(capacity, allocator.clone()),
            table2: Table0::with_capacity_in(capacity, allocator.clone()),
            table3: Table0::with_capacity_in(capacity, allocator.clone()),
            table4: Table0::with_capacity_in(capacity, allocator),
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

    /// # Safety
    ///
    /// * The uninitialized value of returned entry should be written immediately.
    /// * The lifetime of key lives longer than the hashtable.
    #[inline(always)]
    pub unsafe fn insert_and_entry_borrowing(
        &mut self,
        key: *const K,
    ) -> Result<ShortStringHashtableEntryMutRef<'_, K, V>, ShortStringHashtableEntryMutRef<'_, K, V>>
    {
        let key = (*key).as_bytes();
        match key.len() {
            _ if key.last().copied() == Some(0) => {
                self.table4.check_grow();
                self.table4
                    .insert(FallbackKey::new(key))
                    .map(|x| {
                        self.key_size += key.len();
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table4(x),
                        )
                    })
                    .map_err(|x| {
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table4(x),
                        )
                    })
            }
            0 => {
                self.table0
                    .insert()
                    .map(|x| {
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table0(x, PhantomData),
                        )
                    })
                    .map_err(|x| {
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table0(x, PhantomData),
                        )
                    })
            }
            1..=8 => {
                self.table1.check_grow();
                let mut t = [0u64; 1];
                t[0] = read_le(key.as_ptr(), key.len());
                let t = std::mem::transmute::<[u64; 1], InlineKey<0>>(t);
                self.table1
                    .insert(t)
                    .map(|x| {
                        self.key_size += key.len();
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table1(x),
                        )
                    })
                    .map_err(|x| {
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table1(x),
                        )
                    })
            }
            9..=16 => {
                self.table2.check_grow();
                let mut t = [0u64; 2];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = read_le(key.as_ptr().offset(8), key.len() - 8);
                let t = std::mem::transmute::<[u64; 2], InlineKey<1>>(t);
                self.table2
                    .insert(t)
                    .map(|x| {
                        self.key_size += key.len();
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table2(x),
                        )
                    })
                    .map_err(|x| {
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table2(x),
                        )
                    })
            }
            17..=24 => {
                self.table3.check_grow();
                let mut t = [0u64; 3];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = (key.as_ptr() as *const u64).offset(1).read_unaligned();
                t[2] = read_le(key.as_ptr().offset(16), key.len() - 16);
                let t = std::mem::transmute::<[u64; 3], InlineKey<2>>(t);
                self.table3
                    .insert(t)
                    .map(|x| {
                        self.key_size += key.len();
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table3(x),
                        )
                    })
                    .map_err(|x| {
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table3(x),
                        )
                    })
            }
            _ => {
                self.table4.check_grow();
                self.table4
                    .insert(FallbackKey::new(key))
                    .map(|x| {
                        self.key_size += key.len();
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table4(x),
                        )
                    })
                    .map_err(|x| {
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table4(x),
                        )
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
}

pub struct ShortStringHashtableIter<'a, K, V>
where K: UnsizedKeyable + ?Sized
{
    it_0: Option<TableEmptyIter<'a, V>>,
    it_1: Option<Table0Iter<'a, InlineKey<0>, V>>,
    it_2: Option<Table0Iter<'a, InlineKey<1>, V>>,
    it_3: Option<Table0Iter<'a, InlineKey<2>, V>>,
    it_4: Option<Table0Iter<'a, FallbackKey, V>>,
    _phantom: PhantomData<&'a mut K>,
}

impl<'a, K, V> Iterator for ShortStringHashtableIter<'a, K, V>
where K: UnsizedKeyable + ?Sized
{
    type Item = ShortStringHashtableEntryRef<'a, K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(it) = self.it_0.as_mut() {
            if let Some(e) = it.next() {
                return Some(ShortStringHashtableEntryRef(
                    ShortStringHashtableEntryRefInner::Table0(e, PhantomData),
                ));
            }
            self.it_0 = None;
        }
        if let Some(it) = self.it_1.as_mut() {
            if let Some(e) = it.next() {
                return Some(ShortStringHashtableEntryRef(
                    ShortStringHashtableEntryRefInner::Table1(e),
                ));
            }
            self.it_1 = None;
        }
        if let Some(it) = self.it_2.as_mut() {
            if let Some(e) = it.next() {
                return Some(ShortStringHashtableEntryRef(
                    ShortStringHashtableEntryRefInner::Table2(e),
                ));
            }
            self.it_2 = None;
        }
        if let Some(it) = self.it_3.as_mut() {
            if let Some(e) = it.next() {
                return Some(ShortStringHashtableEntryRef(
                    ShortStringHashtableEntryRefInner::Table3(e),
                ));
            }
            self.it_3 = None;
        }
        if let Some(it) = self.it_4.as_mut() {
            if let Some(e) = it.next() {
                return Some(ShortStringHashtableEntryRef(
                    ShortStringHashtableEntryRefInner::Table4(e),
                ));
            }
            self.it_4 = None;
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let mut size = 0;
        if let Some(it) = self.it_0.as_ref() {
            size += it.size_hint().0;
        }
        if let Some(it) = self.it_1.as_ref() {
            size += it.size_hint().0;
        }
        if let Some(it) = self.it_2.as_ref() {
            size += it.size_hint().0;
        }
        if let Some(it) = self.it_3.as_ref() {
            size += it.size_hint().0;
        }
        if let Some(it) = self.it_4.as_ref() {
            size += it.size_hint().0;
        }
        (size, Some(size))
    }
}

unsafe impl<'a, K, V> TrustedLen for ShortStringHashtableIter<'a, K, V> where K: UnsizedKeyable + ?Sized
{}

pub struct ShortStringHashtableIterMut<'a, K, V>
where K: UnsizedKeyable + ?Sized
{
    it_0: Option<TableEmptyIterMut<'a, V>>,
    it_1: Option<Table0IterMut<'a, InlineKey<0>, V>>,
    it_2: Option<Table0IterMut<'a, InlineKey<1>, V>>,
    it_3: Option<Table0IterMut<'a, InlineKey<2>, V>>,
    it_4: Option<Table0IterMut<'a, FallbackKey, V>>,
    _phantom: PhantomData<&'a mut K>,
}

impl<'a, K, V> Iterator for ShortStringHashtableIterMut<'a, K, V>
where K: UnsizedKeyable + ?Sized
{
    type Item = ShortStringHashtableEntryMutRef<'a, K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(it) = self.it_0.as_mut() {
            if let Some(e) = it.next() {
                return Some(ShortStringHashtableEntryMutRef(
                    ShortStringHashtableEntryMutRefInner::Table0(e, PhantomData),
                ));
            }
            self.it_0 = None;
        }
        if let Some(it) = self.it_1.as_mut() {
            if let Some(e) = it.next() {
                return Some(ShortStringHashtableEntryMutRef(
                    ShortStringHashtableEntryMutRefInner::Table1(e),
                ));
            }
            self.it_1 = None;
        }
        if let Some(it) = self.it_2.as_mut() {
            if let Some(e) = it.next() {
                return Some(ShortStringHashtableEntryMutRef(
                    ShortStringHashtableEntryMutRefInner::Table2(e),
                ));
            }
            self.it_2 = None;
        }
        if let Some(it) = self.it_3.as_mut() {
            if let Some(e) = it.next() {
                return Some(ShortStringHashtableEntryMutRef(
                    ShortStringHashtableEntryMutRefInner::Table3(e),
                ));
            }
            self.it_3 = None;
        }
        if let Some(it) = self.it_4.as_mut() {
            if let Some(e) = it.next() {
                return Some(ShortStringHashtableEntryMutRef(
                    ShortStringHashtableEntryMutRefInner::Table4(e),
                ));
            }
            self.it_4 = None;
        }
        None
    }
}

enum ShortStringHashtableEntryRefInner<'a, K: ?Sized, V> {
    Table0(&'a Entry<[u8; 0], V>, PhantomData<K>),
    Table1(&'a Entry<InlineKey<0>, V>),
    Table2(&'a Entry<InlineKey<1>, V>),
    Table3(&'a Entry<InlineKey<2>, V>),
    Table4(&'a Entry<FallbackKey, V>),
}

impl<'a, K: ?Sized, V> Copy for ShortStringHashtableEntryRefInner<'a, K, V> {}

impl<'a, K: ?Sized, V> Clone for ShortStringHashtableEntryRefInner<'a, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, K: ?Sized + UnsizedKeyable, V> ShortStringHashtableEntryRefInner<'a, K, V> {
    fn key(self) -> &'a K {
        use ShortStringHashtableEntryRefInner::*;
        match self {
            Table0(_, _) => unsafe { UnsizedKeyable::from_bytes(&[]) },
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
        use ShortStringHashtableEntryRefInner::*;
        match self {
            Table0(e, _) => e.get(),
            Table1(e) => e.get(),
            Table2(e) => e.get(),
            Table3(e) => e.get(),
            Table4(e) => e.get(),
        }
    }
    fn get_ptr(self) -> *const V {
        use ShortStringHashtableEntryRefInner::*;
        match self {
            Table0(e, _) => e.val.as_ptr(),
            Table1(e) => e.val.as_ptr(),
            Table2(e) => e.val.as_ptr(),
            Table3(e) => e.val.as_ptr(),
            Table4(e) => e.val.as_ptr(),
        }
    }
}

pub struct ShortStringHashtableEntryRef<'a, K: ?Sized, V>(
    ShortStringHashtableEntryRefInner<'a, K, V>,
);

impl<'a, K: ?Sized, V> Copy for ShortStringHashtableEntryRef<'a, K, V> {}

impl<'a, K: ?Sized, V> Clone for ShortStringHashtableEntryRef<'a, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, K: ?Sized + UnsizedKeyable, V> ShortStringHashtableEntryRef<'a, K, V> {
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

enum ShortStringHashtableEntryMutRefInner<'a, K: ?Sized, V> {
    Table0(&'a mut Entry<[u8; 0], V>, PhantomData<K>),
    Table1(&'a mut Entry<InlineKey<0>, V>),
    Table2(&'a mut Entry<InlineKey<1>, V>),
    Table3(&'a mut Entry<InlineKey<2>, V>),
    Table4(&'a mut Entry<FallbackKey, V>),
}

impl<'a, K: ?Sized + UnsizedKeyable, V> ShortStringHashtableEntryMutRefInner<'a, K, V> {
    fn key(&self) -> &'a K {
        use ShortStringHashtableEntryMutRefInner::*;
        match self {
            Table0(_, _) => unsafe { &*(UnsizedKeyable::from_bytes(&[]) as *const K) },
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
        use ShortStringHashtableEntryMutRefInner::*;
        match self {
            Table0(e, _) => e.get(),
            Table1(e) => e.get(),
            Table2(e) => e.get(),
            Table3(e) => e.get(),
            Table4(e) => e.get(),
        }
    }
    fn get_ptr(&self) -> *const V {
        use ShortStringHashtableEntryMutRefInner::*;
        match self {
            Table0(e, _) => e.val.as_ptr(),
            Table1(e) => e.val.as_ptr(),
            Table2(e) => e.val.as_ptr(),
            Table3(e) => e.val.as_ptr(),
            Table4(e) => e.val.as_ptr(),
        }
    }
    fn get_mut(&mut self) -> &mut V {
        use ShortStringHashtableEntryMutRefInner::*;
        match self {
            Table0(e, _) => e.get_mut(),
            Table1(e) => e.get_mut(),
            Table2(e) => e.get_mut(),
            Table3(e) => e.get_mut(),
            Table4(e) => e.get_mut(),
        }
    }
    fn write(&mut self, val: V) {
        use ShortStringHashtableEntryMutRefInner::*;
        match self {
            Table0(e, _) => e.write(val),
            Table1(e) => e.write(val),
            Table2(e) => e.write(val),
            Table3(e) => e.write(val),
            Table4(e) => e.write(val),
        }
    }
}

pub struct ShortStringHashtableEntryMutRef<'a, K: ?Sized, V>(
    ShortStringHashtableEntryMutRefInner<'a, K, V>,
);

impl<'a, K: ?Sized + UnsizedKeyable, V> ShortStringHashtableEntryMutRef<'a, K, V> {
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
    pub(crate) key: Option<NonNull<[u8]>>,
    pub(crate) hash: u64,
}

impl FallbackKey {
    pub(crate) unsafe fn new(key: &[u8]) -> Self {
        Self {
            key: Some(NonNull::from(key)),
            hash: key.fast_hash(),
        }
    }

    pub(crate) unsafe fn new_with_hash(key: &[u8], hash: u64) -> Self {
        Self {
            hash,
            key: Some(NonNull::from(key)),
        }
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "sse4.2"))]
impl PartialEq for FallbackKey {
    fn eq(&self, other: &Self) -> bool {
        if self.hash == other.hash {
            match (self.key, other.key) {
                (Some(a), Some(b)) => unsafe {
                    crate::utils::sse::memcmp_sse(a.as_ref(), b.as_ref())
                },
                (None, None) => true,
                _ => false,
            }
        } else {
            false
        }
    }
}

#[cfg(not(all(any(target_arch = "x86_64"), target_feature = "sse4.2")))]
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
    for ShortStringHashtableEntryRef<'a, K, V>
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
    for ShortStringHashtableEntryMutRef<'a, K, V>
{
    type Key = K;
    type Value = V;

    fn key(&self) -> &Self::Key {
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

impl<V, A> HashtableLike for ShortStringHashtable<[u8], V, A>
where A: Allocator + Clone + Default
{
    type Key = [u8];
    type Value = V;

    type EntryRef<'a> = ShortStringHashtableEntryRef<'a, [u8], V> where Self: 'a, V: 'a;
    type EntryMutRef<'a> = ShortStringHashtableEntryMutRef<'a, [u8], V> where Self: 'a, V: 'a;

    type Iterator<'a> = ShortStringHashtableIter<'a, [u8], V> where Self: 'a, V: 'a;
    type IteratorMut<'a> = ShortStringHashtableIterMut<'a, [u8], V> where Self: 'a, V: 'a;

    fn len(&self) -> usize {
        self.len()
    }

    fn bytes_len(&self, without_arena: bool) -> usize {
        match without_arena {
            true => {
                std::mem::size_of::<Self>()
                    + self.table0.heap_bytes()
                    + self.table1.heap_bytes()
                    + self.table2.heap_bytes()
                    + self.table3.heap_bytes()
                    + self.table4.heap_bytes()
            }
            false => {
                std::mem::size_of::<Self>()
                    + self.arena.allocated_bytes()
                    + self.table0.heap_bytes()
                    + self.table1.heap_bytes()
                    + self.table2.heap_bytes()
                    + self.table3.heap_bytes()
                    + self.table4.heap_bytes()
            }
        }
    }

    fn unsize_key_size(&self) -> Option<usize> {
        Some(self.key_size)
    }

    fn entry(&self, key: &Self::Key) -> Option<Self::EntryRef<'_>> {
        let key = key.as_bytes();
        match key.len() {
            _ if key.last().copied() == Some(0) => unsafe {
                self.table4.get(&FallbackKey::new(key)).map(|x| {
                    ShortStringHashtableEntryRef(ShortStringHashtableEntryRefInner::Table4(x))
                })
            },
            0 => self.table0.get().map(|x| {
                ShortStringHashtableEntryRef(ShortStringHashtableEntryRefInner::Table0(
                    x,
                    PhantomData,
                ))
            }),
            1..=8 => unsafe {
                let mut t = [0u64; 1];
                t[0] = read_le(key.as_ptr(), key.len());
                let t = std::mem::transmute::<[u64; 1], InlineKey<0>>(t);
                self.table1.get(&t).map(|x| {
                    ShortStringHashtableEntryRef(ShortStringHashtableEntryRefInner::Table1(x))
                })
            },
            9..=16 => unsafe {
                let mut t = [0u64; 2];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = read_le(key.as_ptr().offset(8), key.len() - 8);
                let t = std::mem::transmute::<[u64; 2], InlineKey<1>>(t);
                self.table2.get(&t).map(|x| {
                    ShortStringHashtableEntryRef(ShortStringHashtableEntryRefInner::Table2(x))
                })
            },
            17..=24 => unsafe {
                let mut t = [0u64; 3];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = (key.as_ptr() as *const u64).offset(1).read_unaligned();
                t[2] = read_le(key.as_ptr().offset(16), key.len() - 16);
                let t = std::mem::transmute::<[u64; 3], InlineKey<2>>(t);
                self.table3.get(&t).map(|x| {
                    ShortStringHashtableEntryRef(ShortStringHashtableEntryRefInner::Table3(x))
                })
            },
            _ => unsafe {
                self.table4.get(&FallbackKey::new(key)).map(|x| {
                    ShortStringHashtableEntryRef(ShortStringHashtableEntryRefInner::Table4(x))
                })
            },
        }
    }

    fn entry_mut(&mut self, key: &[u8]) -> Option<Self::EntryMutRef<'_>> {
        match key.len() {
            _ if key.last().copied() == Some(0) => unsafe {
                self.table4.get_mut(&FallbackKey::new(key)).map(|x| {
                    ShortStringHashtableEntryMutRef(ShortStringHashtableEntryMutRefInner::Table4(x))
                })
            },
            0 => self.table0.get_mut().map(|x| {
                ShortStringHashtableEntryMutRef(ShortStringHashtableEntryMutRefInner::Table0(
                    x,
                    PhantomData,
                ))
            }),
            1..=8 => unsafe {
                let mut t = [0u64; 1];
                t[0] = read_le(key.as_ptr(), key.len());
                let t = std::mem::transmute::<[u64; 1], InlineKey<0>>(t);
                self.table1.get_mut(&t).map(|x| {
                    ShortStringHashtableEntryMutRef(ShortStringHashtableEntryMutRefInner::Table1(x))
                })
            },
            9..=16 => unsafe {
                let mut t = [0u64; 2];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = read_le(key.as_ptr().offset(8), key.len() - 8);
                let t = std::mem::transmute::<[u64; 2], InlineKey<1>>(t);
                self.table2.get_mut(&t).map(|x| {
                    ShortStringHashtableEntryMutRef(ShortStringHashtableEntryMutRefInner::Table2(x))
                })
            },
            17..=24 => unsafe {
                let mut t = [0u64; 3];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = (key.as_ptr() as *const u64).offset(1).read_unaligned();
                t[2] = read_le(key.as_ptr().offset(16), key.len() - 16);
                let t = std::mem::transmute::<[u64; 3], InlineKey<2>>(t);
                self.table3.get_mut(&t).map(|x| {
                    ShortStringHashtableEntryMutRef(ShortStringHashtableEntryMutRefInner::Table3(x))
                })
            },
            _ => unsafe {
                self.table4.get_mut(&FallbackKey::new(key)).map(|x| {
                    ShortStringHashtableEntryMutRef(ShortStringHashtableEntryMutRefInner::Table4(x))
                })
            },
        }
    }

    fn get(&self, key: &Self::Key) -> Option<&Self::Value> {
        self.entry(key).map(|e| e.get())
    }

    fn get_mut(&mut self, key: &Self::Key) -> Option<&mut Self::Value> {
        self.entry_mut(key)
            .map(|e| unsafe { &mut *(e.get_mut_ptr()) })
    }

    unsafe fn insert(
        &mut self,
        key: &Self::Key,
    ) -> Result<&mut MaybeUninit<Self::Value>, &mut Self::Value> {
        match self.insert_and_entry(key) {
            Ok(e) => Ok(&mut *(e.get_mut_ptr() as *mut MaybeUninit<V>)),
            Err(e) => Err(&mut *e.get_mut_ptr()),
        }
    }

    #[inline(always)]
    unsafe fn insert_and_entry(
        &mut self,
        key: &Self::Key,
    ) -> Result<Self::EntryMutRef<'_>, Self::EntryMutRef<'_>> {
        let key = key.as_bytes();
        match key.len() {
            _ if key.last().copied() == Some(0) => {
                self.table4.check_grow();
                match self.table4.insert(FallbackKey::new(key)) {
                    Ok(e) => {
                        // We need to save the key to avoid drop it.
                        let s = self.arena.alloc_slice_copy(key);
                        e.set_key(FallbackKey::new_with_hash(s, e.key.assume_init_ref().hash));

                        self.key_size += key.len();
                        Ok(ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table4(e),
                        ))
                    }
                    Err(e) => Err(ShortStringHashtableEntryMutRef(
                        ShortStringHashtableEntryMutRefInner::Table4(e),
                    )),
                }
            }
            0 => {
                self.table0
                    .insert()
                    .map(|x| {
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table0(x, PhantomData),
                        )
                    })
                    .map_err(|x| {
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table0(x, PhantomData),
                        )
                    })
            }

            1..=8 => {
                self.table1.check_grow();
                let mut t = [0u64; 1];
                t[0] = read_le(key.as_ptr(), key.len());
                let t = std::mem::transmute::<[u64; 1], InlineKey<0>>(t);
                self.table1
                    .insert(t)
                    .map(|x| {
                        self.key_size += key.len();
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table1(x),
                        )
                    })
                    .map_err(|x| {
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table1(x),
                        )
                    })
            }
            9..=16 => {
                self.table2.check_grow();
                let mut t = [0u64; 2];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = read_le(key.as_ptr().offset(8), key.len() - 8);
                let t = std::mem::transmute::<[u64; 2], InlineKey<1>>(t);
                self.table2
                    .insert(t)
                    .map(|x| {
                        self.key_size += key.len();
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table2(x),
                        )
                    })
                    .map_err(|x| {
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table2(x),
                        )
                    })
            }
            17..=24 => {
                self.table3.check_grow();
                let mut t = [0u64; 3];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = (key.as_ptr() as *const u64).offset(1).read_unaligned();
                t[2] = read_le(key.as_ptr().offset(16), key.len() - 16);
                let t = std::mem::transmute::<[u64; 3], InlineKey<2>>(t);
                self.table3
                    .insert(t)
                    .map(|x| {
                        self.key_size += key.len();
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table3(x),
                        )
                    })
                    .map_err(|x| {
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table3(x),
                        )
                    })
            }
            _ => {
                self.table4.check_grow();
                match self.table4.insert(FallbackKey::new(key)) {
                    Ok(e) => {
                        // We need to save the key to avoid drop it.
                        let s = self.arena.alloc_slice_copy(key);
                        e.set_key(FallbackKey::new_with_hash(s, e.key.assume_init_ref().hash));

                        self.key_size += key.len();
                        Ok(ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table4(e),
                        ))
                    }
                    Err(e) => Err(ShortStringHashtableEntryMutRef(
                        ShortStringHashtableEntryMutRefInner::Table4(e),
                    )),
                }
            }
        }
    }

    #[inline(always)]
    unsafe fn insert_and_entry_with_hash(
        &mut self,
        key: &Self::Key,
        hash: u64,
    ) -> Result<Self::EntryMutRef<'_>, Self::EntryMutRef<'_>> {
        let key = key.as_bytes();
        match key.len() {
            _ if key.last().copied() == Some(0) => {
                self.table4.check_grow();
                match self
                    .table4
                    .insert_with_hash(FallbackKey::new_with_hash(key, hash), hash)
                {
                    Ok(e) => {
                        // We need to save the key to avoid drop it.
                        let s = self.arena.alloc_slice_copy(key);
                        e.set_key(FallbackKey::new_with_hash(s, hash));
                        Ok(ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table4(e),
                        ))
                    }
                    Err(e) => Err(ShortStringHashtableEntryMutRef(
                        ShortStringHashtableEntryMutRefInner::Table4(e),
                    )),
                }
            }
            0 => {
                self.table0
                    .insert()
                    .map(|x| {
                        self.key_size += key.len();
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table0(x, PhantomData),
                        )
                    })
                    .map_err(|x| {
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table0(x, PhantomData),
                        )
                    })
            }
            1..=8 => {
                self.table1.check_grow();
                let mut t = [0u64; 1];
                t[0] = read_le(key.as_ptr(), key.len());
                let t = std::mem::transmute::<[u64; 1], InlineKey<0>>(t);
                self.table1
                    .insert_with_hash(t, hash)
                    .map(|x| {
                        self.key_size += key.len();
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table1(x),
                        )
                    })
                    .map_err(|x| {
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table1(x),
                        )
                    })
            }
            9..=16 => {
                self.table2.check_grow();
                let mut t = [0u64; 2];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = read_le(key.as_ptr().offset(8), key.len() - 8);
                let t = std::mem::transmute::<[u64; 2], InlineKey<1>>(t);
                self.table2
                    .insert_with_hash(t, hash)
                    .map(|x| {
                        self.key_size += key.len();
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table2(x),
                        )
                    })
                    .map_err(|x| {
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table2(x),
                        )
                    })
            }
            17..=24 => {
                self.table3.check_grow();
                let mut t = [0u64; 3];
                t[0] = (key.as_ptr() as *const u64).read_unaligned();
                t[1] = (key.as_ptr() as *const u64).offset(1).read_unaligned();
                t[2] = read_le(key.as_ptr().offset(16), key.len() - 16);
                let t = std::mem::transmute::<[u64; 3], InlineKey<2>>(t);
                self.table3
                    .insert_with_hash(t, hash)
                    .map(|x| {
                        self.key_size += key.len();
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table3(x),
                        )
                    })
                    .map_err(|x| {
                        ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table3(x),
                        )
                    })
            }
            _ => {
                self.table4.check_grow();
                match self
                    .table4
                    .insert_with_hash(FallbackKey::new_with_hash(key, hash), hash)
                {
                    Ok(e) => {
                        // We need to save the key to avoid drop it.
                        let s = self.arena.alloc_slice_copy(key);
                        e.set_key(FallbackKey::new_with_hash(s, hash));

                        self.key_size += key.len();
                        Ok(ShortStringHashtableEntryMutRef(
                            ShortStringHashtableEntryMutRefInner::Table4(e),
                        ))
                    }
                    Err(e) => Err(ShortStringHashtableEntryMutRef(
                        ShortStringHashtableEntryMutRefInner::Table4(e),
                    )),
                }
            }
        }
    }

    fn iter(&self) -> Self::Iterator<'_> {
        ShortStringHashtableIter {
            it_0: Some(self.table0.iter()),
            it_1: Some(self.table1.iter()),
            it_2: Some(self.table2.iter()),
            it_3: Some(self.table3.iter()),
            it_4: Some(self.table4.iter()),
            _phantom: PhantomData,
        }
    }

    fn clear(&mut self) {
        self.table0.clear();
        self.table1.clear();
        self.table2.clear();
        self.table3.clear();
        self.table4.clear();
        // NOTE: Bump provides the reset function to free memory, but it will cause memory leakage(maybe a bug).
        // In fact, we don't need to call the drop function. rust will call it, But we call it to improve the readability of the code.
        drop(std::mem::take(&mut self.arena));
    }
}
