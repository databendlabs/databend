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
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_base::mem_allocator::MmapAllocator;

use super::container::HeapContainer;
use super::table0::Entry;
use super::table0::Table0;
use super::traits::EntryMutRefLike;
use super::traits::EntryRefLike;
use super::traits::HashtableLike;
use super::traits::UnsizedKeyable;
use crate::short_string_hashtable::FallbackKey;
use crate::table0::Table0Iter;
use crate::table0::Table0IterMut;
use crate::table_empty::TableEmpty;
use crate::table_empty::TableEmptyIter;
use crate::table_empty::TableEmptyIterMut;

/// Simple unsized hashtable is used for storing unsized keys in arena. It can be worked with HashMethodSerializer.
/// Different from `ShortStringHashTable`, it doesn't use adaptive sub hashtable to store key values via key size.
/// It can be considered as a minimal hashtable implementation of ShortStringHashTable
pub struct StringHashtable<K, V, A = MmapAllocator>
where
    K: UnsizedKeyable + ?Sized,
    A: Allocator + Clone,
{
    pub(crate) arena: Arc<Bump>,
    pub(crate) key_size: usize,
    pub(crate) table_empty: TableEmpty<V, A>,
    pub(crate) table: Table0<FallbackKey, V, HeapContainer<Entry<FallbackKey, V>, A>, A>,
    pub(crate) _phantom: PhantomData<K>,
}

unsafe impl<K: UnsizedKeyable + ?Sized + Send, V: Send, A: Allocator + Clone + Send> Send
    for StringHashtable<K, V, A>
{
}

unsafe impl<K: UnsizedKeyable + ?Sized + Sync, V: Sync, A: Allocator + Clone + Sync> Sync
    for StringHashtable<K, V, A>
{
}

impl<K, V, A> StringHashtable<K, V, A>
where
    K: UnsizedKeyable + ?Sized,
    A: Allocator + Clone + Default,
{
    pub fn new(bump: Arc<Bump>) -> Self {
        Self::with_capacity(128, bump)
    }
}

impl<K, A> StringHashtable<K, (), A>
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
            for _ in other.table_empty.iter() {
                let _ = self.table_empty.insert();
            }
            self.table.set_merge(&other.table);
        }
    }
}

impl<K, V, A> StringHashtable<K, V, A>
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
            table_empty: TableEmpty::new_in(allocator.clone()),
            table: Table0::with_capacity_in(capacity, allocator),
            _phantom: PhantomData,
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.table_empty.len() + self.table.len()
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.table_empty.capacity() + self.table.capacity()
    }

    /// # Safety
    ///
    /// * The uninitialized value of returned entry should be written immediately.
    /// * The lifetime of key lives longer than the hashtable.
    #[inline(always)]
    pub unsafe fn insert_and_entry_borrowing(
        &mut self,
        key: *const K,
    ) -> Result<StringHashtableEntryMutRef<'_, K, V>, StringHashtableEntryMutRef<'_, K, V>> {
        let key = (*key).as_bytes();
        match key.len() {
            0 => self
                .table_empty
                .insert()
                .map(|x| {
                    StringHashtableEntryMutRef(StringHashtableEntryMutRefInner::TableEmpty(
                        x,
                        PhantomData,
                    ))
                })
                .map_err(|x| {
                    StringHashtableEntryMutRef(StringHashtableEntryMutRefInner::TableEmpty(
                        x,
                        PhantomData,
                    ))
                }),
            _ => {
                self.table.check_grow();
                self.table
                    .insert(FallbackKey::new(key))
                    .map(|x| {
                        self.key_size += key.len();
                        StringHashtableEntryMutRef(StringHashtableEntryMutRefInner::Table(x))
                    })
                    .map_err(|x| {
                        StringHashtableEntryMutRef(StringHashtableEntryMutRefInner::Table(x))
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

    pub unsafe fn get_slot_index(&self, key: &K) -> Option<usize> {
        let key = (*key).as_bytes();

        if key.is_empty() {
            return match self.table_empty.has_zero {
                true => Some(0),
                false => None,
            };
        }

        self.table
            .get_slot_index(&FallbackKey::new(key))
            .map(|x| 1 + x)
    }
}

pub struct StringHashtableIter<'a, K, V>
where K: UnsizedKeyable + ?Sized
{
    it_empty: Option<TableEmptyIter<'a, V>>,
    it: Option<Table0Iter<'a, FallbackKey, V>>,
    _phantom: PhantomData<&'a mut K>,
}

impl<'a, K, V> Iterator for StringHashtableIter<'a, K, V>
where K: UnsizedKeyable + ?Sized
{
    type Item = StringHashtableEntryRef<'a, K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(it) = self.it_empty.as_mut() {
            if let Some(e) = it.next() {
                return Some(StringHashtableEntryRef(
                    StringHashtableEntryRefInner::TableEmpty(e, PhantomData),
                ));
            }
            self.it_empty = None;
        }
        if let Some(it) = self.it.as_mut() {
            if let Some(e) = it.next() {
                return Some(StringHashtableEntryRef(
                    StringHashtableEntryRefInner::Table(e),
                ));
            }
            self.it = None;
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (l, u) = self
            .it_empty
            .as_ref()
            .map_or((0, Some(0)), |it| it.size_hint());
        let (l2, u2) = self.it.as_ref().map_or((0, Some(0)), |it| it.size_hint());

        (l + l2, u.and_then(|u| u2.map(|u2| u + u2)))
    }
}

unsafe impl<'a, K, V> TrustedLen for StringHashtableIter<'a, K, V> where K: UnsizedKeyable + ?Sized {}

pub struct StringHashtableIterMut<'a, K, V>
where K: UnsizedKeyable + ?Sized
{
    it_empty: Option<TableEmptyIterMut<'a, V>>,
    it: Option<Table0IterMut<'a, FallbackKey, V>>,
    _phantom: PhantomData<&'a mut K>,
}

impl<'a, K, V> Iterator for StringHashtableIterMut<'a, K, V>
where K: UnsizedKeyable + ?Sized
{
    type Item = StringHashtableEntryMutRef<'a, K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(it) = self.it_empty.as_mut() {
            if let Some(e) = it.next() {
                return Some(StringHashtableEntryMutRef(
                    StringHashtableEntryMutRefInner::TableEmpty(e, PhantomData),
                ));
            }
            self.it_empty = None;
        }

        if let Some(it) = self.it.as_mut() {
            if let Some(e) = it.next() {
                return Some(StringHashtableEntryMutRef(
                    StringHashtableEntryMutRefInner::Table(e),
                ));
            }
            self.it = None;
        }
        None
    }
}

enum StringHashtableEntryRefInner<'a, K: ?Sized, V> {
    TableEmpty(&'a Entry<[u8; 0], V>, PhantomData<K>),
    Table(&'a Entry<FallbackKey, V>),
}

impl<'a, K: ?Sized, V> Copy for StringHashtableEntryRefInner<'a, K, V> {}

impl<'a, K: ?Sized, V> Clone for StringHashtableEntryRefInner<'a, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, K: ?Sized + UnsizedKeyable, V> StringHashtableEntryRefInner<'a, K, V> {
    fn key(self) -> &'a K {
        use StringHashtableEntryRefInner::*;
        match self {
            TableEmpty(_, _) => unsafe { UnsizedKeyable::from_bytes(&[]) },
            Table(e) => unsafe {
                UnsizedKeyable::from_bytes(e.key.assume_init().key.unwrap().as_ref())
            },
        }
    }
    fn get(self) -> &'a V {
        use StringHashtableEntryRefInner::*;
        match self {
            TableEmpty(e, _) => e.get(),
            Table(e) => e.get(),
        }
    }
    fn get_ptr(self) -> *const V {
        use StringHashtableEntryRefInner::*;
        match self {
            TableEmpty(e, _) => e.val.as_ptr(),
            Table(e) => e.val.as_ptr(),
        }
    }
}

pub struct StringHashtableEntryRef<'a, K: ?Sized, V>(StringHashtableEntryRefInner<'a, K, V>);

impl<'a, K: ?Sized, V> Copy for StringHashtableEntryRef<'a, K, V> {}

impl<'a, K: ?Sized, V> Clone for StringHashtableEntryRef<'a, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, K: ?Sized + UnsizedKeyable, V> StringHashtableEntryRef<'a, K, V> {
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

enum StringHashtableEntryMutRefInner<'a, K: ?Sized, V> {
    TableEmpty(&'a mut Entry<[u8; 0], V>, PhantomData<K>),
    Table(&'a mut Entry<FallbackKey, V>),
}

impl<'a, K: ?Sized + UnsizedKeyable, V> StringHashtableEntryMutRefInner<'a, K, V> {
    fn key(&self) -> &'a K {
        use StringHashtableEntryMutRefInner::*;
        match self {
            TableEmpty(_, _) => unsafe { &*(UnsizedKeyable::from_bytes(&[]) as *const K) },
            Table(e) => unsafe {
                UnsizedKeyable::from_bytes(e.key.assume_init().key.unwrap().as_ref())
            },
        }
    }
    fn get(&self) -> &V {
        use StringHashtableEntryMutRefInner::*;
        match self {
            TableEmpty(e, _) => e.get(),
            Table(e) => e.get(),
        }
    }
    fn get_ptr(&self) -> *const V {
        use StringHashtableEntryMutRefInner::*;
        match self {
            TableEmpty(e, _) => e.val.as_ptr(),
            Table(e) => e.val.as_ptr(),
        }
    }
    fn get_mut(&mut self) -> &mut V {
        use StringHashtableEntryMutRefInner::*;
        match self {
            TableEmpty(e, _) => e.get_mut(),
            Table(e) => e.get_mut(),
        }
    }
    fn write(&mut self, val: V) {
        use StringHashtableEntryMutRefInner::*;
        match self {
            TableEmpty(e, _) => e.write(val),
            Table(e) => e.write(val),
        }
    }
}

pub struct StringHashtableEntryMutRef<'a, K: ?Sized, V>(StringHashtableEntryMutRefInner<'a, K, V>);

impl<'a, K: ?Sized + UnsizedKeyable, V> StringHashtableEntryMutRef<'a, K, V> {
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

impl<'a, K: UnsizedKeyable + ?Sized + 'a, V: 'a> EntryRefLike
    for StringHashtableEntryRef<'a, K, V>
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
    for StringHashtableEntryMutRef<'a, K, V>
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

impl<V, A> HashtableLike for StringHashtable<[u8], V, A>
where A: Allocator + Clone + Default
{
    type Key = [u8];
    type Value = V;

    type EntryRef<'a> = StringHashtableEntryRef<'a, [u8], V> where Self: 'a, V: 'a;
    type EntryMutRef<'a> = StringHashtableEntryMutRef<'a, [u8], V> where Self: 'a, V: 'a;

    type Iterator<'a> = StringHashtableIter<'a, [u8], V> where Self: 'a, V: 'a;
    type IteratorMut<'a> = StringHashtableIterMut<'a, [u8], V> where Self: 'a, V: 'a;

    fn len(&self) -> usize {
        self.len()
    }

    fn bytes_len(&self, without_arena: bool) -> usize {
        match without_arena {
            true => {
                std::mem::size_of::<Self>()
                    + self.table_empty.heap_bytes()
                    + self.table.heap_bytes()
            }
            false => {
                std::mem::size_of::<Self>()
                    + self.arena.allocated_bytes()
                    + self.table_empty.heap_bytes()
                    + self.table.heap_bytes()
            }
        }
    }

    fn unsize_key_size(&self) -> Option<usize> {
        Some(self.key_size)
    }

    fn entry(&self, key: &Self::Key) -> Option<Self::EntryRef<'_>> {
        let key = key.as_bytes();
        match key.len() {
            0 => self.table_empty.get().map(|x| {
                StringHashtableEntryRef(StringHashtableEntryRefInner::TableEmpty(x, PhantomData))
            }),
            _ => unsafe {
                self.table
                    .get(&FallbackKey::new(key))
                    .map(|x| StringHashtableEntryRef(StringHashtableEntryRefInner::Table(x)))
            },
        }
    }

    fn entry_mut(&mut self, key: &[u8]) -> Option<Self::EntryMutRef<'_>> {
        match key.len() {
            0 => self.table_empty.get_mut().map(|x| {
                StringHashtableEntryMutRef(StringHashtableEntryMutRefInner::TableEmpty(
                    x,
                    PhantomData,
                ))
            }),
            _ => unsafe {
                self.table
                    .get_mut(&FallbackKey::new(key))
                    .map(|x| StringHashtableEntryMutRef(StringHashtableEntryMutRefInner::Table(x)))
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
            0 => self
                .table_empty
                .insert()
                .map(|x| {
                    StringHashtableEntryMutRef(StringHashtableEntryMutRefInner::TableEmpty(
                        x,
                        PhantomData,
                    ))
                })
                .map_err(|x| {
                    StringHashtableEntryMutRef(StringHashtableEntryMutRefInner::TableEmpty(
                        x,
                        PhantomData,
                    ))
                }),

            _ => {
                self.table.check_grow();
                match self.table.insert(FallbackKey::new(key)) {
                    Ok(e) => {
                        // We need to save the key to avoid drop it.
                        let s = self.arena.alloc_slice_copy(key);
                        e.set_key(FallbackKey::new_with_hash(s, e.key.assume_init_ref().hash));

                        self.key_size += key.len();
                        Ok(StringHashtableEntryMutRef(
                            StringHashtableEntryMutRefInner::Table(e),
                        ))
                    }
                    Err(e) => Err(StringHashtableEntryMutRef(
                        StringHashtableEntryMutRefInner::Table(e),
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
            0 => self
                .table_empty
                .insert()
                .map(|x| {
                    StringHashtableEntryMutRef(StringHashtableEntryMutRefInner::TableEmpty(
                        x,
                        PhantomData,
                    ))
                })
                .map_err(|x| {
                    StringHashtableEntryMutRef(StringHashtableEntryMutRefInner::TableEmpty(
                        x,
                        PhantomData,
                    ))
                }),
            _ => {
                self.table.check_grow();
                match self
                    .table
                    .insert_with_hash(FallbackKey::new_with_hash(key, hash), hash)
                {
                    Ok(e) => {
                        // We need to save the key to avoid drop it.
                        let s = self.arena.alloc_slice_copy(key);
                        e.set_key(FallbackKey::new_with_hash(s, hash));

                        self.key_size += key.len();
                        Ok(StringHashtableEntryMutRef(
                            StringHashtableEntryMutRefInner::Table(e),
                        ))
                    }
                    Err(e) => Err(StringHashtableEntryMutRef(
                        StringHashtableEntryMutRefInner::Table(e),
                    )),
                }
            }
        }
    }

    fn iter(&self) -> Self::Iterator<'_> {
        StringHashtableIter {
            it_empty: Some(self.table_empty.iter()),
            it: Some(self.table.iter()),
            _phantom: PhantomData,
        }
    }

    fn clear(&mut self) {
        self.table_empty.clear();
        self.table.clear();
        drop(std::mem::take(&mut self.arena));
    }
}
