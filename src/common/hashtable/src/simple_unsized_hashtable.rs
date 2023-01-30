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
use std::marker::PhantomData;
use std::mem::MaybeUninit;

use bumpalo::Bump;
use common_base::mem_allocator::GlobalAllocator;
use common_base::mem_allocator::MmapAllocator;

use super::container::HeapContainer;
use super::table0::Entry;
use super::table0::Table0;
use super::traits::EntryMutRefLike;
use super::traits::EntryRefLike;
use super::traits::HashtableLike;
use super::traits::UnsizedKeyable;
use crate::table0::Table0Iter;
use crate::table0::Table0IterMut;
use crate::table_empty::TableEmpty;
use crate::table_empty::TableEmptyIter;
use crate::table_empty::TableEmptyIterMut;
use crate::tail_array::TailArray;
use crate::tail_array::TailArrayIter;
use crate::tail_array::TailArrayIterMut;
use crate::unsized_hashtable::FallbackKey;

/// Simple unsized hashtable is used for storing unsized keys in arena. It can be worked with HashMethodSerializer.
/// Different from `UnsizedHashtable`, it doesn't use adpative sub hashtable to store key values via key size.
/// It can be considered as a minimal hashtable implementation of UnsizedHashtable
pub struct SimpleUnsizedHashtable<K, V, A = MmapAllocator<GlobalAllocator>>
where
    K: UnsizedKeyable + ?Sized,
    A: Allocator + Clone,
{
    pub(crate) arena: Bump,
    pub(crate) key_size: usize,
    pub(crate) table_empty: TableEmpty<V, A>,
    pub(crate) table: Table0<FallbackKey, V, HeapContainer<Entry<FallbackKey, V>, A>, A>,
    pub(crate) tails: Option<TailArray<FallbackKey, V, A>>,
    pub(crate) _phantom: PhantomData<K>,
}

unsafe impl<K: UnsizedKeyable + ?Sized + Send, V: Send, A: Allocator + Clone + Send> Send
    for SimpleUnsizedHashtable<K, V, A>
{
}

unsafe impl<K: UnsizedKeyable + ?Sized + Sync, V: Sync, A: Allocator + Clone + Sync> Sync
    for SimpleUnsizedHashtable<K, V, A>
{
}

impl<K, V, A> SimpleUnsizedHashtable<K, V, A>
where
    K: UnsizedKeyable + ?Sized,
    A: Allocator + Clone + Default,
{
    pub fn new() -> Self {
        Self::with_capacity(128)
    }
}

impl<K, V, A> Default for SimpleUnsizedHashtable<K, V, A>
where
    K: UnsizedKeyable + ?Sized,
    A: Allocator + Clone + Default,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, A> SimpleUnsizedHashtable<K, (), A>
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

impl<K, V, A> SimpleUnsizedHashtable<K, V, A>
where
    K: UnsizedKeyable + ?Sized,
    A: Allocator + Clone + Default,
{
    /// The bump for strings doesn't allocate memory by `A`.
    pub fn with_capacity(capacity: usize) -> Self {
        let allocator = A::default();
        Self {
            arena: Bump::new(),
            key_size: 0,
            table_empty: TableEmpty::new_in(allocator.clone()),
            table: Table0::with_capacity_in(capacity, allocator),
            tails: None,
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
    ) -> Result<
        SimpleUnsizedHashtableEntryMutRef<'_, K, V>,
        SimpleUnsizedHashtableEntryMutRef<'_, K, V>,
    > {
        let key = (*key).as_bytes();
        match key.len() {
            0 => self
                .table_empty
                .insert()
                .map(|x| {
                    SimpleUnsizedHashtableEntryMutRef(
                        SimpleUnsizedHashtableEntryMutRefInner::TableEmpty(x, PhantomData),
                    )
                })
                .map_err(|x| {
                    SimpleUnsizedHashtableEntryMutRef(
                        SimpleUnsizedHashtableEntryMutRefInner::TableEmpty(x, PhantomData),
                    )
                }),
            _ => {
                if let Some(tails) = &mut self.tails {
                    let key = FallbackKey::new(key);
                    return Ok(SimpleUnsizedHashtableEntryMutRef(
                        SimpleUnsizedHashtableEntryMutRefInner::Table(tails.insert(key)),
                    ));
                }

                self.table.check_grow();
                self.table
                    .insert(FallbackKey::new(key))
                    .map(|x| {
                        self.key_size += key.len();
                        SimpleUnsizedHashtableEntryMutRef(
                            SimpleUnsizedHashtableEntryMutRefInner::Table(x),
                        )
                    })
                    .map_err(|x| {
                        SimpleUnsizedHashtableEntryMutRef(
                            SimpleUnsizedHashtableEntryMutRefInner::Table(x),
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

pub struct SimpleUnsizedHashtableIter<'a, K, V>
where K: UnsizedKeyable + ?Sized
{
    it_empty: Option<TableEmptyIter<'a, V>>,
    it: Option<Table0Iter<'a, FallbackKey, V>>,
    tail_it: Option<TailArrayIter<'a, FallbackKey, V>>,
    _phantom: PhantomData<&'a mut K>,
}

impl<'a, K, V> Iterator for SimpleUnsizedHashtableIter<'a, K, V>
where K: UnsizedKeyable + ?Sized
{
    type Item = SimpleUnsizedHashtableEntryRef<'a, K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(it) = self.it_empty.as_mut() {
            if let Some(e) = it.next() {
                return Some(SimpleUnsizedHashtableEntryRef(
                    SimpleUnsizedHashtableEntryRefInner::TableEmpty(e, PhantomData),
                ));
            }
            self.it_empty = None;
        }
        if let Some(it) = self.it.as_mut() {
            if let Some(e) = it.next() {
                return Some(SimpleUnsizedHashtableEntryRef(
                    SimpleUnsizedHashtableEntryRefInner::Table(e),
                ));
            }
            self.it = None;
        }

        if let Some(it) = self.tail_it.as_mut() {
            if let Some(e) = it.next() {
                return Some(SimpleUnsizedHashtableEntryRef(
                    SimpleUnsizedHashtableEntryRefInner::Table(e),
                ));
            }
            self.tail_it = None;
        }
        None
    }
}

pub struct SimpleUnsizedHashtableIterMut<'a, K, V>
where K: UnsizedKeyable + ?Sized
{
    it_empty: Option<TableEmptyIterMut<'a, V>>,
    it: Option<Table0IterMut<'a, FallbackKey, V>>,
    tail_it: Option<TailArrayIterMut<'a, FallbackKey, V>>,
    _phantom: PhantomData<&'a mut K>,
}

impl<'a, K, V> Iterator for SimpleUnsizedHashtableIterMut<'a, K, V>
where K: UnsizedKeyable + ?Sized
{
    type Item = SimpleUnsizedHashtableEntryMutRef<'a, K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(it) = self.it_empty.as_mut() {
            if let Some(e) = it.next() {
                return Some(SimpleUnsizedHashtableEntryMutRef(
                    SimpleUnsizedHashtableEntryMutRefInner::TableEmpty(e, PhantomData),
                ));
            }
            self.it_empty = None;
        }

        if let Some(it) = self.it.as_mut() {
            if let Some(e) = it.next() {
                return Some(SimpleUnsizedHashtableEntryMutRef(
                    SimpleUnsizedHashtableEntryMutRefInner::Table(e),
                ));
            }
            self.it = None;
        }

        if let Some(it) = self.tail_it.as_mut() {
            if let Some(e) = it.next() {
                return Some(SimpleUnsizedHashtableEntryMutRef(
                    SimpleUnsizedHashtableEntryMutRefInner::Table(e),
                ));
            }
            self.tail_it = None;
        }
        None
    }
}

enum SimpleUnsizedHashtableEntryRefInner<'a, K: ?Sized, V> {
    TableEmpty(&'a Entry<[u8; 0], V>, PhantomData<K>),
    Table(&'a Entry<FallbackKey, V>),
}

impl<'a, K: ?Sized, V> Copy for SimpleUnsizedHashtableEntryRefInner<'a, K, V> {}

impl<'a, K: ?Sized, V> Clone for SimpleUnsizedHashtableEntryRefInner<'a, K, V> {
    fn clone(&self) -> Self {
        use SimpleUnsizedHashtableEntryRefInner::*;
        match self {
            TableEmpty(a, b) => TableEmpty(a, *b),
            Table(a) => Table(a),
        }
    }
}

impl<'a, K: ?Sized + UnsizedKeyable, V> SimpleUnsizedHashtableEntryRefInner<'a, K, V> {
    fn key(self) -> &'a K {
        use SimpleUnsizedHashtableEntryRefInner::*;
        match self {
            TableEmpty(_, _) => unsafe { UnsizedKeyable::from_bytes(&[]) },
            Table(e) => unsafe {
                UnsizedKeyable::from_bytes(e.key.assume_init().key.unwrap().as_ref())
            },
        }
    }
    fn get(self) -> &'a V {
        use SimpleUnsizedHashtableEntryRefInner::*;
        match self {
            TableEmpty(e, _) => e.get(),
            Table(e) => e.get(),
        }
    }
    fn get_ptr(self) -> *const V {
        use SimpleUnsizedHashtableEntryRefInner::*;
        match self {
            TableEmpty(e, _) => e.val.as_ptr(),
            Table(e) => e.val.as_ptr(),
        }
    }
}

pub struct SimpleUnsizedHashtableEntryRef<'a, K: ?Sized, V>(
    SimpleUnsizedHashtableEntryRefInner<'a, K, V>,
);

impl<'a, K: ?Sized, V> Copy for SimpleUnsizedHashtableEntryRef<'a, K, V> {}

impl<'a, K: ?Sized, V> Clone for SimpleUnsizedHashtableEntryRef<'a, K, V> {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

impl<'a, K: ?Sized + UnsizedKeyable, V> SimpleUnsizedHashtableEntryRef<'a, K, V> {
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

enum SimpleUnsizedHashtableEntryMutRefInner<'a, K: ?Sized, V> {
    TableEmpty(&'a mut Entry<[u8; 0], V>, PhantomData<K>),
    Table(&'a mut Entry<FallbackKey, V>),
}

impl<'a, K: ?Sized + UnsizedKeyable, V> SimpleUnsizedHashtableEntryMutRefInner<'a, K, V> {
    fn key(&self) -> &'a K {
        use SimpleUnsizedHashtableEntryMutRefInner::*;
        match self {
            TableEmpty(_, _) => unsafe { &*(UnsizedKeyable::from_bytes(&[]) as *const K) },
            Table(e) => unsafe {
                UnsizedKeyable::from_bytes(e.key.assume_init().key.unwrap().as_ref())
            },
        }
    }
    fn get(&self) -> &V {
        use SimpleUnsizedHashtableEntryMutRefInner::*;
        match self {
            TableEmpty(e, _) => e.get(),
            Table(e) => e.get(),
        }
    }
    fn get_ptr(&self) -> *const V {
        use SimpleUnsizedHashtableEntryMutRefInner::*;
        match self {
            TableEmpty(e, _) => e.val.as_ptr(),
            Table(e) => e.val.as_ptr(),
        }
    }
    fn get_mut(&mut self) -> &mut V {
        use SimpleUnsizedHashtableEntryMutRefInner::*;
        match self {
            TableEmpty(e, _) => e.get_mut(),
            Table(e) => e.get_mut(),
        }
    }
    fn write(&mut self, val: V) {
        use SimpleUnsizedHashtableEntryMutRefInner::*;
        match self {
            TableEmpty(e, _) => e.write(val),
            Table(e) => e.write(val),
        }
    }
}

pub struct SimpleUnsizedHashtableEntryMutRef<'a, K: ?Sized, V>(
    SimpleUnsizedHashtableEntryMutRefInner<'a, K, V>,
);

impl<'a, K: ?Sized + UnsizedKeyable, V> SimpleUnsizedHashtableEntryMutRef<'a, K, V> {
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
    for SimpleUnsizedHashtableEntryRef<'a, K, V>
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
    for SimpleUnsizedHashtableEntryMutRef<'a, K, V>
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

impl<V, A> HashtableLike for SimpleUnsizedHashtable<[u8], V, A>
where A: Allocator + Clone + Default
{
    type Key = [u8];
    type Value = V;

    type EntryRef<'a> = SimpleUnsizedHashtableEntryRef<'a, [u8], V> where Self: 'a, V: 'a;
    type EntryMutRef<'a> = SimpleUnsizedHashtableEntryMutRef<'a, [u8], V> where Self: 'a, V: 'a;

    type Iterator<'a> = SimpleUnsizedHashtableIter<'a, [u8], V> where Self: 'a, V: 'a;
    type IteratorMut<'a> = SimpleUnsizedHashtableIterMut<'a, [u8], V> where Self: 'a, V: 'a;

    fn len(&self) -> usize {
        self.len()
    }

    fn bytes_len(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.arena.allocated_bytes()
            + self.table_empty.heap_bytes()
            + self.table.heap_bytes()
    }

    fn unsize_key_size(&self) -> Option<usize> {
        Some(self.key_size)
    }

    fn entry(&self, key: &Self::Key) -> Option<Self::EntryRef<'_>> {
        let key = key.as_bytes();
        match key.len() {
            0 => self.table_empty.get().map(|x| {
                SimpleUnsizedHashtableEntryRef(SimpleUnsizedHashtableEntryRefInner::TableEmpty(
                    x,
                    PhantomData,
                ))
            }),
            _ => unsafe {
                self.table.get(&FallbackKey::new(key)).map(|x| {
                    SimpleUnsizedHashtableEntryRef(SimpleUnsizedHashtableEntryRefInner::Table(x))
                })
            },
        }
    }

    fn entry_mut(&mut self, key: &[u8]) -> Option<Self::EntryMutRef<'_>> {
        match key.len() {
            0 => self.table_empty.get_mut().map(|x| {
                SimpleUnsizedHashtableEntryMutRef(
                    SimpleUnsizedHashtableEntryMutRefInner::TableEmpty(x, PhantomData),
                )
            }),
            _ => unsafe {
                self.table.get_mut(&FallbackKey::new(key)).map(|x| {
                    SimpleUnsizedHashtableEntryMutRef(
                        SimpleUnsizedHashtableEntryMutRefInner::Table(x),
                    )
                })
            },
        }
    }

    fn get(&self, key: &Self::Key) -> Option<&Self::Value> {
        self.entry(key).map(|e| e.get())
    }

    fn get_mut(&mut self, key: &Self::Key) -> Option<&mut Self::Value> {
        self.entry_mut(key)
            .map(|e| unsafe { &mut *(e.get_mut_ptr() as *mut V) })
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
                    SimpleUnsizedHashtableEntryMutRef(
                        SimpleUnsizedHashtableEntryMutRefInner::TableEmpty(x, PhantomData),
                    )
                })
                .map_err(|x| {
                    SimpleUnsizedHashtableEntryMutRef(
                        SimpleUnsizedHashtableEntryMutRefInner::TableEmpty(x, PhantomData),
                    )
                }),

            _ => {
                if let Some(tails) = &mut self.tails {
                    let s = self.arena.alloc_slice_copy(key);
                    let key = FallbackKey::new(s);

                    return Ok(SimpleUnsizedHashtableEntryMutRef(
                        SimpleUnsizedHashtableEntryMutRefInner::Table(tails.insert(key)),
                    ));
                }

                self.table.check_grow();
                match self.table.insert(FallbackKey::new(key)) {
                    Ok(e) => {
                        // We need to save the key to avoid drop it.
                        let s = self.arena.alloc_slice_copy(key);
                        e.set_key(FallbackKey::new_with_hash(s, e.key.assume_init_ref().hash));

                        self.key_size += key.len();
                        Ok(SimpleUnsizedHashtableEntryMutRef(
                            SimpleUnsizedHashtableEntryMutRefInner::Table(e),
                        ))
                    }
                    Err(e) => Err(SimpleUnsizedHashtableEntryMutRef(
                        SimpleUnsizedHashtableEntryMutRefInner::Table(e),
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
                    SimpleUnsizedHashtableEntryMutRef(
                        SimpleUnsizedHashtableEntryMutRefInner::TableEmpty(x, PhantomData),
                    )
                })
                .map_err(|x| {
                    SimpleUnsizedHashtableEntryMutRef(
                        SimpleUnsizedHashtableEntryMutRefInner::TableEmpty(x, PhantomData),
                    )
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
                        Ok(SimpleUnsizedHashtableEntryMutRef(
                            SimpleUnsizedHashtableEntryMutRefInner::Table(e),
                        ))
                    }
                    Err(e) => Err(SimpleUnsizedHashtableEntryMutRef(
                        SimpleUnsizedHashtableEntryMutRefInner::Table(e),
                    )),
                }
            }
        }
    }

    fn iter(&self) -> Self::Iterator<'_> {
        SimpleUnsizedHashtableIter {
            it_empty: Some(self.table_empty.iter()),
            it: Some(self.table.iter()),
            tail_it: self.tails.as_ref().map(|x| x.iter()),
            _phantom: PhantomData,
        }
    }

    fn enable_tail_array(&mut self) {
        if self.tails.is_none() {
            self.tails = Some(TailArray::new(Default::default()));
        }
    }

    fn clear(&mut self) {
        self.table_empty.clear();
        self.table.clear();
        let _ = self.tails.take();
        drop(std::mem::take(&mut self.arena));
    }
}
