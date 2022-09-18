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

use super::container::HeapContainer;
use super::hashtable::Hashtable;
use super::hashtable::HashtableIter;
use super::hashtable::HashtableIterMut;
use super::hashtable::HashtableIterMutPtr;
use super::hashtable::HashtableIterPtr;
use super::table0::Entry;
use super::table0::Table0;
use super::table0::Table0Iter;
use super::table0::Table0IterMut;
use super::table0::Table0IterPtr;
use super::traits::Keyable;
use super::utils::SliceIterPtr;
use super::utils::ZeroEntry;

const BUCKETS: usize = 256;
const BUCKETS_LG2: u32 = 8;

pub struct TwolevelHashtable<K, V, A = super::allocator::Default>
where
    K: Keyable,
    A: Allocator + Clone,
{
    zero: ZeroEntry<K, V>,
    tables: [Table0<K, V, HeapContainer<Entry<K, V>, A>, A>; BUCKETS],
}

unsafe impl<K: Keyable + Send, V: Send, A: Allocator + Clone + Send> Send
    for TwolevelHashtable<K, V, A>
{
}

unsafe impl<K: Keyable + Sync, V: Sync, A: Allocator + Clone + Sync> Sync
    for TwolevelHashtable<K, V, A>
{
}

impl<K, V, A> TwolevelHashtable<K, V, A>
where
    K: Keyable,
    A: Allocator + Clone + Default,
{
    pub fn new() -> Self {
        Self::new_in(Default::default())
    }
}

impl<K, V, A> TwolevelHashtable<K, V, A>
where
    K: Keyable,
    A: Allocator + Clone,
{
    pub fn new_in(allocator: A) -> Self {
        Self {
            zero: ZeroEntry(None),
            tables: std::array::from_fn(|_| Table0::with_capacity_in(256, allocator.clone())),
        }
    }
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.zero.is_some() as usize + self.tables.iter().map(|x| x.len()).sum::<usize>()
    }
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.zero.is_some() as usize + self.tables.iter().map(|x| x.capacity()).sum::<usize>()
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
        let hash = K::hash(key);
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        unsafe { self.tables[index].get_with_hash(key, hash) }
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
        let hash = K::hash(key);
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        unsafe { self.tables[index].get_with_hash_mut(key, hash) }
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
        let hash = K::hash(&key);
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        if unlikely((self.tables[index].len() + 1) * 2 > self.tables[index].capacity()) {
            if (self.tables[index].entries.len() >> 14) == 0 {
                self.tables[index].grow(2);
            } else {
                self.tables[index].grow(1);
            }
        }
        self.tables[index].insert_with_hash(key, hash)
    }
    #[inline(always)]
    pub unsafe fn insert(&mut self, key: K) -> Result<&mut MaybeUninit<V>, &mut V> {
        match self.insert_and_entry(key) {
            Ok(e) => Ok(&mut e.val),
            Err(e) => Err(e.val.assume_init_mut()),
        }
    }
    pub fn iter(&self) -> TwolevelHashtableIter<'_, K, V, A> {
        TwolevelHashtableIter {
            inner: self.zero.iter().chain(self.tables.iter().flat_map(
                Table0::iter
                    as fn(
                        &'_ Table0<K, V, HeapContainer<Entry<K, V>, A>, A>,
                    ) -> Table0Iter<'_, _, _>,
            )),
        }
    }
    pub fn iter_mut(&mut self) -> TwolevelHashtableIterMut<'_, K, V, A> {
        TwolevelHashtableIterMut {
            inner: self.zero.iter_mut().chain(self.tables.iter_mut().flat_map(
                Table0::iter_mut
                    as fn(
                        &'_ mut Table0<K, V, HeapContainer<Entry<K, V>, A>, A>,
                    ) -> Table0IterMut<'_, _, _>,
            )),
        }
    }
    pub fn iter_ptr(&self) -> TwolevelHashtableIterPtr<K, V, A> {
        TwolevelHashtableIterPtr {
            inner: self.zero.as_ref().map(|x| x as *const _).into_iter().chain(
                SliceIterPtr::new(self.tables.as_ref()).flat_map(
                    (|x| unsafe { Table0::iter_ptr(&*x) })
                        as fn(
                            *const Table0<K, V, HeapContainer<Entry<K, V>, A>, A>,
                        ) -> Table0IterPtr<K, V>,
                ),
            ),
        }
    }
    pub fn iter_mut_ptr(&self) -> TwolevelHashtableIterMutPtr<K, V, A> {
        TwolevelHashtableIterMutPtr {
            inner: self.iter_ptr(),
        }
    }
}

impl<K, V, A> From<Hashtable<K, V, A>> for TwolevelHashtable<K, V, A>
where
    K: Keyable,
    A: Allocator + Clone,
{
    fn from(mut src: Hashtable<K, V, A>) -> Self {
        let mut res = TwolevelHashtable::new_in(src.table.allocator.clone());
        unsafe {
            src.table.dropped = true;
            res.zero = ZeroEntry(src.zero.take());
            for entry in src.table.iter_mut_ptr() {
                let key = *(*entry).key.assume_init_ref();
                let val = std::ptr::read((*entry).val.assume_init_ref());
                let hash = K::hash(&key);
                let index = hash as usize >> (64u32 - BUCKETS_LG2);
                if unlikely((res.tables[index].len() + 1) * 2 > res.tables[index].capacity()) {
                    if (res.tables[index].entries.len() >> 14) == 0 {
                        res.tables[index].grow(2);
                    } else {
                        res.tables[index].grow(1);
                    }
                }
                res.tables[index]
                    .insert_with_hash(key, hash)
                    .ok()
                    .unwrap()
                    .write(val);
            }
        }
        res
    }
}

impl<K, A> TwolevelHashtable<K, (), A>
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
        for (i, table) in other.tables.into_iter().enumerate() {
            while (self.tables[i].len() + table.len()) * 2 > self.tables[i].capacity() {
                if (self.tables[i].entries.len() >> 22) == 0 {
                    self.tables[i].grow(2);
                } else {
                    self.tables[i].grow(1);
                }
            }
            unsafe {
                self.tables[i].merge(table);
            }
        }
    }
}

pub struct TwolevelHashtableIter<'a, K, V, A = super::allocator::Default>
where
    K: Keyable,
    A: Allocator + Clone,
{
    inner: std::iter::Chain<
        std::option::Iter<'a, Entry<K, V>>,
        std::iter::FlatMap<
            std::slice::Iter<'a, Table0<K, V, HeapContainer<Entry<K, V>, A>, A>>,
            Table0Iter<'a, K, V>,
            fn(&'a Table0<K, V, HeapContainer<Entry<K, V>, A>, A>) -> Table0Iter<'a, K, V>,
        >,
    >,
}

impl<'a, K, V, A> Iterator for TwolevelHashtableIter<'a, K, V, A>
where
    K: Keyable,
    A: Allocator + Clone,
{
    type Item = &'a Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub struct TwolevelHashtableIterMut<'a, K, V, A = super::allocator::Default>
where
    K: Keyable,
    A: Allocator + Clone,
{
    inner: std::iter::Chain<
        std::option::IterMut<'a, Entry<K, V>>,
        std::iter::FlatMap<
            std::slice::IterMut<'a, Table0<K, V, HeapContainer<Entry<K, V>, A>, A>>,
            Table0IterMut<'a, K, V>,
            fn(&'a mut Table0<K, V, HeapContainer<Entry<K, V>, A>, A>) -> Table0IterMut<'a, K, V>,
        >,
    >,
}

impl<'a, K, V, A> Iterator for TwolevelHashtableIterMut<'a, K, V, A>
where
    K: Keyable,
    A: Allocator + Clone,
{
    type Item = &'a mut Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub struct TwolevelHashtableIterPtr<K, V, A = super::allocator::Default>
where
    K: Keyable,
    A: Allocator + Clone,
{
    inner: std::iter::Chain<
        std::option::IntoIter<*const Entry<K, V>>,
        std::iter::FlatMap<
            SliceIterPtr<Table0<K, V, HeapContainer<Entry<K, V>, A>, A>>,
            Table0IterPtr<K, V>,
            fn(*const Table0<K, V, HeapContainer<Entry<K, V>, A>, A>) -> Table0IterPtr<K, V>,
        >,
    >,
}

impl<K, V, A> Iterator for TwolevelHashtableIterPtr<K, V, A>
where
    K: Keyable,
    A: Allocator + Clone,
{
    type Item = *const Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub struct TwolevelHashtableIterMutPtr<K, V, A = super::allocator::Default>
where
    K: Keyable,
    A: Allocator + Clone,
{
    inner: TwolevelHashtableIterPtr<K, V, A>,
}

impl<K, V, A> Iterator for TwolevelHashtableIterMutPtr<K, V, A>
where
    K: Keyable,
    A: Allocator + Clone,
{
    type Item = *mut Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|x| x as *mut _)
    }
}

pub enum HashtableKind<K, V, A = super::allocator::Default>
where
    K: Keyable,
    A: Allocator + Clone,
{
    Onelevel(Hashtable<K, V, A>),
    Twolevel(TwolevelHashtable<K, V, A>),
}

impl<K, V, A> HashtableKind<K, V, A>
where
    K: Keyable,
    A: Allocator + Clone + Default,
{
    pub fn new() -> Self {
        Self::new_in(Default::default())
    }
    pub fn new_twolevel() -> Self {
        Self::new_twolevel_in(Default::default())
    }
}

impl<K, V, A> HashtableKind<K, V, A>
where
    K: Keyable,
    A: Allocator + Clone,
{
    pub fn new_in(allocator: A) -> Self {
        Self::Onelevel(Hashtable::new_in(allocator))
    }
    pub fn new_twolevel_in(allocator: A) -> Self {
        Self::Twolevel(TwolevelHashtable::new_in(allocator))
    }
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        use HashtableKind::*;
        match self {
            Onelevel(x) => Hashtable::is_empty(x),
            Twolevel(x) => TwolevelHashtable::is_empty(x),
        }
    }
    #[inline(always)]
    pub fn len(&self) -> usize {
        use HashtableKind::*;
        match self {
            Onelevel(x) => Hashtable::len(x),
            Twolevel(x) => TwolevelHashtable::len(x),
        }
    }
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        use HashtableKind::*;
        match self {
            Onelevel(x) => Hashtable::capacity(x),
            Twolevel(x) => TwolevelHashtable::capacity(x),
        }
    }
    #[inline(always)]
    pub fn entry(&self, key: &K) -> Option<&Entry<K, V>> {
        use HashtableKind::*;
        match self {
            Onelevel(x) => Hashtable::entry(x, key),
            Twolevel(x) => TwolevelHashtable::entry(x, key),
        }
    }
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<&V> {
        use HashtableKind::*;
        match self {
            Onelevel(x) => Hashtable::get(x, key),
            Twolevel(x) => TwolevelHashtable::get(x, key),
        }
    }
    #[inline(always)]
    pub fn entry_mut(&mut self, key: &K) -> Option<&mut Entry<K, V>> {
        use HashtableKind::*;
        match self {
            Onelevel(x) => Hashtable::entry_mut(x, key),
            Twolevel(x) => TwolevelHashtable::entry_mut(x, key),
        }
    }
    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        use HashtableKind::*;
        match self {
            Onelevel(x) => Hashtable::get_mut(x, key),
            Twolevel(x) => TwolevelHashtable::get_mut(x, key),
        }
    }
    #[inline(always)]
    pub unsafe fn insert_and_entry(
        &mut self,
        key: K,
    ) -> Result<&mut Entry<K, V>, &mut Entry<K, V>> {
        use HashtableKind::*;
        match self {
            Onelevel(x) => Hashtable::insert_and_entry(x, key),
            Twolevel(x) => TwolevelHashtable::insert_and_entry(x, key),
        }
    }
    #[inline(always)]
    pub unsafe fn insert(&mut self, key: K) -> Result<&mut MaybeUninit<V>, &mut V> {
        use HashtableKind::*;
        match self {
            Onelevel(x) => Hashtable::insert(x, key),
            Twolevel(x) => TwolevelHashtable::insert(x, key),
        }
    }
    pub fn iter(&self) -> HashtableKindIter<'_, K, V, A> {
        use HashtableKind::*;
        match self {
            Onelevel(x) => HashtableKindIter::Onelevel(Hashtable::iter(x)),
            Twolevel(x) => HashtableKindIter::Twolevel(TwolevelHashtable::iter(x)),
        }
    }
    pub fn iter_mut(&mut self) -> HashtableKindIterMut<'_, K, V, A> {
        use HashtableKind::*;
        match self {
            Onelevel(x) => HashtableKindIterMut::Onelevel(Hashtable::iter_mut(x)),
            Twolevel(x) => HashtableKindIterMut::Twolevel(TwolevelHashtable::iter_mut(x)),
        }
    }
    pub fn iter_ptr(&self) -> HashtableKindIterPtr<K, V, A> {
        use HashtableKind::*;
        match self {
            Onelevel(x) => HashtableKindIterPtr::Onelevel(Hashtable::iter_ptr(x)),
            Twolevel(x) => HashtableKindIterPtr::Twolevel(TwolevelHashtable::iter_ptr(x)),
        }
    }
    pub fn iter_mut_ptr(&self) -> HashtableKindIterMutPtr<K, V, A> {
        use HashtableKind::*;
        match self {
            Onelevel(x) => HashtableKindIterMutPtr::Onelevel(Hashtable::iter_mut_ptr(x)),
            Twolevel(x) => HashtableKindIterMutPtr::Twolevel(TwolevelHashtable::iter_mut_ptr(x)),
        }
    }
    pub fn convert_to_twolevel(&mut self) {
        use HashtableKind::*;
        unsafe {
            if let Onelevel(x) = self {
                let onelevel = std::ptr::read(x);
                let twolevel = TwolevelHashtable::<K, V, A>::from(onelevel);
                std::ptr::write(self, Twolevel(twolevel));
            }
        }
    }
}

pub enum HashtableKindIter<'a, K, V, A = super::allocator::Default>
where
    K: Keyable,
    A: Allocator + Clone,
{
    Onelevel(HashtableIter<'a, K, V>),
    Twolevel(TwolevelHashtableIter<'a, K, V, A>),
}

impl<'a, K, V, A> Iterator for HashtableKindIter<'a, K, V, A>
where
    K: Keyable,
    A: Allocator + Clone,
{
    type Item = &'a Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        use HashtableKindIter::*;
        match self {
            Onelevel(x) => x.next(),
            Twolevel(x) => x.next(),
        }
    }
}

pub enum HashtableKindIterMut<'a, K, V, A = super::allocator::Default>
where
    K: Keyable,
    A: Allocator + Clone,
{
    Onelevel(HashtableIterMut<'a, K, V>),
    Twolevel(TwolevelHashtableIterMut<'a, K, V, A>),
}

impl<'a, K, V, A> Iterator for HashtableKindIterMut<'a, K, V, A>
where
    K: Keyable,
    A: Allocator + Clone,
{
    type Item = &'a mut Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        use HashtableKindIterMut::*;
        match self {
            Onelevel(x) => x.next(),
            Twolevel(x) => x.next(),
        }
    }
}

pub enum HashtableKindIterPtr<K, V, A = super::allocator::Default>
where
    K: Keyable,
    A: Allocator + Clone,
{
    Onelevel(HashtableIterPtr<K, V>),
    Twolevel(TwolevelHashtableIterPtr<K, V, A>),
}

impl<K, V, A> Iterator for HashtableKindIterPtr<K, V, A>
where
    K: Keyable,
    A: Allocator + Clone,
{
    type Item = *const Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        use HashtableKindIterPtr::*;
        match self {
            Onelevel(x) => x.next(),
            Twolevel(x) => x.next(),
        }
    }
}

pub enum HashtableKindIterMutPtr<K, V, A = super::allocator::Default>
where
    K: Keyable,
    A: Allocator + Clone,
{
    Onelevel(HashtableIterMutPtr<K, V>),
    Twolevel(TwolevelHashtableIterMutPtr<K, V, A>),
}

impl<K, V, A> Iterator for HashtableKindIterMutPtr<K, V, A>
where
    K: Keyable,
    A: Allocator + Clone,
{
    type Item = *mut Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        use HashtableKindIterMutPtr::*;
        match self {
            Onelevel(x) => x.next(),
            Twolevel(x) => x.next(),
        }
    }
}
