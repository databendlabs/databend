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

use std::intrinsics::assume;
use std::intrinsics::unlikely;
use std::iter::TrustedLen;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_base::mem_allocator::MmapAllocator;

use crate::container::Container;
use crate::container::HeapContainer;
use crate::short_string_hashtable::FallbackKey;
use crate::table0::Entry;
use crate::table_empty::TableEmpty;
use crate::traits::EntryMutRefLike;
use crate::traits::EntryRefLike;
use crate::DictionaryStringHashMap;
use crate::FastHash;
use crate::HashtableLike;
use crate::StringHashSet;

#[derive(Clone, Copy, Debug, Eq)]
pub struct DictionaryKeys {
    pub keys: NonNull<[NonNull<[u8]>]>,
}

impl PartialEq for DictionaryKeys {
    fn eq(&self, other: &Self) -> bool {
        unsafe { self.keys.as_ref() == other.keys.as_ref() }
    }
}

impl FastHash for DictionaryKeys {
    #[inline(always)]
    fn fast_hash(&self) -> u64 {
        unsafe {
            let hash = self
                .keys
                .as_ref()
                .iter()
                .map(|x| x.as_ref().fast_hash())
                .reduce(|left, right| {
                    let mut a = (left ^ right).wrapping_mul(0x9ddfea08eb382d69_u64);
                    a ^= a >> 47;
                    let mut b = (right ^ a).wrapping_mul(0x9ddfea08eb382d69_u64);
                    b ^= b >> 47;
                    b.wrapping_mul(0x9ddfea08eb382d69_u64)
                })
                .unwrap_or_default();
            if cfg!(target_feature = "sse4.2") {
                hash >> 32
            } else {
                hash
            }
        }
    }
}

impl DictionaryKeys {
    pub fn create(keys: &[NonNull<[u8]>]) -> DictionaryKeys {
        DictionaryKeys {
            keys: NonNull::from(keys),
        }
    }
}

unsafe impl Send for DictionaryKeys {}

unsafe impl Sync for DictionaryKeys {}

pub struct DictionaryEntry<V> {
    pub(crate) key: MaybeUninit<NonNull<NonNull<[u8]>>>,
    pub(crate) val: MaybeUninit<V>,
    pub(crate) hash: u64,
}

impl<V> DictionaryEntry<V> {
    pub fn is_zero(&self) -> bool {
        #[allow(useless_ptr_null_checks)]
        unsafe {
            self.key.assume_init_ref().as_ptr().is_null()
        }
    }
}

pub struct DictionaryStringHashTable<V> {
    arena: Arc<Bump>,
    dict_keys: usize,
    entries_len: usize,
    pub(crate) entries: HeapContainer<DictionaryEntry<V>, MmapAllocator>,
    pub dictionary_hashset: StringHashSet<[u8]>,
}

unsafe impl<V> Send for DictionaryStringHashTable<V> {}

unsafe impl<V> Sync for DictionaryStringHashTable<V> {}

impl<V> DictionaryStringHashTable<V> {
    pub fn new(bump: Arc<Bump>, dict_keys: usize) -> DictionaryStringHashMap<V> {
        DictionaryStringHashTable::<V> {
            arena: bump.clone(),
            dict_keys,
            entries_len: 0,
            entries: unsafe { HeapContainer::new_zeroed(256, MmapAllocator::default()) },
            dictionary_hashset: StringHashSet::new(bump),
        }
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub fn check_grow(&mut self) {
        if unlikely((self.len() + 1) * 2 > self.capacity()) {
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

            if self.entries[i].is_zero() {
                continue;
            }

            let hash = self.entries[i].hash;
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
            if self.entries[i].is_zero() {
                break;
            }

            let hash = self.entries[i].hash;
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

    /// # Safety
    ///
    /// Provided hash is correct.
    #[inline(always)]
    pub unsafe fn get_with_hash(
        &self,
        key: &[NonNull<[u8]>],
        hash: u64,
    ) -> Option<&DictionaryEntry<V>> {
        assume(key.len() == self.dict_keys);
        let index = (hash as usize) & (self.entries.len() - 1);
        for i in (index..self.entries.len()).chain(0..index) {
            assume(i < self.entries.len());
            if self.entries[i].is_zero() {
                return None;
            }

            let slice = std::slice::from_raw_parts(
                self.entries[i].key.assume_init_ref().as_ptr(),
                self.dict_keys,
            );

            if key == slice {
                return Some(&self.entries[i]);
            }
        }
        None
    }

    /// # Safety
    ///
    /// Provided hash is correct.
    #[inline(always)]
    pub unsafe fn get_mut_with_hash(
        &mut self,
        key: &[NonNull<[u8]>],
        hash: u64,
    ) -> Option<&mut DictionaryEntry<V>> {
        assume(key.len() == self.dict_keys);
        let index = (hash as usize) & (self.entries.len() - 1);
        for i in (index..self.entries.len()).chain(0..index) {
            assume(i < self.entries.len());
            if self.entries[i].is_zero() {
                return None;
            }

            let slice = std::slice::from_raw_parts(
                self.entries[i].key.assume_init_ref().as_ptr(),
                self.dict_keys,
            );

            if key == slice {
                return Some(&mut self.entries[i]);
            }
        }
        None
    }

    pub fn dictionary_slot_iter(&self) -> DictionarySlotIter<'_> {
        DictionarySlotIter {
            i: 0,
            empty: Some(&self.dictionary_hashset.table_empty),
            entities_slice: self.dictionary_hashset.table.entries.as_ref(),
        }
    }

    pub fn iter_slot(&self) -> DictionaryTableKeySlotIter<'_, V> {
        DictionaryTableKeySlotIter::<'_, V> {
            entities_i: 0,
            dict_keys_i: 0,
            dict_keys: self.dict_keys,
            entities: self.entries.as_ref(),
            dictionary_hashset: &self.dictionary_hashset,
        }
    }
}

impl<V> HashtableLike for DictionaryStringHashTable<V> {
    type Key = DictionaryKeys;
    type Value = V;
    type EntryRef<'a> = DictionaryEntryRef<'a, V> where Self: 'a, V: 'a;
    type EntryMutRef<'a> = DictionaryMutEntryRef<'a, V> where Self: 'a, V: 'a;
    type Iterator<'a> = DictionaryTableIter<'a, V> where Self: 'a, V: 'a;
    type IteratorMut<'a> = DictionaryTableMutIter<'a, V> where Self: 'a, V: 'a;

    fn len(&self) -> usize {
        self.entries_len
    }

    fn bytes_len(&self, without_arena: bool) -> usize {
        match without_arena {
            true => self.dictionary_hashset.bytes_len(true) + self.entries.heap_bytes(),
            false => {
                self.dictionary_hashset.bytes_len(false)
                    + self.entries.heap_bytes()
                    + self.arena.allocated_bytes()
            }
        }
    }

    fn entry(&self, key: &Self::Key) -> Option<Self::EntryRef<'_>> {
        unsafe {
            assume(key.keys.len() == self.dict_keys);
            let mut dictionary_keys = Vec::with_capacity(self.dict_keys);

            for key in key.keys.as_ref() {
                let entry = self.dictionary_hashset.entry(key.as_ref())?;
                dictionary_keys.push(NonNull::from(entry.key()));
            }

            self.get_with_hash(&dictionary_keys, key.fast_hash())
                .map(|entry| DictionaryEntryRef::create(entry, self.dict_keys))
        }
    }

    fn entry_mut(&mut self, key: &Self::Key) -> Option<Self::EntryMutRef<'_>> {
        unsafe {
            assume(key.keys.len() == self.dict_keys);
            let mut dictionary_keys = Vec::with_capacity(self.dict_keys);

            for key in key.keys.as_ref() {
                let entry = self.dictionary_hashset.entry(key.as_ref())?;
                dictionary_keys.push(NonNull::from(entry.key()));
            }

            let keys = self.dict_keys;
            self.get_mut_with_hash(&dictionary_keys, key.fast_hash())
                .map(|entry| DictionaryMutEntryRef::create(entry, keys))
        }
    }

    fn get(&self, key: &Self::Key) -> Option<&Self::Value> {
        self.entry(key).map(|e| e.get())
    }

    fn get_mut(&mut self, key: &Self::Key) -> Option<&mut Self::Value> {
        unsafe { self.entry_mut(key).map(|mut e| &mut *e.get_mut_ptr()) }
    }

    unsafe fn insert(
        &mut self,
        key: &Self::Key,
    ) -> Result<&mut MaybeUninit<Self::Value>, &mut Self::Value> {
        match self.insert_and_entry(key) {
            Ok(mut e) => Ok(&mut *(e.get_mut_ptr() as *mut MaybeUninit<V>)),
            Err(mut e) => Err(&mut *e.get_mut_ptr()),
        }
    }

    unsafe fn insert_and_entry(
        &mut self,
        key: &Self::Key,
    ) -> Result<Self::EntryMutRef<'_>, Self::EntryMutRef<'_>> {
        self.insert_and_entry_with_hash(key, key.fast_hash())
    }

    unsafe fn insert_and_entry_with_hash(
        &mut self,
        key: &Self::Key,
        hash: u64,
    ) -> Result<Self::EntryMutRef<'_>, Self::EntryMutRef<'_>> {
        self.check_grow();

        assume(key.keys.len() == self.dict_keys);
        let mut dictionary_keys = Vec::with_capacity(self.dict_keys);

        for key in key.keys.as_ref() {
            // println!("insert: {:?}", String::from_utf8(key.as_ref().to_vec()).unwrap());
            dictionary_keys.push(NonNull::from(
                match self.dictionary_hashset.insert_and_entry(key.as_ref()) {
                    Ok(e) => e.key(),
                    Err(e) => e.key(),
                },
            ));
        }

        let index = (hash as usize) & (self.entries.len() - 1);
        for i in (index..self.entries.len()).chain(0..index) {
            assume(i < self.entries.len());

            if self.entries[i].is_zero() {
                self.entries_len += 1;

                let global_keys = self.arena.alloc_slice_copy(&dictionary_keys);
                // for key in global_keys.iter() {
                //     println!("insert: {:?}", String::from_utf8(key.as_ref().to_vec()).unwrap());
                // }
                self.entries[i].hash = hash;
                self.entries[i]
                    .key
                    .write(NonNull::new(global_keys.as_mut_ptr()).unwrap());
                return Ok(DictionaryMutEntryRef::create(
                    &mut self.entries[i],
                    self.dict_keys,
                ));
            }

            let slice = std::slice::from_raw_parts(
                self.entries[i].key.assume_init_ref().as_ptr(),
                self.dict_keys,
            );

            if dictionary_keys.as_slice() == slice {
                return Err(DictionaryMutEntryRef::create(
                    &mut self.entries[i],
                    self.dict_keys,
                ));
            }
        }

        panic!("the hash table overflows")
    }

    fn iter(&self) -> Self::Iterator<'_> {
        DictionaryTableIter {
            i: 0,
            slice: &self.entries,
            dict_keys: self.dict_keys,
        }
    }

    fn clear(&mut self) {
        unsafe {
            self.entries_len = 0;

            if std::mem::needs_drop::<V>() {
                for entry in self.entries.as_mut() {
                    if !entry.is_zero() {
                        std::ptr::drop_in_place(&mut entry.val);
                    }
                }
            }

            self.entries = HeapContainer::new_zeroed(0, MmapAllocator::default());
        }

        self.dictionary_hashset.clear();
    }
}

pub struct DictionaryEntryRef<'a, V>
where Self: 'a
{
    key: DictionaryKeys,
    entry: &'a DictionaryEntry<V>,
}

impl<'a, V> Clone for DictionaryEntryRef<'a, V>
where Self: 'a
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, V> Copy for DictionaryEntryRef<'a, V> {}

impl<'a, V> DictionaryEntryRef<'a, V>
where Self: 'a
{
    pub fn create(entry: &'a DictionaryEntry<V>, keys: usize) -> DictionaryEntryRef<'a, V> {
        unsafe {
            let begin = entry.key.assume_init_ref().as_ptr();
            let slice = std::slice::from_raw_parts(begin, keys);

            // for x in slice {
            // for key in res.key.keys.as_ref() {
            // println!("key {:?}", String::from_utf8(x.as_ref().to_vec()).unwrap());
            // }
            // }

            DictionaryEntryRef {
                entry,
                key: DictionaryKeys::create(slice),
            }
        }
    }
}

impl<'a, V: 'a> EntryRefLike for DictionaryEntryRef<'a, V> {
    type KeyRef = &'a DictionaryKeys;
    type ValueRef = &'a V;

    fn key(&self) -> Self::KeyRef {
        unsafe { &*(&self.key as *const _) }
    }

    fn get(&self) -> Self::ValueRef {
        unsafe { self.entry.val.assume_init_ref() }
    }
}

pub struct DictionaryMutEntryRef<'a, V> {
    key: DictionaryKeys,
    entry: &'a mut DictionaryEntry<V>,
}

impl<'a, V> DictionaryMutEntryRef<'a, V> {
    pub fn create(entry: &'a mut DictionaryEntry<V>, keys: usize) -> DictionaryMutEntryRef<'a, V> {
        unsafe {
            let begin = entry.key.assume_init_ref().as_ptr();
            let slice = std::slice::from_raw_parts(begin, keys);

            DictionaryMutEntryRef {
                entry,
                key: DictionaryKeys::create(slice),
            }
        }
    }

    pub fn get_mut_ptr(&mut self) -> *mut V {
        self.entry.val.as_mut_ptr()
    }
}

impl<'a, V: 'a> EntryMutRefLike for DictionaryMutEntryRef<'a, V> {
    type Key = DictionaryKeys;
    type Value = V;

    fn key(&self) -> &Self::Key {
        unsafe { &*(&self.key as *const _) }
    }

    fn get(&self) -> &Self::Value {
        unsafe { self.entry.val.assume_init_ref() }
    }

    fn get_mut(&mut self) -> &mut Self::Value {
        unsafe { self.entry.val.assume_init_mut() }
    }

    fn write(&mut self, value: Self::Value) {
        self.entry.val.write(value);
    }
}

pub struct DictionaryTableIter<'a, V> {
    slice: &'a [DictionaryEntry<V>],
    i: usize,
    dict_keys: usize,
}

unsafe impl<'a, V> TrustedLen for DictionaryTableIter<'a, V> {}

impl<'a, V> Iterator for DictionaryTableIter<'a, V> {
    type Item = DictionaryEntryRef<'a, V>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.i < self.slice.len() && self.slice[self.i].is_zero() {
            self.i += 1;
        }

        if self.i == self.slice.len() {
            None
        } else {
            let res = unsafe { &*(self.slice.as_ptr().add(self.i) as *const _) };
            self.i += 1;
            Some(DictionaryEntryRef::create(res, self.dict_keys))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remain = self.slice.len() - self.i;
        (remain, Some(remain))
    }
}

pub struct DictionaryTableMutIter<'a, V> {
    slice: &'a mut [DictionaryEntry<V>],
    i: usize,
    dict_keys: usize,
}

impl<'a, V> Iterator for DictionaryTableMutIter<'a, V> {
    type Item = DictionaryMutEntryRef<'a, V>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.i < self.slice.len() && self.slice[self.i].is_zero() {
            self.i += 1;
        }
        if self.i == self.slice.len() {
            None
        } else {
            let res = unsafe { &mut *(self.slice.as_ptr().add(self.i) as *mut _) };
            self.i += 1;
            Some(DictionaryMutEntryRef::create(res, self.dict_keys))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remain = self.slice.len() - self.i;
        (remain, Some(remain))
    }
}

pub struct DictionarySlotIter<'a> {
    empty: Option<&'a TableEmpty<(), MmapAllocator>>,
    entities_slice: &'a [Entry<FallbackKey, ()>],
    i: usize,
}

impl<'a> Iterator for DictionarySlotIter<'a> {
    type Item = Option<&'a [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.empty.is_some() {
            self.empty = None;
            return Some(None);
        }

        match self.i == self.entities_slice.len() {
            true => None,
            false => {
                let res = match &self.entities_slice[self.i].key().key {
                    None => Some(None),
                    Some(x) => unsafe { Some(Some(x.as_ref())) },
                };

                self.i += 1;
                res
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remain = self.entities_slice.len() - self.i;
        match self.empty.is_some() {
            true => (remain + 1, Some(remain + 1)),
            false => (remain, Some(remain)),
        }
    }
}

pub struct DictionaryTableKeySlotIter<'a, T> {
    entities_i: usize,
    dict_keys_i: usize,
    dict_keys: usize,
    entities: &'a [DictionaryEntry<T>],
    dictionary_hashset: &'a StringHashSet<[u8]>,
}

impl<'a, T> Iterator for DictionaryTableKeySlotIter<'a, T> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        'worker: loop {
            while self.entities_i < self.entities.len() && self.entities[self.entities_i].is_zero()
            {
                self.entities_i += 1;
            }

            return match self.entities_i == self.entities.len() {
                true => None,
                false => unsafe {
                    if self.dict_keys_i == self.dict_keys {
                        self.dict_keys_i = 0;
                        self.entities_i += 1;
                        continue 'worker;
                    }

                    let slice = std::slice::from_raw_parts(
                        self.entities[self.entities_i]
                            .key
                            .assume_init_ref()
                            .as_ptr(),
                        self.dict_keys,
                    );

                    let key = slice[self.dict_keys_i];
                    self.dict_keys_i += 1;
                    Some(
                        self.dictionary_hashset
                            .get_slot_index(key.as_ref())
                            .unwrap(),
                    )
                },
            };
        }
    }
}
