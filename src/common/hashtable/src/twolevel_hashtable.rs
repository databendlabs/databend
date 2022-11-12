// Copyright 2022 Datafuse Labs.
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

use std::collections::VecDeque;
use std::mem::MaybeUninit;

use crate::FastHash;
use crate::HashtableLike;

const BUCKETS: usize = 256;
const BUCKETS_LG2: u32 = 8;

pub struct TwoLevelHashtable<Impl> {
    tables: Vec<Impl>,
}

impl<Impl> TwoLevelHashtable<Impl> {
    pub fn create(tables: Vec<Impl>) -> Self {
        assert_eq!(tables.len(), BUCKETS);
        TwoLevelHashtable::<Impl> { tables }
    }
}

impl<K: ?Sized + FastHash, V, Impl: HashtableLike<Key = K, Value = V>> HashtableLike
    for TwoLevelHashtable<Impl>
{
    type Key = Impl::Key;
    type Value = Impl::Value;
    type EntryRef<'a> = Impl::EntryRef<'a> where Self: 'a, Self::Key: 'a, Self::Value: 'a;
    type EntryMutRef<'a> = Impl::EntryMutRef<'a> where Self: 'a, Self::Key: 'a, Self::Value: 'a;
    type Iterator<'a> = TwoLevelHashtableIter<Impl::Iterator<'a>> where Self: 'a, Self::Key: 'a, Self::Value: 'a;
    type IteratorMut<'a> = TwoLevelHashtableIter<Impl::IteratorMut<'a>> where Self: 'a, Self::Key: 'a, Self::Value: 'a;

    fn len(&self) -> usize {
        self.tables.iter().map(|x| x.len()).sum::<usize>()
    }

    fn entry(&self, key: &Self::Key) -> Option<Self::EntryRef<'_>> {
        let hash = key.fast_hash();
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        self.tables[index].entry(key)
    }

    fn entry_mut(&mut self, key: &Self::Key) -> Option<Self::EntryMutRef<'_>> {
        let hash = key.fast_hash();
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        self.tables[index].entry_mut(key)
    }

    fn get(&self, key: &Self::Key) -> Option<&Self::Value> {
        let hash = key.fast_hash();
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        self.tables[index].get(key)
    }

    fn get_mut(&mut self, key: &Self::Key) -> Option<&mut Self::Value> {
        let hash = key.fast_hash();
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        self.tables[index].get_mut(key)
    }

    unsafe fn insert(
        &mut self,
        key: &Self::Key,
    ) -> Result<&mut MaybeUninit<Self::Value>, &mut Self::Value> {
        let hash = key.fast_hash();
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        self.tables[index].insert(key)
    }

    #[inline(always)]
    unsafe fn insert_and_entry(
        &mut self,
        key: &Self::Key,
    ) -> Result<Self::EntryMutRef<'_>, Self::EntryMutRef<'_>> {
        let hash = key.fast_hash();
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        self.tables[index].insert_and_entry_with_hash(key, hash)
    }

    #[inline(always)]
    unsafe fn insert_and_entry_with_hash(
        &mut self,
        key: &Self::Key,
        hash: u64,
    ) -> Result<Self::EntryMutRef<'_>, Self::EntryMutRef<'_>> {
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        self.tables[index].insert_and_entry_with_hash(key, hash)
    }

    fn iter(&self) -> Self::Iterator<'_> {
        let mut inner = VecDeque::with_capacity(self.tables.len());
        for table in &self.tables {
            inner.push_back(table.iter());
        }

        TwoLevelHashtableIter::create(inner)
    }

    fn iter_mut(&mut self) -> Self::IteratorMut<'_> {
        let mut inner = VecDeque::with_capacity(self.tables.len());
        for table in &mut self.tables {
            inner.push_back(table.iter_mut());
        }

        TwoLevelHashtableIter::create(inner)
    }
}

pub struct TwoLevelHashtableIter<Impl> {
    inner: VecDeque<Impl>,
}

impl<Impl> TwoLevelHashtableIter<Impl> {
    pub fn create(inner: VecDeque<Impl>) -> Self {
        TwoLevelHashtableIter::<Impl> { inner }
    }
}

impl<Impl: Iterator> Iterator for TwoLevelHashtableIter<Impl> {
    type Item = Impl::Item;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.front_mut() {
                None => {
                    return None;
                }
                Some(front) => {
                    if let Some(next) = front.next() {
                        return Some(next);
                    }
                }
            }

            self.inner.pop_front();
        }
    }
}
