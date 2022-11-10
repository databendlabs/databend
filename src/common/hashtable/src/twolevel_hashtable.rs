use std::collections::VecDeque;
use std::mem::MaybeUninit;

use crate::FastHash;
use crate::HashtableLike;

const BUCKETS: usize = 256;
const BUCKETS_LG2: u32 = 8;

pub struct TwoLevelHashtable<Impl> {
    tables: [Impl; BUCKETS],
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

    fn entry<'a>(&self, key: &'a Self::Key) -> Option<Self::EntryRef<'_>>
    where Self::Key: 'a {
        let hash = key.fast_hash();
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        self.tables[index].entry(key)
    }

    fn entry_mut<'a>(&mut self, key: &'a Self::Key) -> Option<Self::EntryMutRef<'_>>
    where Self::Key: 'a {
        let hash = key.fast_hash();
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        self.tables[index].entry_mut(key)
    }

    fn get<'a>(&self, key: &'a Self::Key) -> Option<&Self::Value>
    where Self::Key: 'a {
        let hash = key.fast_hash();
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        self.tables[index].get(key)
    }

    fn get_mut<'a>(&mut self, key: &'a Self::Key) -> Option<&mut Self::Value>
    where Self::Key: 'a {
        let hash = key.fast_hash();
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        self.tables[index].get_mut(key)
    }

    unsafe fn insert<'a>(
        &mut self,
        key: &'a Self::Key,
    ) -> Result<&mut MaybeUninit<Self::Value>, &mut Self::Value>
    where
        Self::Key: 'a,
    {
        let hash = key.fast_hash();
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        self.tables[index].insert(key)
    }

    unsafe fn insert_and_entry<'a>(
        &mut self,
        key: &'a Self::Key,
    ) -> Result<Self::EntryMutRef<'_>, Self::EntryMutRef<'_>>
    where
        Self::Key: 'a,
    {
        let hash = key.fast_hash();
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        self.tables[index].insert_and_entry(key)
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
