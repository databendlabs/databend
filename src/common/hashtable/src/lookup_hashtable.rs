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
use std::mem;
use std::mem::MaybeUninit;

use databend_common_base::mem_allocator::MmapAllocator;

use crate::table0::Entry;
use crate::HashtableLike;

pub struct LookupHashtable<K: Sized, const CAPACITY: usize, V, A: Allocator + Clone = MmapAllocator>
{
    flags: Box<[bool; CAPACITY], A>,
    data: Box<[Entry<K, V>; CAPACITY], A>,
    len: usize,
    allocator: A,
}

pub struct LookupTableIter<'a, const CAPACITY: usize, K, V> {
    flags: &'a [bool; CAPACITY],
    slice: &'a [Entry<K, V>; CAPACITY],
    i: usize,
}

pub struct LookupTableIterMut<'a, const CAPACITY: usize, K, V> {
    flags: &'a [bool; CAPACITY],
    slice: &'a mut [Entry<K, V>; CAPACITY],
    i: usize,
}

impl<'a, const CAPACITY: usize, K: Sized, V> LookupTableIter<'a, CAPACITY, K, V> {
    pub fn create(flags: &'a [bool; CAPACITY], slice: &'a [Entry<K, V>; CAPACITY]) -> Self {
        LookupTableIter::<'a, CAPACITY, K, V> { i: 0, flags, slice }
    }
}

impl<'a, const CAPACITY: usize, K: Sized, V> LookupTableIterMut<'a, CAPACITY, K, V> {
    pub fn create(flags: &'a [bool; CAPACITY], slice: &'a mut [Entry<K, V>; CAPACITY]) -> Self {
        LookupTableIterMut::<'a, CAPACITY, K, V> { i: 0, flags, slice }
    }
}

impl<K: Sized, const CAPACITY: usize, V, A: Allocator + Clone> LookupHashtable<K, CAPACITY, V, A> {
    pub fn create(allocator: A) -> LookupHashtable<K, CAPACITY, V, A> {
        unsafe {
            LookupHashtable::<K, CAPACITY, V, A> {
                flags: Box::<[bool; CAPACITY], A>::new_zeroed_in(allocator.clone()).assume_init(),
                data: Box::<[Entry<K, V>; CAPACITY], A>::new_zeroed_in(allocator.clone())
                    .assume_init(),
                len: 0,
                allocator,
            }
        }
    }
}

macro_rules! lookup_impl {
    ($ty:ident, $capacity:expr) => {
        impl<V, A: Allocator + Clone> HashtableLike for LookupHashtable<$ty, $capacity, V, A> {
            type Key = $ty;
            type Value = V;
            type EntryRef<'a> = &'a Entry<$ty, V> where Self: 'a, Self::Key: 'a, Self::Value: 'a;
            type EntryMutRef<'a> = &'a mut Entry<$ty, V> where Self: 'a, Self::Key:'a, Self::Value: 'a;
            type Iterator<'a> = LookupTableIter<'a, $capacity, $ty, V> where Self: 'a, Self::Key: 'a, Self::Value: 'a;
            type IteratorMut<'a> = LookupTableIterMut<'a, $capacity, $ty, V> where Self: 'a, Self::Key: 'a, Self::Value: 'a;

            fn len(&self) -> usize {
                self.len
            }

            fn bytes_len(&self, _without_arena: bool) -> usize {
                mem::size_of::<MaybeUninit<[bool; $capacity]>>() +
                mem::size_of::<MaybeUninit<[Entry<$ty, V>; $capacity]>>() +
                mem::size_of::<Self>()
            }

            fn entry(&self, key: &$ty) -> Option<Self::EntryRef<'_>> {
                match self.flags[*key as usize] {
                    true => Some(&self.data[*key as usize]),
                    false => None,
                }
            }

            fn entry_mut(&mut self, key: &$ty) -> Option<Self::EntryMutRef<'_>> {
                match self.flags[*key as usize] {
                    true => Some(&mut self.data[*key as usize]),
                    false => None,
                }
            }

            fn get(&self, key: &$ty) -> Option<&Self::Value> {
                unsafe { self.entry(key).map(|e| e.val.assume_init_ref()) }
            }

            fn get_mut(&mut self, key: &$ty) -> Option<&mut Self::Value> {
                unsafe { self.entry_mut(key).map(|e| e.val.assume_init_mut()) }
            }

            unsafe fn insert(&mut self, key: &$ty) -> Result<&mut MaybeUninit<Self::Value>, &mut Self::Value> {
                match self.insert_and_entry(key) {
                    Ok(e) => Ok(&mut e.val),
                    Err(e) => Err(e.val.assume_init_mut()),
                }
            }

            unsafe fn insert_and_entry(&mut self, key: &$ty) -> Result<Self::EntryMutRef<'_>, Self::EntryMutRef<'_>> {
                match self.flags[*key as usize] {
                    true => Err(&mut self.data[*key as usize]),
                    false => {
                        self.flags[*key as usize] = true;
                        let e = &mut self.data[*key as usize];
                        self.len += 1;
                        e.key.write(*key);
                        Ok(e)
                    }
                }
            }

            unsafe fn insert_and_entry_with_hash(&mut self, key: &$ty, _hash: u64) -> Result<Self::EntryMutRef<'_>, Self::EntryMutRef<'_>> {
                self.insert_and_entry(key)
            }

            fn iter(&self) -> Self::Iterator<'_> {
                LookupTableIter::create(&self.flags, &self.data)
            }

            fn clear(&mut self) {
                unsafe {
                    self.len = 0;
                    self.flags = Box::<[bool; $capacity], A>::new_zeroed_in(self.allocator.clone()).assume_init();
                    self.data = Box::<[Entry<$ty, V>; $capacity], A>::new_zeroed_in(self.allocator.clone()).assume_init();
                }
            }
        }

        impl<'a, V> Iterator for LookupTableIter<'a, $capacity, $ty, V> {
            type Item = &'a Entry<$ty, V>;

            fn next(&mut self) -> Option<Self::Item> {
                while self.i < $capacity && !self.flags[self.i] {
                    self.i += 1;
                }

                if self.i == $capacity {
                    None
                } else {
                    let res = unsafe { &*(self.slice.as_ptr().add(self.i)) };
                    self.i += 1;
                    Some(res)
                }
            }

            fn size_hint(&self) -> (usize, Option<usize>) {
                let remaining = $capacity - self.i;
                (remaining, Some(remaining))
            }
        }

        unsafe impl<'a, V> TrustedLen for LookupTableIter<'a, $capacity, $ty, V> {}

        impl<'a, V> Iterator for LookupTableIterMut<'a, $capacity, $ty, V> {
            type Item = &'a mut Entry<$ty, V>;

            fn next(&mut self) -> Option<Self::Item> {
                while self.i < $capacity && !self.flags[self.i] {
                    self.i += 1;
                }

                if self.i == $capacity {
                    None
                } else {
                    let res = unsafe { &mut *(self.slice.as_ptr().add(self.i) as *mut _) };
                    self.i += 1;
                    Some(res)
                }
            }
        }
    };
}

lookup_impl!(u8, 256);
lookup_impl!(u16, 65536);
