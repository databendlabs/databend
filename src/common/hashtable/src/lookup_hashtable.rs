use std::alloc::Allocator;
use std::mem::MaybeUninit;

use common_base::mem_allocator::GlobalAllocator;
use common_base::mem_allocator::MmapAllocator;

use crate::table0::Entry;
use crate::HashtableLike;

pub struct LookupHashtable<
    K: Sized,
    const CAPACITY: usize,
    V,
    A: Allocator + Clone = MmapAllocator<GlobalAllocator>,
> {
    flags: [bool; CAPACITY],
    data: Box<[Entry<K, V>; CAPACITY], A>,
    len: usize,
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
                flags: [false; CAPACITY],
                data: Box::<[Entry<K, V>; CAPACITY], A>::new_zeroed_in(allocator).assume_init(),
                len: 0,
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

            fn entry<'a>(&self, key: &'a $ty) -> Option<Self::EntryRef<'_>> where Self::Key: 'a {
                match self.flags[*key as usize] {
                    true => Some(&self.data[*key as usize]),
                    false => None,
                }
            }

            fn entry_mut<'a>(&mut self, key: &'a $ty) -> Option<Self::EntryMutRef<'_>> where Self::Key: 'a {
                match self.flags[*key as usize] {
                    true => Some(&mut self.data[*key as usize]),
                    false => None,
                }
            }

            fn get<'a>(&self, key: &'a $ty) -> Option<&Self::Value> where Self::Key: 'a {
                unsafe { self.entry(key).map(|e| e.val.assume_init_ref()) }
            }

            fn get_mut<'a>(&mut self, key: &'a $ty) -> Option<&mut Self::Value> where Self::Key: 'a {
                unsafe { self.entry_mut(key).map(|e| e.val.assume_init_mut()) }
            }

            unsafe fn insert<'a>(&mut self, key: &'a $ty) -> Result<&mut MaybeUninit<Self::Value>, &mut Self::Value> where Self::Key: 'a {
                match self.insert_and_entry(key) {
                    Ok(e) => Ok(&mut e.val),
                    Err(e) => Err(e.val.assume_init_mut()),
                }
            }

            unsafe fn insert_and_entry<'a>(&mut self, key: &'a $ty) -> Result<Self::EntryMutRef<'_>, Self::EntryMutRef<'_>> where Self::Key: 'a {
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

            fn iter(&self) -> Self::Iterator<'_> {
                LookupTableIter::create(&self.flags, &self.data)
            }

            fn iter_mut(&mut self) -> Self::IteratorMut<'_> {
                LookupTableIterMut::create(&self.flags, &mut self.data)
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
        }

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

// pub struct LookupHashtable<K, V, A = MmapAllocator<GlobalAllocator>> {
//     pub(crate) zero: ZeroEntry<K, V>,
//     pub(crate) table: Table0<K, V, HeapContainer<Entry<K, V>, A>, A>,
// }

// unsafe impl<K: Keyable + Send, V: Send, A: Allocator + Clone + Send> Send for Hashtable<K, V, A> {}
//
// unsafe impl<K, V: Sync, A: Allocator + Clone + Sync> Sync for Hashtable<K, V, A> {}
