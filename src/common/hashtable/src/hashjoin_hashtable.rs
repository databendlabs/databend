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
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use common_base::mem_allocator::MmapAllocator;

use super::traits::HashJoinHashtableLike;
use super::traits::Keyable;

#[derive(Clone, Copy, Debug)]
pub struct RowPtr {
    pub chunk_index: u32,
    pub row_index: u32,
}

impl RowPtr {
    pub fn new(chunk_index: u32, row_index: u32) -> Self {
        RowPtr {
            chunk_index,
            row_index,
        }
    }
}

impl PartialEq for RowPtr {
    fn eq(&self, other: &Self) -> bool {
        self.chunk_index == other.chunk_index && self.row_index == other.row_index
    }
}

pub struct RawEntry<K> {
    pub row_ptr: RowPtr,
    pub key: K,
    pub next: u64,
}

pub struct HashJoinHashTable<K: Keyable, A: Allocator + Clone = MmapAllocator> {
    pub(crate) pointers: Box<[u64], A>,
    pub(crate) atomic_pointers: *mut AtomicU64,
    pub(crate) hash_mask: u64,
    pub(crate) phantom: PhantomData<K>,
}

unsafe impl<K: Keyable + Send, A: Allocator + Clone + Send> Send for HashJoinHashTable<K, A> {}

unsafe impl<K: Keyable + Sync, A: Allocator + Clone + Sync> Sync for HashJoinHashTable<K, A> {}

impl<K: Keyable, A: Allocator + Clone + Default> HashJoinHashTable<K, A> {
    pub fn with_build_row_num(row_num: usize) -> Self {
        let capacity = std::cmp::max((row_num * 2).next_power_of_two(), 1 << 10);
        let mut hashtable = Self {
            pointers: unsafe {
                Box::new_zeroed_slice_in(capacity, Default::default()).assume_init()
            },
            atomic_pointers: std::ptr::null_mut(),
            hash_mask: capacity as u64 - 1,
            phantom: PhantomData,
        };
        hashtable.atomic_pointers = unsafe {
            std::mem::transmute::<*mut u64, *mut AtomicU64>(hashtable.pointers.as_mut_ptr())
        };
        hashtable
    }

    pub fn insert(&mut self, key: K, raw_entry_ptr: *mut RawEntry<K>) {
        let index = (key.hash() & self.hash_mask) as usize;
        // # Safety
        // `index` is less than the capacity of hash table.
        let mut head = unsafe { (*self.atomic_pointers.add(index)).load(Ordering::Relaxed) };
        loop {
            let res = unsafe {
                (*self.atomic_pointers.add(index)).compare_exchange_weak(
                    head,
                    raw_entry_ptr as u64,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
            };
            match res {
                Ok(_) => break,
                Err(x) => head = x,
            };
        }
        unsafe { (*raw_entry_ptr).next = head };
    }
}

impl<K, A> HashJoinHashtableLike for HashJoinHashTable<K, A>
where
    K: Keyable,
    A: Allocator + Clone + 'static,
{
    type Key = K;

    // Using hashes to probe hash table and converting them in-place to pointers for memory reuse.
    fn probe(&self, hashes: &mut [u64]) {
        hashes
            .iter_mut()
            .for_each(|hash| *hash = self.pointers[(*hash & self.hash_mask) as usize]);
    }

    fn next_contains(&self, key: &Self::Key, mut ptr: u64) -> bool {
        loop {
            if ptr == 0 {
                break;
            }
            let raw_entry = unsafe { &*(ptr as *mut RawEntry<K>) };
            if key == &raw_entry.key {
                return true;
            }
            ptr = raw_entry.next;
        }
        false
    }

    fn next_probe(
        &self,
        key: &Self::Key,
        mut ptr: u64,
        vec_ptr: *mut RowPtr,
        mut occupied: usize,
        capacity: usize,
    ) -> (usize, u64) {
        let origin = occupied;
        loop {
            if ptr == 0 || occupied >= capacity {
                break;
            }
            let raw_entry = unsafe { &*(ptr as *mut RawEntry<K>) };
            if key == &raw_entry.key {
                // # Safety
                // occupied is less than the capacity of vec_ptr.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        &raw_entry.row_ptr as *const RowPtr,
                        vec_ptr.add(occupied),
                        1,
                    )
                };
                occupied += 1;
            }
            ptr = raw_entry.next;
        }
        if occupied > origin {
            (occupied - origin, ptr)
        } else {
            (0, 0)
        }
    }
}
