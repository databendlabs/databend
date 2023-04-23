// Copyright 2023 Datafuse Labs.
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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use common_base::mem_allocator::MmapAllocator;

use super::traits::HashJoinHashtableLike;
use crate::traits::FastHash;
use crate::RowPtr;

pub struct StringRawEntry {
    pub row_ptr: RowPtr,
    pub length: u32,
    pub early: [u8; 4],
    pub key: *mut u8,
    pub next: u64,
    pub(crate) _alignment: [u64; 0],
}

pub struct HashJoinStringHashTable<A: Allocator + Clone = MmapAllocator> {
    pub(crate) pointers: Box<[u64], A>,
    pub(crate) atomic_pointers: *mut AtomicU64,
    mask: usize,
}

unsafe impl<A: Allocator + Clone + Send> Send for HashJoinStringHashTable<A> {}

unsafe impl<A: Allocator + Clone + Sync> Sync for HashJoinStringHashTable<A> {}

impl<A: Allocator + Clone + Default> HashJoinStringHashTable<A> {
    pub fn with_fixed_capacity(capacity: usize) -> Self {
        let mut hashtable = Self {
            pointers: unsafe {
                Box::new_zeroed_slice_in(capacity, Default::default()).assume_init()
            },
            atomic_pointers: std::ptr::null_mut(),
            mask: capacity - 1,
        };
        hashtable.atomic_pointers = unsafe {
            std::mem::transmute::<*mut u64, *mut AtomicU64>(hashtable.pointers.as_mut_ptr())
        };
        hashtable
    }

    pub fn insert(&mut self, key: &[u8], raw_entry: *mut StringRawEntry) {
        let index = (key.fast_hash() as usize) & self.mask;
        let mut head = unsafe { (*self.atomic_pointers.add(index)).load(Ordering::Relaxed) };
        loop {
            unsafe { (*raw_entry).next = head };
            let res = unsafe {
                (*self.atomic_pointers.add(index)).compare_exchange_weak(
                    head,
                    raw_entry as u64,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
            };
            if res.is_ok() {
                break;
            }
            head = unsafe { (*self.atomic_pointers.add(index)).load(Ordering::Relaxed) };
        }
    }
}

impl<A> HashJoinHashtableLike for HashJoinStringHashTable<A>
where A: Allocator + Clone + 'static
{
    type Key = [u8];

    fn contains(&self, key_ref: &Self::Key) -> bool {
        let index = (key_ref.fast_hash() as usize) & self.mask;
        let mut ptr = self.pointers[index];
        loop {
            if ptr == 0 {
                break;
            }
            let raw_entry = unsafe { &*(ptr as *mut StringRawEntry) };
            let min_len = std::cmp::min(4, std::cmp::min(key_ref.len(), raw_entry.length as usize));
            if raw_entry.length as usize == key_ref.len()
                && key_ref[0..min_len] == raw_entry.early[0..min_len]
            {
                let key = unsafe {
                    std::slice::from_raw_parts(
                        raw_entry.key as *const u8,
                        raw_entry.length as usize,
                    )
                };
                if key == key_ref {
                    return true;
                }
            }
            ptr = raw_entry.next;
        }
        false
    }

    fn probe_hash_table(
        &self,
        key_ref: &Self::Key,
        vec_ptr: *mut RowPtr,
        mut occupied: usize,
        capacity: usize,
    ) -> (usize, u64) {
        let index = (key_ref.fast_hash() as usize) & self.mask;
        let origin = occupied;
        let mut ptr = self.pointers[index];
        loop {
            if ptr == 0 || occupied >= capacity {
                break;
            }
            let raw_entry = unsafe { &*(ptr as *mut StringRawEntry) };
            let min_len = std::cmp::min(4, key_ref.len());
            if raw_entry.length as usize == key_ref.len()
                && key_ref[0..min_len] == raw_entry.early[0..min_len]
            {
                let key = unsafe {
                    std::slice::from_raw_parts(
                        raw_entry.key as *const u8,
                        raw_entry.length as usize,
                    )
                };
                if key == key_ref {
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            &raw_entry.row_ptr as *const RowPtr,
                            vec_ptr.add(occupied),
                            1,
                        )
                    };
                    occupied += 1;
                }
            }
            ptr = raw_entry.next;
        }
        if occupied > origin {
            (occupied - origin, ptr)
        } else {
            (0, 0)
        }
    }

    fn next_incomplete_ptr(
        &self,
        key_ref: &Self::Key,
        mut incomplete_ptr: u64,
        vec_ptr: *mut RowPtr,
        mut occupied: usize,
        capacity: usize,
    ) -> (usize, u64) {
        let origin = occupied;
        loop {
            if incomplete_ptr == 0 || occupied >= capacity {
                break;
            }
            let raw_entry = unsafe { &*(incomplete_ptr as *mut StringRawEntry) };
            let min_len = std::cmp::min(4, key_ref.len());
            if raw_entry.length as usize == key_ref.len()
                && key_ref[0..min_len] == raw_entry.early[0..min_len]
            {
                let key = unsafe {
                    std::slice::from_raw_parts(
                        raw_entry.key as *const u8,
                        raw_entry.length as usize,
                    )
                };
                if key == key_ref {
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            &raw_entry.row_ptr as *const RowPtr,
                            vec_ptr.add(occupied),
                            1,
                        )
                    };
                    occupied += 1;
                }
            }
            incomplete_ptr = raw_entry.next;
        }
        if occupied > origin {
            (occupied - origin, incomplete_ptr)
        } else {
            (0, 0)
        }
    }
}
