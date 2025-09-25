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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use databend_common_base::mem_allocator::DefaultAllocator;
use databend_common_column::bitmap::Bitmap;

use super::traits::HashJoinHashtableLike;
use crate::hashjoin_hashtable::combine_header;
use crate::hashjoin_hashtable::early_filtering;
use crate::hashjoin_hashtable::hash_bits;
use crate::hashjoin_hashtable::new_header;
use crate::hashjoin_hashtable::remove_header_tag;
use crate::traits::hash_join_fast_string_hash;
use crate::RowPtr;

pub const STRING_EARLY_SIZE: usize = 4;
pub struct StringRawEntry {
    pub row_ptr: RowPtr,
    pub length: u32,
    pub early: [u8; STRING_EARLY_SIZE],
    pub key: *mut u8,
    pub next: u64,
}

pub struct HashJoinStringHashTable<A: Allocator + Clone = DefaultAllocator> {
    pub(crate) pointers: Box<[u64], A>,
    pub(crate) atomic_pointers: *mut AtomicU64,
    pub(crate) hash_shift: usize,
}

unsafe impl<A: Allocator + Clone + Send> Send for HashJoinStringHashTable<A> {}

unsafe impl<A: Allocator + Clone + Sync> Sync for HashJoinStringHashTable<A> {}

impl<A: Allocator + Clone + Default> HashJoinStringHashTable<A> {
    pub fn with_build_row_num(row_num: usize) -> Self {
        let capacity = std::cmp::max((row_num * 2).next_power_of_two(), 1 << 10);
        let mut hashtable = Self {
            pointers: unsafe {
                Box::new_zeroed_slice_in(capacity, Default::default()).assume_init()
            },
            atomic_pointers: std::ptr::null_mut(),
            hash_shift: (hash_bits() - capacity.trailing_zeros()) as usize,
        };
        hashtable.atomic_pointers = unsafe {
            std::mem::transmute::<*mut u64, *mut AtomicU64>(hashtable.pointers.as_mut_ptr())
        };
        hashtable
    }

    pub fn insert(&self, key: &[u8], entry_ptr: *mut StringRawEntry) {
        let hash = hash_join_fast_string_hash(key);
        let index = (hash >> self.hash_shift) as usize;
        let new_header = new_header(entry_ptr as u64, hash);
        // # Safety
        // `index` is less than the capacity of hash table.
        let mut old_header = unsafe { (*self.atomic_pointers.add(index)).load(Ordering::Relaxed) };
        loop {
            let res = unsafe {
                (*self.atomic_pointers.add(index)).compare_exchange_weak(
                    old_header,
                    combine_header(new_header, old_header),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
            };
            match res {
                Ok(_) => break,
                Err(x) => old_header = x,
            };
        }
        unsafe { (*entry_ptr).next = remove_header_tag(old_header) };
    }
}

impl<A> HashJoinHashtableLike for HashJoinStringHashTable<A>
where A: Allocator + Clone + 'static
{
    type Key = [u8];

    // Using hashes to probe hash table and converting them in-place to pointers for memory reuse.
    fn probe(&self, hashes: &mut [u64], bitmap: Option<Bitmap>) -> usize {
        let mut valids = None;
        if let Some(bitmap) = bitmap {
            if bitmap.null_count() == bitmap.len() {
                hashes.iter_mut().for_each(|hash| {
                    *hash = 0;
                });
                return 0;
            } else if bitmap.null_count() > 0 {
                valids = Some(bitmap);
            }
        }
        let mut count = 0;
        match valids {
            Some(valids) => {
                hashes.iter_mut().enumerate().for_each(|(idx, hash)| {
                    if unsafe { valids.get_bit_unchecked(idx) } {
                        let header = self.pointers[(*hash >> self.hash_shift) as usize];
                        if header != 0 {
                            *hash = remove_header_tag(header);
                            count += 1;
                        } else {
                            *hash = 0;
                        }
                    } else {
                        *hash = 0;
                    };
                });
            }
            None => {
                hashes.iter_mut().for_each(|hash| {
                    let header = self.pointers[(*hash >> self.hash_shift) as usize];
                    if header != 0 {
                        *hash = remove_header_tag(header);
                        count += 1;
                    } else {
                        *hash = 0;
                    }
                });
            }
        }
        count
    }

    // Perform early filtering probe, store matched indexes in `matched_selection` and store unmatched indexes
    // in `unmatched_selection`, return the number of matched and unmatched indexes.
    fn early_filtering_probe(
        &self,
        hashes: &mut [u64],
        bitmap: Option<Bitmap>,
        matched_selection: &mut [u32],
        unmatched_selection: &mut [u32],
    ) -> (usize, usize) {
        let mut valids = None;
        if let Some(bitmap) = bitmap {
            if bitmap.null_count() == bitmap.len() {
                unmatched_selection
                    .iter_mut()
                    .enumerate()
                    .for_each(|(idx, val)| {
                        *val = idx as u32;
                    });
                return (0, hashes.len());
            } else if bitmap.null_count() > 0 {
                valids = Some(bitmap);
            }
        }
        let mut matched_idx = 0;
        let mut unmatched_idx = 0;
        match valids {
            Some(valids) => {
                hashes.iter_mut().enumerate().for_each(|(idx, hash)| {
                    if unsafe { valids.get_bit_unchecked(idx) } {
                        let header = self.pointers[(*hash >> self.hash_shift) as usize];
                        if header != 0 && early_filtering(header, *hash) {
                            *hash = remove_header_tag(header);
                            unsafe {
                                *matched_selection.get_unchecked_mut(matched_idx) = idx as u32
                            };
                            matched_idx += 1;
                        } else {
                            unsafe {
                                *unmatched_selection.get_unchecked_mut(unmatched_idx) = idx as u32
                            };
                            unmatched_idx += 1;
                        }
                    } else {
                        unsafe {
                            *unmatched_selection.get_unchecked_mut(unmatched_idx) = idx as u32
                        };
                        unmatched_idx += 1;
                    }
                });
            }
            None => {
                hashes.iter_mut().enumerate().for_each(|(idx, hash)| {
                    let header = self.pointers[(*hash >> self.hash_shift) as usize];
                    if header != 0 && early_filtering(header, *hash) {
                        *hash = remove_header_tag(header);
                        unsafe { *matched_selection.get_unchecked_mut(matched_idx) = idx as u32 };
                        matched_idx += 1;
                    } else {
                        unsafe {
                            *unmatched_selection.get_unchecked_mut(unmatched_idx) = idx as u32
                        };
                        unmatched_idx += 1;
                    }
                });
            }
        }
        (matched_idx, unmatched_idx)
    }

    // Perform early filtering probe and store matched indexes in `selection`, return the number of matched indexes.
    fn early_filtering_matched_probe(
        &self,
        hashes: &mut [u64],
        bitmap: Option<Bitmap>,
        selection: &mut [u32],
    ) -> usize {
        let mut valids = None;
        if let Some(bitmap) = bitmap {
            if bitmap.null_count() == bitmap.len() {
                hashes.iter_mut().for_each(|hash| {
                    *hash = 0;
                });
                return 0;
            } else if bitmap.null_count() > 0 {
                valids = Some(bitmap);
            }
        }
        let mut count = 0;
        match valids {
            Some(valids) => {
                hashes.iter_mut().enumerate().for_each(|(idx, hash)| {
                    if unsafe { valids.get_bit_unchecked(idx) } {
                        let header = self.pointers[(*hash >> self.hash_shift) as usize];
                        if header != 0 && early_filtering(header, *hash) {
                            *hash = remove_header_tag(header);
                            unsafe { *selection.get_unchecked_mut(count) = idx as u32 };
                            count += 1;
                        } else {
                            *hash = 0;
                        }
                    } else {
                        *hash = 0;
                    }
                });
            }
            None => {
                hashes.iter_mut().enumerate().for_each(|(idx, hash)| {
                    let header = self.pointers[(*hash >> self.hash_shift) as usize];
                    if header != 0 && early_filtering(header, *hash) {
                        *hash = remove_header_tag(header);
                        unsafe { *selection.get_unchecked_mut(count) = idx as u32 };
                        count += 1;
                    } else {
                        *hash = 0;
                    }
                });
            }
        }
        count
    }

    fn next_contains(&self, key: &Self::Key, mut ptr: u64) -> bool {
        loop {
            if ptr == 0 {
                break;
            }
            let raw_entry = unsafe { &*(ptr as *mut StringRawEntry) };
            // Compare `early` and the length of the string, the size of `early` is 4.
            let min_len = std::cmp::min(
                STRING_EARLY_SIZE,
                std::cmp::min(key.len(), raw_entry.length as usize),
            );
            if raw_entry.length as usize == key.len()
                && key[0..min_len] == raw_entry.early[0..min_len]
            {
                let key_ref = unsafe {
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
            let raw_entry = unsafe { &*(ptr as *mut StringRawEntry) };
            // Compare `early` and the length of the string, the size of `early` is 4.
            let min_len = std::cmp::min(STRING_EARLY_SIZE, key.len());
            if raw_entry.length as usize == key.len()
                && key[0..min_len] == raw_entry.early[0..min_len]
            {
                let key_ref = unsafe {
                    std::slice::from_raw_parts(
                        raw_entry.key as *const u8,
                        raw_entry.length as usize,
                    )
                };
                if key == key_ref {
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
            }
            ptr = raw_entry.next;
        }
        if occupied > origin {
            (occupied - origin, ptr)
        } else {
            (0, 0)
        }
    }

    fn next_matched_ptr(&self, key: &Self::Key, mut ptr: u64) -> u64 {
        loop {
            if ptr == 0 {
                break;
            }
            let raw_entry = unsafe { &*(ptr as *mut StringRawEntry) };
            // Compare `early` and the length of the string, the size of `early` is 4.
            let min_len = std::cmp::min(
                STRING_EARLY_SIZE,
                std::cmp::min(key.len(), raw_entry.length as usize),
            );
            if raw_entry.length as usize == key.len()
                && key[0..min_len] == raw_entry.early[0..min_len]
            {
                let key_ref = unsafe {
                    std::slice::from_raw_parts(
                        raw_entry.key as *const u8,
                        raw_entry.length as usize,
                    )
                };
                if key == key_ref {
                    return ptr;
                }
            }
            ptr = raw_entry.next;
        }
        0
    }
}
