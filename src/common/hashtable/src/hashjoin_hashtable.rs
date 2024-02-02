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

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_base::mem_allocator::MmapAllocator;

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

/// Hash join early filtering:
/// For each bucket in the hash table, its type is u64. We use the upper 16 bits to store a tag
/// for early filtering and the lower 48 bits to store a pointer to the RowEntry.  We construct
/// the tag during the finalize phase of the hash join, encoding it into the upper 16 bits. During
/// the probing phase, we can check the tag first. If the key is not found in the tag, we can avoid
/// reading the RowEntry, which can reduce random memory accesses.
pub const POINTER_BITS_SIZE: u64 = 48;
pub const TAG_BITS_SIZE: u64 = 64 - POINTER_BITS_SIZE;
pub const TAG_BITS_SIZE_MASK: u64 = TAG_BITS_SIZE - 1;
pub const POINTER_MASK: u64 = (1 << POINTER_BITS_SIZE) - 1;
pub const TAG_MASK: u64 = !POINTER_MASK;

/// Generate a tag using hash % 16.
#[inline(always)]
pub fn tag(hash: u64) -> u64 {
    1 << (POINTER_BITS_SIZE + (hash & TAG_BITS_SIZE_MASK))
}

/// Generate a tag and encode it into the upper 16 bits of bucket.
#[inline(always)]
pub fn new_header(ptr: u64, hash: u64) -> u64 {
    ptr | tag(hash)
}

/// Combine the tags of new_header and old_header.
#[inline(always)]
pub fn combine_header(new_header: u64, old_header: u64) -> u64 {
    new_header | (old_header & TAG_MASK)
}

/// Remove the tag from the bucket.
#[inline(always)]
pub fn remove_header_tag(old_header: u64) -> u64 {
    old_header & POINTER_MASK
}

/// Obtain the tag by performing a right shift operation on the bucket,
/// and then check if the key is in the tag.
#[inline(always)]
pub fn early_filtering(header: u64, hash: u64) -> bool {
    ((header >> POINTER_BITS_SIZE) & (1 << (hash & TAG_BITS_SIZE_MASK))) != 0
}

/// For SSE4.2, we use CRC32 to calculate the hash, and the hash type is u32.
#[inline(always)]
pub fn hash_bits() -> u32 {
    cfg_if::cfg_if! {
        if #[cfg(target_feature = "sse4.2")] { 32 } else { 64 }
    }
}

pub struct HashJoinHashTable<K: Keyable, A: Allocator + Clone = MmapAllocator> {
    pub(crate) pointers: Box<[u64], A>,
    pub(crate) atomic_pointers: *mut AtomicU64,
    pub(crate) hash_shift: usize,
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
            hash_shift: (hash_bits() - capacity.trailing_zeros()) as usize,
            phantom: PhantomData,
        };
        hashtable.atomic_pointers = unsafe {
            std::mem::transmute::<*mut u64, *mut AtomicU64>(hashtable.pointers.as_mut_ptr())
        };
        hashtable
    }

    pub fn insert(&mut self, key: K, entry_ptr: *mut RawEntry<K>) {
        let hash = key.hash();
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

impl<K, A> HashJoinHashtableLike for HashJoinHashTable<K, A>
where
    K: Keyable,
    A: Allocator + Clone + 'static,
{
    type Key = K;

    // Using hashes to probe hash table and converting them in-place to pointers for memory reuse.
    fn probe(&self, hashes: &mut [u64], bitmap: Option<Bitmap>) -> usize {
        let mut valids = None;
        if let Some(bitmap) = bitmap {
            if bitmap.unset_bits() == bitmap.len() {
                hashes.iter_mut().for_each(|hash| {
                    *hash = 0;
                });
                return 0;
            } else if bitmap.unset_bits() > 0 {
                valids = Some(bitmap);
            }
        }
        let mut count = 0;
        match valids {
            Some(valids) => {
                valids
                    .iter()
                    .zip(hashes.iter_mut())
                    .for_each(|(valid, hash)| {
                        if valid {
                            let header = self.pointers[(*hash >> self.hash_shift) as usize];
                            if header != 0 {
                                *hash = remove_header_tag(header);
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
            if bitmap.unset_bits() == bitmap.len() {
                unmatched_selection
                    .iter_mut()
                    .enumerate()
                    .for_each(|(idx, val)| {
                        *val = idx as u32;
                    });
                return (0, hashes.len());
            } else if bitmap.unset_bits() > 0 {
                valids = Some(bitmap);
            }
        }
        let mut matched_idx = 0;
        let mut unmatched_idx = 0;
        match valids {
            Some(valids) => {
                valids.iter().zip(hashes.iter_mut().enumerate()).for_each(
                    |(valid, (idx, hash))| {
                        if valid {
                            let header = self.pointers[(*hash >> self.hash_shift) as usize];
                            if header != 0 && early_filtering(header, *hash) {
                                *hash = remove_header_tag(header);
                                unsafe {
                                    *matched_selection.get_unchecked_mut(matched_idx) = idx as u32
                                };
                                matched_idx += 1;
                            } else {
                                unsafe {
                                    *unmatched_selection.get_unchecked_mut(unmatched_idx) =
                                        idx as u32
                                };
                                unmatched_idx += 1;
                            }
                        } else {
                            unsafe {
                                *unmatched_selection.get_unchecked_mut(unmatched_idx) = idx as u32
                            };
                            unmatched_idx += 1;
                        }
                    },
                );
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
            if bitmap.unset_bits() == bitmap.len() {
                return 0;
            } else if bitmap.unset_bits() > 0 {
                valids = Some(bitmap);
            }
        }
        let mut count = 0;
        match valids {
            Some(valids) => {
                valids.iter().zip(hashes.iter_mut().enumerate()).for_each(
                    |(valid, (idx, hash))| {
                        if valid {
                            let header = self.pointers[(*hash >> self.hash_shift) as usize];
                            if header != 0 && early_filtering(header, *hash) {
                                *hash = remove_header_tag(header);
                                unsafe { *selection.get_unchecked_mut(count) = idx as u32 };
                                count += 1;
                            }
                        }
                    },
                );
            }
            None => {
                hashes.iter_mut().enumerate().for_each(|(idx, hash)| {
                    let header = self.pointers[(*hash >> self.hash_shift) as usize];
                    if header != 0 && early_filtering(header, *hash) {
                        *hash = remove_header_tag(header);
                        unsafe { *selection.get_unchecked_mut(count) = idx as u32 };
                        count += 1;
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
