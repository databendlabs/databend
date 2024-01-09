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

use super::traits::Keyable;
use crate::hashjoin_hashtable::combine_header;
use crate::hashjoin_hashtable::early_filtering;
use crate::hashjoin_hashtable::hash_bits;
use crate::hashjoin_hashtable::new_header;
use crate::hashjoin_hashtable::remove_header_tag;
use crate::utils::BlockInfoIndex;
use crate::utils::Interval;
use crate::HashJoinHashtableLike;
use crate::RawEntry;
use crate::RowPtr;

// This hashtable is only used for target build merge into (both standalone and distributed mode).
// Advantages:
//      1. Reduces redundant I/O operations, enhancing performance.
//      2. Lowers the maintenance overhead of deduplicating row_id.(But in distributed design, we also need to give rowid)
//      3. Allows the scheduling of the subsequent mutation pipeline to be entirely allocated to not matched append operations.
// Disadvantages:
//      1. This solution is likely to be a one-time approach (especially if there are not matched insert operations involved),
// potentially leading to the target table being unsuitable for use as a build table in the future.
//      2. Requires a significant amount of memory to be efficient and currently does not support spill operations.
#[allow(unused)]
pub struct HashJoinBlockInfoHashTable<K: Keyable, A: Allocator + Clone = MmapAllocator> {
    pub(crate) pointers: Box<[u64], A>,
    pub(crate) atomic_pointers: *mut AtomicU64,
    pub(crate) hash_shift: usize,
    pub(crate) is_distributed: bool,
    // matched[idx] means
    pub(crate) matched: Box<[u64]>,
    pub(crate) block_info_index: BlockInfoIndex,
    pub(crate) phantom: PhantomData<K>,
}

unsafe impl<K: Keyable + Send, A: Allocator + Clone + Send> Send
    for HashJoinBlockInfoHashTable<K, A>
{
}

unsafe impl<K: Keyable + Sync, A: Allocator + Clone + Sync> Sync
    for HashJoinBlockInfoHashTable<K, A>
{
}

impl<K: Keyable, A: Allocator + Clone + Default> HashJoinBlockInfoHashTable<K, A> {
    pub fn with_build_row_num(
        row_num: usize,
        is_distributed: bool,
        block_info_index: BlockInfoIndex,
    ) -> Self {
        let capacity = std::cmp::max((row_num * 2).next_power_of_two(), 1 << 10);
        let mut hashtable = Self {
            pointers: unsafe {
                Box::new_zeroed_slice_in(capacity, Default::default()).assume_init()
            },
            atomic_pointers: std::ptr::null_mut(),
            hash_shift: (hash_bits() - capacity.trailing_zeros()) as usize,
            phantom: PhantomData,
            is_distributed,
            matched: unsafe { Box::new_zeroed_slice_in(row_num, Default::default()).assume_init() },
            block_info_index,
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

impl<K, A> HashJoinHashtableLike for HashJoinBlockInfoHashTable<K, A>
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

    // Using hashes to probe hash table and converting them in-place to pointers for memory reuse.
    fn early_filtering_probe(&self, hashes: &mut [u64], bitmap: Option<Bitmap>) -> usize {
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
                            if header != 0 && early_filtering(header, *hash) {
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
                    if header != 0 && early_filtering(header, *hash) {
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

    // Using hashes to probe hash table and converting them in-place to pointers for memory reuse.
    fn early_filtering_probe_with_selection(
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

    // for merge into block info hash table
    fn gather_partial_modified_block(&self) -> (Interval, u64) {
        unreachable!()
    }

    // for merge into block info hash table
    fn reduce_false_matched_for_conjuct(&mut self) {
        unreachable!()
    }
}
