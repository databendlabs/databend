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

use std::sync::atomic::Ordering;

use databend_common_base::hints::assume;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FixedKey;
use databend_common_expression::HashMethod;
use databend_common_expression::HashMethodFixedKeys;
use databend_common_expression::KeyAccessor;
use databend_common_expression::ProjectedBlock;
use databend_common_hashtable::HashJoinHashMap;
use databend_common_hashtable::HashtableKeyable;
use databend_common_hashtable::RawEntry;
use databend_common_hashtable::RowPtr;

use crate::pipelines::processors::transforms::FixedKeyHashJoinHashTable;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::ProbeData;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::AllUnmatchedProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::EmptyProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbedRows;

/// Pointer bits size (lower 48 bits for pointer).
const POINTER_BITS_SIZE: u64 = 48;
/// Mask for extracting pointer (lower 48 bits).
const POINTER_MASK: u64 = (1 << POINTER_BITS_SIZE) - 1;
/// Mask for extracting tag/salt (upper 16 bits).
const TAG_MASK: u64 = !POINTER_MASK;

/// Extract salt from hash (upper 16 bits, with lower 48 bits set to 1s).
#[inline(always)]
fn extract_salt(hash: u64) -> u64 {
    hash | POINTER_MASK
}

/// Get salt (just the upper 16 bits).
#[inline(always)]
fn get_salt(header: u64) -> u64 {
    header & TAG_MASK
}

/// Create a new entry with salt and pointer.
#[inline(always)]
fn new_entry_with_salt(salt: u64, ptr: u64) -> u64 {
    ptr | (salt & TAG_MASK)
}

/// Remove the tag from the bucket.
#[inline(always)]
fn remove_header_tag(old_header: u64) -> u64 {
    old_header & POINTER_MASK
}

/// Increment slot and wrap around using capacity mask.
#[inline(always)]
fn increment_slot(slot: usize, capacity_mask: usize) -> usize {
    (slot + 1) & capacity_mask
}

impl<T: HashtableKeyable + FixedKey, const UNIQUE: bool> FixedKeyHashJoinHashTable<T, UNIQUE> {
    pub fn new(
        hash_table: HashJoinHashMap<T, UNIQUE>,
        hash_method: HashMethodFixedKeys<T>,
    ) -> Self {
        FixedKeyHashJoinHashTable::<T, UNIQUE> {
            hash_table,
            hash_method,
        }
    }

    /// Insert using open addressing with linear probing.
    pub fn insert(&self, keys: DataBlock, chunk: usize, arena: &mut Vec<u8>) -> Result<()> {
        let num_rows = keys.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        let keys = ProjectedBlock::from(keys.columns());
        let keys_state = self.hash_method.build_keys_state(keys, num_rows)?;

        // Compute all hashes in batch for cache efficiency
        let mut hashes = Vec::with_capacity(num_rows);
        self.hash_method.build_keys_hashes(&keys_state, &mut hashes);

        let build_keys_iter = self.hash_method.build_keys_iter(&keys_state)?;

        let entry_size = std::mem::size_of::<RawEntry<T>>();
        arena.reserve(num_rows * entry_size);

        let mut raw_entry_ptr =
            unsafe { std::mem::transmute::<*mut u8, *mut RawEntry<T>>(arena.as_mut_ptr()) };

        let atomic_pointers = self.hash_table.atomic_pointers();
        let capacity_mask = self.hash_table.capacity() - 1;
        let hash_shift = self.hash_table.hash_shift();

        // Insert entries using linear probing (compute salt and slot inline)
        for (row_index, key) in build_keys_iter.enumerate() {
            let row_ptr = RowPtr {
                chunk_index: chunk as u32,
                row_index: row_index as u32,
            };

            unsafe {
                *raw_entry_ptr = RawEntry {
                    row_ptr,
                    key: *key,
                    next: 0,
                }
            }

            let hash = hashes[row_index];
            let salt = extract_salt(hash) & TAG_MASK;
            let new_entry = new_entry_with_salt(salt, raw_entry_ptr as u64);

            let mut expected = 0;
            let mut slot = (hash >> hash_shift) as usize;
            let mut atomic_ptr = unsafe { &*atomic_pointers.add(slot) };

            loop {
                // Try to insert into empty slot directly (optimistic)
                match atomic_ptr.compare_exchange_weak(
                    expected,
                    new_entry,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Ok(old_value) => unsafe {
                        (*raw_entry_ptr).next = remove_header_tag(old_value);
                        break;
                    },
                    Err(new_expected) => {
                        // Slot is occupied, check if we can chain (same key)
                        let current_salt = get_salt(new_expected);
                        if current_salt == salt {
                            let existing_ptr = remove_header_tag(new_expected);
                            let existing_entry = unsafe { &*(existing_ptr as *mut RawEntry<T>) };

                            if *key == existing_entry.key {
                                expected = new_expected;
                                continue;
                            }
                        }

                        // Different key or different salt, linear probe to next slot
                        expected = 0;
                        slot = increment_slot(slot, capacity_mask);
                        atomic_ptr = unsafe { &*atomic_pointers.add(slot) };
                    }
                }
            }

            raw_entry_ptr = unsafe { raw_entry_ptr.add(1) };
        }

        Ok(())
    }

    /// Batch probe using open addressing with linear probing.
    fn probe_open_addressing_batch(
        &self,
        keys: &dyn KeyAccessor<Key = T>,
        hashes: &mut [u64],
        bitmap: Option<databend_common_column::bitmap::Bitmap>,
    ) -> usize {
        if hashes.is_empty() {
            return 0;
        }

        let capacity_mask = self.hash_table.capacity() - 1;
        let hash_shift = self.hash_table.hash_shift();
        let pointers = self.hash_table.pointers();

        // Handle all-null case
        if let Some(ref valids) = bitmap {
            if valids.null_count() == valids.len() {
                hashes.iter_mut().for_each(|hash| *hash = 0);
                return 0;
            }
        }

        // Compute salts and slots in batch
        let mut salts: Vec<u64> = Vec::with_capacity(hashes.len());
        let mut slots: Vec<usize> = Vec::with_capacity(hashes.len());

        assume(salts.capacity() >= hashes.len());
        assume(slots.capacity() >= hashes.len());

        for hash in hashes.iter() {
            salts.push(extract_salt(*hash) & TAG_MASK);
            slots.push((*hash >> hash_shift) as usize);
        }

        // First-round probe - check initial slots in batch
        // Store idx only for keys that need further probing
        let mut need_continue: Vec<usize> = Vec::new();
        let mut count = 0;

        match bitmap {
            Some(valids) if valids.null_count() > 0 => {
                for (idx, valid) in valids.iter().enumerate() {
                    if !valid {
                        hashes[idx] = 0;
                        continue;
                    }

                    let slot = slots[idx];
                    let current = pointers[slot];

                    if current == 0 {
                        hashes[idx] = 0;
                        continue;
                    }

                    let current_salt = get_salt(current);
                    if current_salt == salts[idx] {
                        let entry_ptr = remove_header_tag(current);
                        let entry = unsafe { &*(entry_ptr as *mut RawEntry<T>) };
                        let key = unsafe { keys.key_unchecked(idx) };

                        if key == &entry.key {
                            hashes[idx] = entry_ptr;
                            count += 1;
                            continue;
                        }
                    }

                    slots[idx] = increment_slot(slot, capacity_mask);
                    need_continue.push(idx);
                }
            }
            _ => {
                for idx in 0..hashes.len() {
                    let slot = slots[idx];
                    let current = pointers[slot];

                    if current == 0 {
                        hashes[idx] = 0;
                        continue;
                    }

                    let current_salt = get_salt(current);
                    if current_salt == salts[idx] {
                        let entry_ptr = remove_header_tag(current);
                        let entry = unsafe { &*(entry_ptr as *mut RawEntry<T>) };
                        let key = unsafe { keys.key_unchecked(idx) };

                        if key == &entry.key {
                            hashes[idx] = entry_ptr;
                            count += 1;
                            continue;
                        }
                    }

                    slots[idx] = increment_slot(slot, capacity_mask);
                    need_continue.push(idx);
                }
            }
        }

        // Handle collisions - continue probing for remaining keys
        // Use in-place modification to avoid allocating new Vec each iteration
        while !need_continue.is_empty() {
            let mut write_idx = 0;

            for read_idx in 0..need_continue.len() {
                let idx = need_continue[read_idx];
                let slot = slots[idx];
                let current = pointers[slot];

                if current == 0 {
                    hashes[idx] = 0;
                    continue;
                }

                let current_salt = get_salt(current);
                if current_salt == salts[idx] {
                    let entry_ptr = remove_header_tag(current);
                    let entry = unsafe { &*(entry_ptr as *mut RawEntry<T>) };
                    let key = unsafe { keys.key_unchecked(idx) };

                    if key == &entry.key {
                        hashes[idx] = entry_ptr;
                        count += 1;
                        continue;
                    }
                }

                // Still need to continue
                slots[idx] = increment_slot(slot, capacity_mask);
                need_continue[write_idx] = idx;
                write_idx += 1;
            }

            need_continue.truncate(write_idx);
        }

        count
    }

    pub fn probe_matched<'a>(&'a self, data: ProbeData<'a>) -> Result<Box<dyn ProbeStream + 'a>> {
        let num_rows = data.num_rows();
        let hash_method = &self.hash_method;
        let mut hashes = Vec::with_capacity(num_rows);

        let keys = ProjectedBlock::from(data.columns());
        let keys_state = hash_method.build_keys_state(keys, num_rows)?;
        hash_method.build_keys_hashes(&keys_state, &mut hashes);
        let keys = hash_method.build_keys_accessor(keys_state.clone())?;

        let probed_rows = data.non_null_rows();
        let (_, valids, ctx) = data.into_raw();

        // Probe using open addressing with linear probing
        let matched_rows = self.probe_open_addressing_batch(keys.as_ref(), &mut hashes, valids);

        ctx.probed_rows += probed_rows;
        ctx.matched_rows += matched_rows;

        match matched_rows {
            0 => Ok(Box::new(EmptyProbeStream)),
            _ => Ok(OpenAddressingProbeStream::<T, true, UNIQUE>::create(hashes)),
        }
    }

    pub fn probe<'a>(&'a self, data: ProbeData<'a>) -> Result<Box<dyn ProbeStream + 'a>> {
        let num_rows = data.num_rows();
        let hash_method = &self.hash_method;
        let mut hashes = Vec::with_capacity(num_rows);

        let keys = ProjectedBlock::from(data.columns());
        let keys_state = hash_method.build_keys_state(keys, num_rows)?;
        hash_method.build_keys_hashes(&keys_state, &mut hashes);
        let keys = hash_method.build_keys_accessor(keys_state.clone())?;

        let probed_rows = data.non_null_rows();
        let (_, valids, ctx) = data.into_raw();

        // Probe using open addressing with linear probing
        let matched_rows = self.probe_open_addressing_batch(keys.as_ref(), &mut hashes, valids);

        ctx.probed_rows += probed_rows;
        ctx.matched_rows += matched_rows;

        match matched_rows {
            0 => Ok(AllUnmatchedProbeStream::create(hashes.len())),
            _ => Ok(OpenAddressingProbeStream::<T, false, UNIQUE>::create(
                hashes,
            )),
        }
    }
}

struct OpenAddressingProbeStream<
    Key: HashtableKeyable + 'static,
    const MATCHED: bool,
    const MATCH_FIRST: bool,
> {
    key_idx: usize,
    pointers: Vec<u64>,
    probe_entry_ptr: u64,
    _phantom: std::marker::PhantomData<Key>,
}

impl<Key: HashtableKeyable + 'static, const MATCHED: bool, const MATCH_FIRST: bool>
    OpenAddressingProbeStream<Key, MATCHED, MATCH_FIRST>
{
    pub fn create(pointers: Vec<u64>) -> Box<dyn ProbeStream> {
        Box::new(OpenAddressingProbeStream::<Key, MATCHED, MATCH_FIRST> {
            pointers,
            key_idx: 0,
            probe_entry_ptr: 0,
            _phantom: std::marker::PhantomData,
        })
    }
}

impl<Key: HashtableKeyable + 'static, const MATCHED: bool, const MATCH_FIRST: bool> ProbeStream
    for OpenAddressingProbeStream<Key, MATCHED, MATCH_FIRST>
{
    fn advance(&mut self, res: &mut ProbedRows, max_rows: usize) -> Result<()> {
        while self.key_idx < self.pointers.len() {
            assume(res.matched_probe.len() == res.matched_build.len());
            assume(res.matched_build.len() < res.matched_build.capacity());
            assume(res.matched_probe.len() < res.matched_probe.capacity());

            if res.matched_probe.len() == max_rows {
                break;
            }

            if self.pointers[self.key_idx] == 0 {
                if !MATCHED {
                    res.unmatched.push(self.key_idx as u64);
                }

                self.key_idx += 1;
                continue;
            }

            if self.probe_entry_ptr == 0 {
                self.probe_entry_ptr = self.pointers[self.key_idx];
            }

            // In open addressing, all entries in the chain have the same key
            // (they were chained during insert because key matched)
            // So we don't need to compare keys here, and matched_num_rows >= 1 is guaranteed
            while self.probe_entry_ptr != 0 {
                let raw_entry = unsafe { &*(self.probe_entry_ptr as *mut RawEntry<Key>) };

                res.matched_probe.push(self.key_idx as u64);
                res.matched_build.push(raw_entry.row_ptr);

                if res.matched_probe.len() == max_rows {
                    self.probe_entry_ptr = match MATCH_FIRST {
                        true => 0,
                        false => raw_entry.next,
                    };

                    if self.probe_entry_ptr == 0 {
                        self.key_idx += 1;
                    }

                    return Ok(());
                }

                if MATCH_FIRST {
                    self.probe_entry_ptr = 0;
                    break;
                }

                self.probe_entry_ptr = raw_entry.next;
            }

            self.key_idx += 1;
        }

        Ok(())
    }
}
