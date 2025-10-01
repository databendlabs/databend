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
use databend_common_hashtable::HashJoinHashtableLike;
use databend_common_hashtable::HashtableKeyable;
use databend_common_hashtable::RawEntry;
use databend_common_hashtable::RowPtr;

use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::{AllUnmatchedProbeStream, ProbeStream};
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbedRows;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::ProbeData;
use crate::pipelines::processors::transforms::FixedKeyHashJoinHashTable;

impl<T: HashtableKeyable + FixedKey> FixedKeyHashJoinHashTable<T> {
    pub fn new(hash_table: HashJoinHashMap<T>, hash_method: HashMethodFixedKeys<T>) -> Self {
        FixedKeyHashJoinHashTable::<T> {
            hash_table,
            hash_method,
            probed_rows: Default::default(),
            matched_probe_rows: Default::default(),
        }
    }

    pub fn insert(&self, keys: DataBlock, chunk: usize, arena: &mut Vec<u8>) -> Result<()> {
        let num_rows = keys.num_rows();
        let keys = ProjectedBlock::from(keys.columns());
        let keys_state = self.hash_method.build_keys_state(keys, num_rows)?;
        let build_keys_iter = self.hash_method.build_keys_iter(&keys_state)?;

        let entry_size = std::mem::size_of::<RawEntry<T>>();
        arena.reserve(num_rows * entry_size);

        let mut raw_entry_ptr =
            unsafe { std::mem::transmute::<*mut u8, *mut RawEntry<T>>(arena.as_mut_ptr()) };

        for (row_index, key) in build_keys_iter.enumerate() {
            let row_ptr = RowPtr {
                chunk_index: chunk as u32,
                row_index: row_index as u32,
            };

            // # Safety
            // The memory address of `raw_entry_ptr` is valid.
            unsafe {
                *raw_entry_ptr = RawEntry {
                    row_ptr,
                    key: *key,
                    next: 0,
                }
            }

            self.hash_table.insert(*key, raw_entry_ptr);
            raw_entry_ptr = unsafe { raw_entry_ptr.add(1) };
        }

        Ok(())
    }

    pub fn probe(&self, probe_data: ProbeData) -> Result<Box<dyn ProbeStream + '_>> {
        let num_rows = probe_data.num_rows();
        let hash_method = &self.hash_method;
        let mut hashes = Vec::with_capacity(num_rows);

        let keys = ProjectedBlock::from(probe_data.columns());
        let keys_state = hash_method.build_keys_state(keys, num_rows)?;
        hash_method.build_keys_hashes(&keys_state, &mut hashes);
        let keys = hash_method.build_keys_accessor(keys_state.clone())?;

        let enable_early_filtering = match self.probed_rows.load(Ordering::Relaxed) {
            0 => false,
            probed_rows => {
                let matched_probe_rows = self.matched_probe_rows.load(Ordering::Relaxed) as f64;
                matched_probe_rows / (probed_rows as f64) < 0.8
            }
        };

        let probed_rows = probe_data.non_null_rows();
        self.probed_rows.fetch_add(probed_rows, Ordering::Relaxed);

        let (_, valids) = probe_data.into_raw();

        match enable_early_filtering {
            true => {
                let mut selection = vec![0; num_rows];

                match self.hash_table.early_filtering_matched_probe(
                    &mut hashes,
                    valids,
                    &mut selection,
                ) {
                    0 => Ok(AllUnmatchedProbeStream::create(hashes.len())),
                    _ => Ok(FixedKeysProbeStream::create(hashes, keys)),
                }
            }
            false => match self.hash_table.probe(&mut hashes, valids) {
                0 => Ok(AllUnmatchedProbeStream::create(hashes.len())),
                _ => Ok(FixedKeysProbeStream::create(hashes, keys)),
            },
        }
    }
}

pub struct FixedKeysProbeStream<Key: FixedKey + HashtableKeyable> {
    key_idx: usize,
    pointers: Vec<u64>,
    keys: Box<(dyn KeyAccessor<Key = Key>)>,
    probe_entry_ptr: u64,
}

impl<Key: FixedKey + HashtableKeyable> FixedKeysProbeStream<Key> {
    pub fn create(
        pointers: Vec<u64>,
        keys: Box<dyn KeyAccessor<Key = Key>>,
    ) -> Box<dyn ProbeStream> {
        Box::new(FixedKeysProbeStream {
            keys,
            pointers,
            key_idx: 0,
            probe_entry_ptr: 0,
        })
    }
}

impl<Key: FixedKey + HashtableKeyable> ProbeStream for FixedKeysProbeStream<Key> {
    fn next(&mut self, max_rows: usize) -> Result<ProbedRows> {
        unsafe {
            let mut matched_build = Vec::with_capacity(max_rows);
            let mut matched_probe = Vec::with_capacity(max_rows);
            let mut unmatched = Vec::with_capacity(max_rows);

            while self.key_idx < self.keys.len() {
                assume(unmatched.len() <= unmatched.capacity());
                assume(matched_probe.len() == matched_build.len());
                assume(matched_build.len() <= matched_build.capacity());
                assume(matched_probe.len() <= matched_probe.capacity());
                assume(self.key_idx < self.pointers.len());

                if matched_probe.len() == max_rows {
                    break;
                }

                if self.probe_entry_ptr == 0 {
                    self.probe_entry_ptr = self.pointers[self.key_idx];

                    if self.probe_entry_ptr == 0 {
                        unmatched.push(self.key_idx);
                        self.key_idx += 1;
                        continue;
                    }
                }

                let key = self.keys.key_unchecked(self.key_idx);

                while self.probe_entry_ptr != 0 {
                    let raw_entry = &*(self.probe_entry_ptr as *mut RawEntry<Key>);

                    if key == &raw_entry.key {
                        let row_ptr = raw_entry.row_ptr;
                        matched_probe.push(self.key_idx as u64);
                        matched_build.push(row_ptr);

                        if matched_probe.len() == max_rows {
                            self.probe_entry_ptr = raw_entry.next;

                            if self.probe_entry_ptr == 0 {
                                self.key_idx += 1;
                            }

                            return Ok(ProbedRows::new(unmatched, matched_probe, matched_build));
                        }
                    }

                    self.probe_entry_ptr = raw_entry.next;
                }

                self.key_idx += 1;
            }

            Ok(ProbedRows::new(unmatched, matched_probe, matched_build))
        }
    }
}
