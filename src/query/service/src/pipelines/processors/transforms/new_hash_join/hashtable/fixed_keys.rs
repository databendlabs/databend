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

use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::AllUnmatchedProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::EmptyProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbedRows;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::ProbeData;
use crate::pipelines::processors::transforms::FixedKeyHashJoinHashTable;

impl<T: HashtableKeyable + FixedKey, const SKIP_DUPLICATES: bool>
    FixedKeyHashJoinHashTable<T, SKIP_DUPLICATES>
{
    pub fn new(
        hash_table: HashJoinHashMap<T, SKIP_DUPLICATES>,
        hash_method: HashMethodFixedKeys<T>,
    ) -> Self {
        FixedKeyHashJoinHashTable::<T, SKIP_DUPLICATES> {
            hash_table,
            hash_method,
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

        let enable_early_filtering = match ctx.probed_rows {
            0 => false,
            probed_rows => (ctx.matched_rows as f64) / (probed_rows as f64) < 0.8,
        };

        let matched_rows = match enable_early_filtering {
            true => self.hash_table.early_filtering_matched_probe(
                &mut hashes,
                valids,
                &mut ctx.selection,
            ),
            false => self.hash_table.probe(&mut hashes, valids),
        };

        ctx.probed_rows += probed_rows;
        ctx.matched_rows += matched_rows;

        match matched_rows {
            0 => Ok(Box::new(EmptyProbeStream)),
            _ => match enable_early_filtering {
                true => Ok(EarlyFilteringProbeStream::<_, true>::create(
                    hashes,
                    keys,
                    &ctx.selection,
                    &[],
                )),
                false => Ok(FixedKeyProbeStream::<_, true>::create(hashes, keys)),
            },
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

        let enable_early_filtering = match ctx.probed_rows {
            0 => false,
            probed_rows => (ctx.matched_rows as f64) / (probed_rows as f64) < 0.8,
        };

        let matched_rows = match enable_early_filtering {
            true => {
                let (matched_rows, _) = self.hash_table.early_filtering_probe(
                    &mut hashes,
                    valids,
                    &mut ctx.selection,
                    &mut ctx.unmatched_selection,
                );
                matched_rows
            }
            false => self.hash_table.probe(&mut hashes, valids),
        };

        ctx.probed_rows += probed_rows;
        ctx.matched_rows += matched_rows;

        match matched_rows {
            0 => Ok(AllUnmatchedProbeStream::create(hashes.len())),
            _ => match enable_early_filtering {
                true => Ok(EarlyFilteringProbeStream::<_, false>::create(
                    hashes,
                    keys,
                    &ctx.selection,
                    &ctx.unmatched_selection,
                )),
                false => Ok(FixedKeyProbeStream::<_, false>::create(hashes, keys)),
            },
        }
    }
}

struct FixedKeyProbeStream<Key: FixedKey + HashtableKeyable, const MATCHED: bool> {
    key_idx: usize,
    pointers: Vec<u64>,
    probe_entry_ptr: u64,
    keys: Box<(dyn KeyAccessor<Key = Key>)>,
    matched_num_rows: usize,
}

impl<Key: FixedKey + HashtableKeyable, const MATCHED: bool> FixedKeyProbeStream<Key, MATCHED> {
    pub fn create(
        pointers: Vec<u64>,
        keys: Box<dyn KeyAccessor<Key = Key>>,
    ) -> Box<dyn ProbeStream> {
        Box::new(FixedKeyProbeStream::<Key, MATCHED> {
            keys,
            pointers,
            key_idx: 0,
            probe_entry_ptr: 0,
            matched_num_rows: 0,
        })
    }
}

impl<Key: FixedKey + HashtableKeyable, const MATCHED: bool> ProbeStream
    for FixedKeyProbeStream<Key, MATCHED>
{
    fn advance(&mut self, res: &mut ProbedRows, max_rows: usize) -> Result<()> {
        while self.key_idx < self.keys.len() {
            assume(res.matched_probe.len() == res.matched_build.len());
            assume(res.matched_build.len() < res.matched_build.capacity());
            assume(res.matched_probe.len() < res.matched_probe.capacity());
            assume(self.key_idx < self.pointers.len());

            if res.matched_probe.len() == max_rows {
                break;
            }

            if self.probe_entry_ptr == 0 {
                self.probe_entry_ptr = self.pointers[self.key_idx];

                if self.probe_entry_ptr == 0 {
                    if !MATCHED {
                        res.unmatched.push(self.key_idx as u64);
                    }

                    self.key_idx += 1;
                    self.matched_num_rows = 0;
                    continue;
                }
            }

            let key = unsafe { self.keys.key_unchecked(self.key_idx) };

            while self.probe_entry_ptr != 0 {
                let raw_entry = unsafe { &*(self.probe_entry_ptr as *mut RawEntry<Key>) };

                if key == &raw_entry.key {
                    let row_ptr = raw_entry.row_ptr;
                    res.matched_probe.push(self.key_idx as u64);
                    res.matched_build.push(row_ptr);
                    self.matched_num_rows += 1;

                    if res.matched_probe.len() == max_rows {
                        self.probe_entry_ptr = raw_entry.next;

                        if self.probe_entry_ptr == 0 {
                            self.key_idx += 1;
                            self.matched_num_rows = 0;
                        }

                        return Ok(());
                    }
                }

                self.probe_entry_ptr = raw_entry.next;
            }

            if !MATCHED && self.matched_num_rows == 0 {
                res.unmatched.push(self.key_idx as u64);
            }

            self.key_idx += 1;
            self.matched_num_rows = 0;
        }

        Ok(())
    }
}

struct EarlyFilteringProbeStream<'a, Key: FixedKey + HashtableKeyable, const MATCHED: bool> {
    idx: usize,
    pointers: Vec<u64>,
    probe_entry_ptr: u64,
    keys: Box<(dyn KeyAccessor<Key = Key>)>,
    selections: &'a [u32],
    unmatched_selection: &'a [u32],
    matched_num_rows: usize,
    returned_unmatched: bool,
}

impl<'a, Key: FixedKey + HashtableKeyable, const MATCHED: bool>
    EarlyFilteringProbeStream<'a, Key, MATCHED>
{
    pub fn create(
        pointers: Vec<u64>,
        keys: Box<dyn KeyAccessor<Key = Key>>,
        selections: &'a [u32],
        unmatched_selection: &'a [u32],
    ) -> Box<dyn ProbeStream + 'a> {
        Box::new(EarlyFilteringProbeStream::<Key, MATCHED> {
            keys,
            pointers,
            selections,
            unmatched_selection,
            idx: 0,
            probe_entry_ptr: 0,
            matched_num_rows: 0,
            returned_unmatched: false,
        })
    }
}

impl<'a, Key: FixedKey + HashtableKeyable, const MATCHED: bool> ProbeStream
    for EarlyFilteringProbeStream<'a, Key, MATCHED>
{
    fn advance(&mut self, res: &mut ProbedRows, max_rows: usize) -> Result<()> {
        if !MATCHED && !self.returned_unmatched {
            self.returned_unmatched = true;
            res.unmatched
                .extend(self.unmatched_selection.iter().map(|x| *x as u64));
        }

        while self.idx < self.selections.len() {
            let key_idx = self.selections[self.idx] as usize;

            assume(res.unmatched.len() < res.unmatched.capacity());
            assume(res.matched_probe.len() == res.matched_build.len());
            assume(res.matched_build.len() < res.matched_build.capacity());
            assume(res.matched_probe.len() < res.matched_probe.capacity());
            assume(key_idx < self.pointers.len());

            if res.matched_probe.len() == max_rows {
                break;
            }

            if self.probe_entry_ptr == 0 {
                self.probe_entry_ptr = self.pointers[key_idx];

                if self.probe_entry_ptr == 0 {
                    self.idx += 1;
                    self.matched_num_rows = 0;
                    continue;
                }
            }

            let key = unsafe { self.keys.key_unchecked(key_idx) };

            while self.probe_entry_ptr != 0 {
                let raw_entry = unsafe { &*(self.probe_entry_ptr as *mut RawEntry<Key>) };

                if key == &raw_entry.key {
                    let row_ptr = raw_entry.row_ptr;
                    res.matched_probe.push(key_idx as u64);
                    res.matched_build.push(row_ptr);
                    self.matched_num_rows += 1;

                    if res.matched_probe.len() == max_rows {
                        self.probe_entry_ptr = raw_entry.next;

                        if self.probe_entry_ptr == 0 {
                            self.idx += 1;
                            self.matched_num_rows = 0;
                        }

                        return Ok(());
                    }
                }

                self.probe_entry_ptr = raw_entry.next;
            }

            if !MATCHED && self.matched_num_rows == 0 {
                res.unmatched.push(key_idx as u64);
            }

            self.idx += 1;
            self.matched_num_rows = 0;
        }

        Ok(())
    }
}
