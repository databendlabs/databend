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
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::HashMethod;
use databend_common_expression::HashMethodSerializer;
use databend_common_expression::KeyAccessor;
use databend_common_expression::KeysState;
use databend_common_expression::ProjectedBlock;

use crate::pipelines::processors::transforms::SerializerHashJoinHashTable;
use crate::pipelines::processors::transforms::hash_join_table::BinaryHashJoinHashMap;
use crate::pipelines::processors::transforms::hash_join_table::HashJoinHashtableLike;
use crate::pipelines::processors::transforms::hash_join_table::RowPtr;
use crate::pipelines::processors::transforms::hash_join_table::STRING_EARLY_SIZE;
use crate::pipelines::processors::transforms::hash_join_table::StringRawEntry;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::ProbeData;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::AllUnmatchedProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::EmptyProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbedRows;

impl<const UNIQUE: bool> SerializerHashJoinHashTable<UNIQUE> {
    pub fn new(
        hash_table: BinaryHashJoinHashMap<UNIQUE>,
        hash_method: HashMethodSerializer,
    ) -> SerializerHashJoinHashTable<UNIQUE> {
        SerializerHashJoinHashTable {
            hash_table,
            hash_method,
        }
    }

    pub fn insert(&self, keys: DataBlock, chunk: usize, arena: &mut Vec<u8>) -> Result<()> {
        let num_rows = keys.num_rows();
        let keys = ProjectedBlock::from(keys.columns());
        let keys_state = self.hash_method.build_keys_state(keys, num_rows)?;
        let build_keys_iter = self.hash_method.build_keys_iter(&keys_state)?;

        let space_size = match &keys_state {
            // safe to unwrap(): offset.len() >= 1.
            KeysState::Column(Column::Bitmap(col)) => col.data().len(),
            KeysState::Column(Column::Binary(col)) => col.data().len(),
            KeysState::Column(Column::Variant(col)) => col.data().len(),
            KeysState::Column(Column::String(col)) => col.total_bytes_len(),
            _ => unreachable!(),
        };

        static ENTRY_SIZE: usize = std::mem::size_of::<StringRawEntry>();
        arena.reserve(num_rows * ENTRY_SIZE + space_size);

        let (mut raw_entry_ptr, mut string_local_space_ptr) = unsafe {
            (
                std::mem::transmute::<*mut u8, *mut StringRawEntry>(arena.as_mut_ptr()),
                arena.as_mut_ptr().add(num_rows * ENTRY_SIZE),
            )
        };

        for (row_index, key) in build_keys_iter.enumerate() {
            let row_ptr = RowPtr {
                chunk_index: chunk as u32,
                row_index: row_index as u32,
            };

            // # Safety
            // The memory address of `raw_entry_ptr` is valid.
            // string_offset + key.len() <= space_size.
            unsafe {
                (*raw_entry_ptr).row_ptr = row_ptr;
                (*raw_entry_ptr).length = key.len() as u32;
                (*raw_entry_ptr).next = 0;
                (*raw_entry_ptr).key = string_local_space_ptr;
                // The size of `early` is 4.
                std::ptr::copy_nonoverlapping(
                    key.as_ptr(),
                    (*raw_entry_ptr).early.as_mut_ptr(),
                    std::cmp::min(STRING_EARLY_SIZE, key.len()),
                );
                std::ptr::copy_nonoverlapping(key.as_ptr(), string_local_space_ptr, key.len());
                string_local_space_ptr = string_local_space_ptr.add(key.len());
            }

            self.hash_table.insert(key, raw_entry_ptr);
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
                true => Ok(EarlyFilteringProbeStream::<true, UNIQUE>::create(
                    hashes,
                    keys,
                    &ctx.selection,
                    &[],
                )),
                false => Ok(BinaryKeyProbeStream::<true, UNIQUE>::create(hashes, keys)),
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
                true => Ok(EarlyFilteringProbeStream::<false, UNIQUE>::create(
                    hashes,
                    keys,
                    &ctx.selection,
                    &ctx.unmatched_selection,
                )),
                false => Ok(BinaryKeyProbeStream::<false, UNIQUE>::create(hashes, keys)),
            },
        }
    }
}

pub struct BinaryKeyProbeStream<const MATCHED: bool, const MATCH_FIRST: bool> {
    key_idx: usize,
    pointers: Vec<u64>,
    keys: Box<dyn KeyAccessor<Key = [u8]>>,
    probe_entry_ptr: u64,
    matched_num_rows: usize,
}

impl<const MATCHED: bool, const MATCH_FIRST: bool> BinaryKeyProbeStream<MATCHED, MATCH_FIRST> {
    pub fn create(
        pointers: Vec<u64>,
        keys: Box<dyn KeyAccessor<Key = [u8]>>,
    ) -> Box<dyn ProbeStream> {
        Box::new(BinaryKeyProbeStream::<MATCHED, MATCH_FIRST> {
            keys,
            pointers,
            key_idx: 0,
            probe_entry_ptr: 0,
            matched_num_rows: 0,
        })
    }
}

impl<const MATCHED: bool, const MATCH_FIRST: bool> ProbeStream
    for BinaryKeyProbeStream<MATCHED, MATCH_FIRST>
{
    fn advance(&mut self, res: &mut ProbedRows, max_rows: usize) -> Result<()> {
        while self.key_idx < self.keys.len() {
            assume(res.unmatched.len() < res.unmatched.capacity());
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
                let raw_entry = unsafe { &*(self.probe_entry_ptr as *mut StringRawEntry) };
                // Compare `early` and the length of the string, the size of `early` is 4.
                let min_len = std::cmp::min(STRING_EARLY_SIZE, key.len());

                unsafe {
                    if raw_entry.length as usize == key.len()
                        && key[0..min_len] == raw_entry.early[0..min_len]
                    {
                        let key_ref = std::slice::from_raw_parts(
                            raw_entry.key as *const u8,
                            raw_entry.length as usize,
                        );
                        if key == key_ref {
                            let row_ptr = raw_entry.row_ptr;
                            res.matched_probe.push(self.key_idx as u64);
                            res.matched_build.push(row_ptr);
                            self.matched_num_rows += 1;

                            if res.matched_probe.len() == max_rows {
                                self.probe_entry_ptr = match MATCH_FIRST {
                                    true => 0,
                                    false => raw_entry.next,
                                };

                                if self.probe_entry_ptr == 0 {
                                    self.key_idx += 1;
                                    self.matched_num_rows = 0;
                                }

                                return Ok(());
                            }

                            if MATCH_FIRST {
                                self.probe_entry_ptr = 0;
                                break;
                            }
                        }
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

pub struct EarlyFilteringProbeStream<'a, const MATCHED: bool, const MATCH_FIRST: bool> {
    idx: usize,
    pointers: Vec<u64>,
    keys: Box<dyn KeyAccessor<Key = [u8]>>,
    probe_entry_ptr: u64,
    selections: &'a [u32],
    unmatched_selection: &'a [u32],
    matched_num_rows: usize,
    returned_unmatched: bool,
}

impl<'a, const MATCHED: bool, const MATCH_FIRST: bool>
    EarlyFilteringProbeStream<'a, MATCHED, MATCH_FIRST>
{
    pub fn create(
        pointers: Vec<u64>,
        keys: Box<dyn KeyAccessor<Key = [u8]>>,
        selections: &'a [u32],
        unmatched_selection: &'a [u32],
    ) -> Box<dyn ProbeStream + 'a> {
        Box::new(EarlyFilteringProbeStream::<'a, MATCHED, MATCH_FIRST> {
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

impl<'a, const MATCHED: bool, const MATCH_FIRST: bool> ProbeStream
    for EarlyFilteringProbeStream<'a, MATCHED, MATCH_FIRST>
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
                    if !MATCHED {
                        res.unmatched.push(key_idx as u64);
                    }

                    self.idx += 1;
                    self.matched_num_rows = 0;
                    continue;
                }
            }

            let key = unsafe { self.keys.key_unchecked(key_idx) };

            while self.probe_entry_ptr != 0 {
                let raw_entry = unsafe { &*(self.probe_entry_ptr as *mut StringRawEntry) };
                // Compare `early` and the length of the string, the size of `early` is 4.
                let min_len = std::cmp::min(STRING_EARLY_SIZE, key.len());

                unsafe {
                    if raw_entry.length as usize == key.len()
                        && key[0..min_len] == raw_entry.early[0..min_len]
                    {
                        let key_ref = std::slice::from_raw_parts(
                            raw_entry.key as *const u8,
                            raw_entry.length as usize,
                        );
                        if key == key_ref {
                            let row_ptr = raw_entry.row_ptr;
                            res.matched_probe.push(key_idx as u64);
                            res.matched_build.push(row_ptr);
                            self.matched_num_rows += 1;

                            if res.matched_probe.len() == max_rows {
                                self.probe_entry_ptr = match MATCH_FIRST {
                                    true => 0,
                                    false => raw_entry.next,
                                };

                                if self.probe_entry_ptr == 0 {
                                    self.idx += 1;
                                    self.matched_num_rows = 0;
                                }

                                return Ok(());
                            }

                            if MATCH_FIRST {
                                self.probe_entry_ptr = 0;
                                break;
                            }
                        }
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
