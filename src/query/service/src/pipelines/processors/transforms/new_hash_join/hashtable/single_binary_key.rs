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

use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::HashMethod;
use databend_common_expression::HashMethodSingleBinary;
use databend_common_expression::KeysState;
use databend_common_expression::ProjectedBlock;

use crate::pipelines::processors::transforms::SingleBinaryHashJoinHashTable;
use crate::pipelines::processors::transforms::hash_join_table::BinaryHashJoinHashMap;
use crate::pipelines::processors::transforms::hash_join_table::HashJoinHashtableLike;
use crate::pipelines::processors::transforms::hash_join_table::RowPtr;
use crate::pipelines::processors::transforms::hash_join_table::STRING_EARLY_SIZE;
use crate::pipelines::processors::transforms::hash_join_table::StringRawEntry;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::ProbeData;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::AllUnmatchedProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::EmptyProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::serialize_keys::BinaryKeyProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::serialize_keys::EarlyFilteringProbeStream;

impl<const UNIQUE: bool> SingleBinaryHashJoinHashTable<UNIQUE> {
    pub fn new(
        hash_table: BinaryHashJoinHashMap<UNIQUE>,
        hash_method: HashMethodSingleBinary,
    ) -> SingleBinaryHashJoinHashTable<UNIQUE> {
        SingleBinaryHashJoinHashTable {
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
