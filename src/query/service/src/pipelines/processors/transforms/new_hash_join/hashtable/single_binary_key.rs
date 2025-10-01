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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::HashMethod;
use databend_common_expression::HashMethodSingleBinary;
use databend_common_expression::KeysState;
use databend_common_expression::ProjectedBlock;
use databend_common_hashtable::BinaryHashJoinHashMap;
use databend_common_hashtable::HashJoinHashtableLike;
use databend_common_hashtable::RowPtr;
use databend_common_hashtable::StringRawEntry;
use databend_common_hashtable::STRING_EARLY_SIZE;

use crate::pipelines::processors::transforms::new_hash_join::hashtable::serialize_keys::BinaryKeyProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::ProbeData;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::{AllUnmatchedProbeStream, ProbeStream};
use crate::pipelines::processors::transforms::SingleBinaryHashJoinHashTable;

impl SingleBinaryHashJoinHashTable {
    pub fn new(
        hash_table: BinaryHashJoinHashMap,
        hash_method: HashMethodSingleBinary,
    ) -> SingleBinaryHashJoinHashTable {
        SingleBinaryHashJoinHashTable {
            hash_table,
            hash_method,
            probed_rows: AtomicUsize::new(0),
            matched_probe_rows: AtomicUsize::new(0),
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
                    _ => Ok(BinaryKeyProbeStream::create(hashes, keys)),
                }
            }
            false => match self.hash_table.probe(&mut hashes, valids) {
                0 => Ok(AllUnmatchedProbeStream::create(hashes.len())),
                _ => Ok(BinaryKeyProbeStream::create(hashes, keys)),
            },
        }
    }
}
