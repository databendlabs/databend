// Copyright 2022 Datafuse Labs.
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

use std::iter::TrustedLen;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_datablocks::DataBlock;
use common_exception::Result;
use common_hashtable::HashMap;
use common_hashtable::HashTableKeyable;

use crate::pipelines::processors::transforms::hash_join::row::RowPtr;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::JoinHashTable;
use crate::sql::plans::JoinType;

impl JoinHashTable {
    /// Used by right join/right semi(anti) join
    pub(crate) fn probe_right_join<Key, IT>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &DataBlock,
    ) -> Result<Vec<DataBlock>>
    where
        Key: HashTableKeyable + Clone + 'static,
        IT: Iterator<Item = Key> + TrustedLen,
    {
        let local_build_indexes = &mut probe_state.build_indexs;
        let probe_indexes = &mut probe_state.probe_indexs;
        let valids = &probe_state.valids;
        let mut validity = MutableBitmap::new();
        let mut build_indexes = self.hash_join_desc.right_join_desc.build_indexes.write();
        for (i, key) in keys_iter.enumerate() {
            let probe_result_ptr = self.probe_key(hash_table, key, valids, i);
            if let Some(v) = probe_result_ptr {
                let probe_result_ptrs = v.get_value();
                build_indexes.extend(probe_result_ptrs);
                local_build_indexes.extend_from_slice(probe_result_ptrs);
                for row_ptr in probe_result_ptrs.iter() {
                    {
                        let mut row_state = self.hash_join_desc.right_join_desc.row_state.write();
                        row_state
                            .entry(*row_ptr)
                            .and_modify(|e| *e += 1)
                            .or_insert(1_usize);
                    }
                }
                probe_indexes.extend(std::iter::repeat(i as u32).take(probe_result_ptrs.len()));
                validity.extend_constant(probe_result_ptrs.len(), true);
            }
        }

        let build_block = self.row_space.gather(local_build_indexes)?;
        let mut probe_block = DataBlock::block_take_by_indices(input, probe_indexes)?;

        // If join type is right join, need to wrap nullable for probe side
        // If join type is semi/anti right join, directly merge `build_block` and `probe_block`
        if self.hash_join_desc.join_type == JoinType::Right {
            let validity: Bitmap = validity.into();
            let nullable_columns = probe_block
                .columns()
                .iter()
                .map(|c| Self::set_validity(c, &validity))
                .collect::<Result<Vec<_>>>()?;
            probe_block = DataBlock::create(self.probe_schema.clone(), nullable_columns);
        }

        Ok(vec![self.merge_eq_block(&build_block, &probe_block)?])
    }
}
