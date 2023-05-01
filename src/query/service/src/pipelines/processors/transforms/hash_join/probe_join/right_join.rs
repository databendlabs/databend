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

use std::iter::TrustedLen;
use std::sync::atomic::Ordering;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_hashtable::HashJoinHashtableLike;
use common_sql::plans::JoinType;

use crate::pipelines::processors::transforms::hash_join::desc::JOIN_MAX_BLOCK_SIZE;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::JoinHashTable;

impl JoinHashTable {
    /// Used by right join/right semi(anti) join
    pub(crate) fn probe_right_join<'a, H: HashJoinHashtableLike, IT>(
        &self,
        hash_table: &H,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &DataBlock,
    ) -> Result<Vec<DataBlock>>
    where
        IT: Iterator<Item = &'a H::Key> + TrustedLen,
        H::Key: 'a,
    {
        let valids = &probe_state.valids;
        // The right join will return multiple data blocks of similar size.
        let mut occupied = 0;
        let mut probed_blocks = vec![];
        let mut probe_indexes_len = 0;
        let local_probe_indexes = &mut probe_state.probe_indexes;
        let local_build_indexes = &mut probe_state.build_indexes;
        let local_build_indexes_ptr = local_build_indexes.as_mut_ptr();
        let mut validity = MutableBitmap::with_capacity(JOIN_MAX_BLOCK_SIZE);

        let data_blocks = self.row_space.chunks.read().unwrap();
        let data_blocks = data_blocks
            .iter()
            .map(|c| &c.data_block)
            .collect::<Vec<_>>();
        let num_rows = data_blocks
            .iter()
            .fold(0, |acc, chunk| acc + chunk.num_rows());

        for (i, key) in keys_iter.enumerate() {
            let (mut match_count, mut incomplete_ptr) = self.probe_key(
                hash_table,
                key,
                valids,
                i,
                local_build_indexes_ptr,
                occupied,
            );
            if match_count == 0 {
                continue;
            }
            occupied += match_count;
            local_probe_indexes[probe_indexes_len] = (i as u32, match_count as u32);
            probe_indexes_len += 1;
            validity.extend_constant(match_count, true);
            if occupied >= JOIN_MAX_BLOCK_SIZE {
                loop {
                    if self.interrupt.load(Ordering::Relaxed) {
                        return Err(ErrorCode::AbortedQuery(
                            "Aborted query, because the server is shutting down or the query was killed.",
                        ));
                    }

                    {
                        let mut build_indexes =
                            self.hash_join_desc.join_state.build_indexes.write();
                        build_indexes.extend_from_slice(local_build_indexes);
                    }

                    let build_block =
                        self.row_space
                            .gather(local_build_indexes, &data_blocks, &num_rows)?;
                    let mut probe_block = DataBlock::take_compacted_indices(
                        input,
                        &local_probe_indexes[0..probe_indexes_len],
                        occupied,
                    )?;

                    // If join type is right join, need to wrap nullable for probe side
                    // If join type is semi/anti right join, directly merge `build_block` and `probe_block`
                    if self.hash_join_desc.join_type == JoinType::Right {
                        let validity: Bitmap = validity.into();
                        let nullable_columns = probe_block
                            .columns()
                            .iter()
                            .map(|c| Self::set_validity(c, probe_block.num_rows(), &validity))
                            .collect::<Vec<_>>();
                        probe_block = DataBlock::new(nullable_columns, validity.len());
                    }

                    if !probe_block.is_empty() {
                        probed_blocks.push(self.merge_eq_block(&build_block, &probe_block)?);
                    }

                    probe_indexes_len = 0;
                    occupied = 0;
                    validity = MutableBitmap::new();

                    if incomplete_ptr == 0 {
                        break;
                    }
                    (match_count, incomplete_ptr) = hash_table.next_incomplete_ptr(
                        key,
                        incomplete_ptr,
                        local_build_indexes_ptr,
                        occupied,
                        JOIN_MAX_BLOCK_SIZE,
                    );
                    if match_count == 0 {
                        break;
                    }

                    occupied += match_count;
                    local_probe_indexes[probe_indexes_len] = (i as u32, match_count as u32);
                    probe_indexes_len += 1;
                    validity.extend_constant(match_count, true);

                    if occupied < JOIN_MAX_BLOCK_SIZE {
                        break;
                    }
                }
            }
        }

        {
            let mut build_indexes = self.hash_join_desc.join_state.build_indexes.write();
            build_indexes.extend_from_slice(&local_build_indexes[0..occupied]);
        }

        let mut probe_block = DataBlock::take_compacted_indices(
            input,
            &local_probe_indexes[0..probe_indexes_len],
            occupied,
        )?;

        // If join type is right join, need to wrap nullable for probe side
        // If join type is semi/anti right join, directly merge `build_block` and `probe_block`
        if self.hash_join_desc.join_type == JoinType::Right {
            let validity: Bitmap = validity.into();
            let nullable_columns = probe_block
                .columns()
                .iter()
                .map(|c| Self::set_validity(c, probe_block.num_rows(), &validity))
                .collect::<Vec<_>>();
            probe_block = DataBlock::new(nullable_columns, validity.len());
        }

        let mut rest_pairs = self.hash_join_desc.join_state.rest_pairs.write();
        rest_pairs.0.push(probe_block);
        rest_pairs.1.extend(&local_build_indexes[0..occupied]);

        Ok(probed_blocks)
    }
}
