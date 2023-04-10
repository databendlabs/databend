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
use std::sync::atomic::Ordering;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_hashtable::HashtableEntryRefLike;
use common_hashtable::HashtableLike;
use common_sql::plans::JoinType;

use crate::pipelines::processors::transforms::hash_join::desc::JOIN_MAX_BLOCK_SIZE;
use crate::pipelines::processors::transforms::hash_join::row::RowPtr;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::JoinHashTable;

impl JoinHashTable {
    /// Used by right join/right semi(anti) join
    pub(crate) fn probe_right_join<'a, H: HashtableLike<Value = Vec<RowPtr>>, IT>(
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
        let mut probed_num = 0;
        let mut probed_blocks = vec![];
        let mut local_probe_indexes = Vec::with_capacity(JOIN_MAX_BLOCK_SIZE);
        let mut local_build_indexes = Vec::with_capacity(JOIN_MAX_BLOCK_SIZE);
        let mut validity = MutableBitmap::with_capacity(JOIN_MAX_BLOCK_SIZE);
        let mut build_indexes = self.hash_join_desc.join_state.build_indexes.write();

        let chunks = &self.row_space.chunks.read().unwrap();
        let data_blocks = chunks
            .iter()
            .map(|c| c.data_block.clone())
            .collect::<Vec<DataBlock>>();
        let num_rows = data_blocks
            .iter()
            .fold(0, |acc, chunk| acc + chunk.num_rows());

        for (i, key) in keys_iter.enumerate() {
            let probe_result_ptr = self.probe_key(hash_table, key, valids, i);

            if let Some(v) = probe_result_ptr {
                let probed_rows = v.get();

                if local_probe_indexes.len() + probed_rows.len() < local_probe_indexes.capacity() {
                    for it in probed_rows {
                        build_indexes.push(*it);
                        local_build_indexes.push(it);
                    }
                    local_probe_indexes.push((i as u32, probed_rows.len() as u32));
                    probed_num += probed_rows.len();
                    validity.extend_constant(probed_rows.len(), true);
                } else {
                    let mut index = 0_usize;
                    let mut remain = probed_rows.len();

                    while index < probed_rows.len() {
                        if local_probe_indexes.len() + remain < local_probe_indexes.capacity() {
                            for it in probed_rows {
                                build_indexes.push(*it);
                                local_build_indexes.push(it);
                            }
                            local_probe_indexes.push((i as u32, probed_rows.len() as u32));
                            probed_num += remain;
                            validity.extend_constant(remain, true);

                            index += remain;
                        } else {
                            if self.interrupt.load(Ordering::Relaxed) {
                                return Err(ErrorCode::AbortedQuery(
                                    "Aborted query, because the server is shutting down or the query was killed.",
                                ));
                            }

                            let addition =
                                local_probe_indexes.capacity() - local_probe_indexes.len();
                            let new_index = index + addition;

                            for it in probed_rows {
                                build_indexes.push(*it);
                                local_build_indexes.push(it);
                            }
                            local_probe_indexes.push((i as u32, probed_rows.len() as u32));
                            probed_num += addition;
                            validity.extend_constant(addition, true);

                            let build_block = self.row_space.gather_build(
                                &local_build_indexes,
                                &data_blocks,
                                &num_rows,
                            )?;
                            let mut probe_block =
                                DataBlock::probe_take(input, &local_probe_indexes, probed_num)?;

                            // If join type is right join, need to wrap nullable for probe side
                            // If join type is semi/anti right join, directly merge `build_block` and `probe_block`
                            if self.hash_join_desc.join_type == JoinType::Right {
                                let validity: Bitmap = validity.into();
                                let nullable_columns = probe_block
                                    .columns()
                                    .iter()
                                    .map(|c| {
                                        Self::set_validity(c, probe_block.num_rows(), &validity)
                                    })
                                    .collect::<Vec<_>>();
                                probe_block = DataBlock::new(nullable_columns, validity.len());
                            }

                            if !probe_block.is_empty() {
                                probed_blocks
                                    .push(self.merge_eq_block(&build_block, &probe_block)?);
                            }

                            index = new_index;
                            remain -= addition;

                            local_probe_indexes.clear();
                            local_build_indexes.clear();
                            probed_num = 0;
                            validity = MutableBitmap::new();
                        }
                    }
                }
            }
        }

        let mut probe_block = DataBlock::probe_take(input, &local_probe_indexes, probed_num)?;

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
        rest_pairs.1.extend(local_build_indexes);

        Ok(probed_blocks)
    }
}
