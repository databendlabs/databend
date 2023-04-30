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

use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_hashtable::HashJoinHashtableLike;
use common_hashtable::RowPtr;

use crate::pipelines::processors::transforms::hash_join::desc::JOIN_MAX_BLOCK_SIZE;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::JoinHashTable;

/// Semi join contain semi join and semi-anti join
impl JoinHashTable {
    pub(crate) fn probe_left_semi_join<'a, H: HashJoinHashtableLike, IT>(
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
        match self.hash_join_desc.other_predicate.is_none() {
            true => {
                self.left_semi_anti_join::<true, _, _>(hash_table, probe_state, keys_iter, input)
            }
            false => self.left_semi_anti_join_with_other_conjunct::<true, _, _>(
                hash_table,
                probe_state,
                keys_iter,
                input,
            ),
        }
    }

    pub(crate) fn probe_left_anti_semi_join<'a, H: HashJoinHashtableLike, IT>(
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
        match self.hash_join_desc.other_predicate.is_none() {
            true => {
                self.left_semi_anti_join::<false, _, _>(hash_table, probe_state, keys_iter, input)
            }
            false => self.left_semi_anti_join_with_other_conjunct::<false, _, _>(
                hash_table,
                probe_state,
                keys_iter,
                input,
            ),
        }
    }

    fn left_semi_anti_join<'a, const SEMI: bool, H: HashJoinHashtableLike, IT>(
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
        // If there is no build key, the result is input
        // Eg: select * from onecolumn as a right semi join twocolumn as b on true order by b.x
        let mut probe_indexes = Vec::with_capacity(input.num_rows());
        let valids = &probe_state.valids;

        for (i, key) in keys_iter.enumerate() {
            let contains = if self.hash_join_desc.from_correlated_subquery {
                hash_table.contains(key)
            } else {
                self.contains(hash_table, key, valids, i)
            };

            match (contains, SEMI) {
                (true, true) | (false, false) => {
                    probe_indexes.push((i as u32, 1));
                }
                _ => {}
            }
        }
        Ok(vec![DataBlock::take_compacted_indices(
            input,
            &probe_indexes,
            probe_indexes.len(),
        )?])
    }

    fn left_semi_anti_join_with_other_conjunct<'a, const SEMI: bool, H: HashJoinHashtableLike, IT>(
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
        // The semi join will return multiple data chunks of similar size.
        let mut occupied = 0;
        let mut probed_blocks = vec![];
        let mut probe_indexes_len = 0;
        let probe_indexes = &mut probe_state.probe_indexes;
        let build_indexes = &mut probe_state.build_indexes;
        let build_indexes_ptr = build_indexes.as_mut_ptr();

        let data_blocks = self.row_space.chunks.read().unwrap();
        let data_blocks = data_blocks
            .iter()
            .map(|c| &c.data_block)
            .collect::<Vec<_>>();
        let num_rows = data_blocks
            .iter()
            .fold(0, |acc, chunk| acc + chunk.num_rows());

        let other_predicate = self.hash_join_desc.other_predicate.as_ref().unwrap();
        // For semi join, it defaults to all.
        let mut row_state = vec![0_u32; input.num_rows()];
        let dummy_probed_rows = vec![RowPtr {
            chunk_index: 0,
            row_index: 0,
            marker: None,
        }];

        for (i, key) in keys_iter.enumerate() {
            let (mut match_count, mut incomplete_ptr) = if self
                .hash_join_desc
                .from_correlated_subquery
            {
                hash_table.probe_hash_table(key, build_indexes_ptr, occupied, JOIN_MAX_BLOCK_SIZE)
            } else {
                self.probe_key(hash_table, key, valids, i, build_indexes_ptr, occupied)
            };

            let true_match_count = match_count;
            match match_count > 0 {
                false if SEMI => {
                    continue;
                }
                false => {
                    // dummy_probed_rows
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            &dummy_probed_rows[0] as *const RowPtr,
                            build_indexes_ptr.add(occupied),
                            1,
                        )
                    }
                    match_count = 1;
                }
                true => (),
            };

            if true_match_count > 0 && !SEMI {
                row_state[i] += true_match_count as u32;
            }

            occupied += match_count;
            probe_indexes[probe_indexes_len] = (i as u32, match_count as u32);
            probe_indexes_len += 1;
            if occupied >= JOIN_MAX_BLOCK_SIZE {
                loop {
                    if self.interrupt.load(Ordering::Relaxed) {
                        return Err(ErrorCode::AbortedQuery(
                            "Aborted query, because the server is shutting down or the query was killed.",
                        ));
                    }

                    let probe_block = DataBlock::take_compacted_indices(
                        input,
                        &probe_indexes[0..probe_indexes_len],
                        occupied,
                    )?;
                    let build_block =
                        self.row_space
                            .gather(build_indexes, &data_blocks, &num_rows)?;
                    let merged_block = self.merge_eq_block(&build_block, &probe_block)?;

                    let mut bm = match self.get_other_filters(&merged_block, other_predicate)? {
                        (Some(b), _, _) => b.into_mut().right().unwrap(),
                        (_, true, _) => MutableBitmap::from_len_set(merged_block.num_rows()),
                        (_, _, true) => MutableBitmap::from_len_zeroed(merged_block.num_rows()),
                        _ => unreachable!(),
                    };

                    if SEMI {
                        self.fill_null_for_semi_join(
                            &mut bm,
                            probe_indexes,
                            probe_indexes_len,
                            &mut row_state,
                        );
                    } else {
                        self.fill_null_for_anti_join(
                            &mut bm,
                            probe_indexes,
                            probe_indexes_len,
                            &mut row_state,
                        );
                    }

                    let probed_data_block = DataBlock::filter_with_bitmap(probe_block, &bm.into())?;

                    if !probed_data_block.is_empty() {
                        probed_blocks.push(probed_data_block);
                    }

                    probe_indexes_len = 0;
                    occupied = 0;

                    if incomplete_ptr == 0 {
                        break;
                    }
                    (match_count, incomplete_ptr) = hash_table.next_incomplete_ptr(
                        key,
                        incomplete_ptr,
                        build_indexes_ptr,
                        occupied,
                        JOIN_MAX_BLOCK_SIZE,
                    );
                    if match_count == 0 {
                        break;
                    }

                    if !SEMI {
                        row_state[i] += match_count as u32;
                    }

                    occupied += match_count;
                    probe_indexes[probe_indexes_len] = (i as u32, match_count as u32);
                    probe_indexes_len += 1;

                    if occupied < JOIN_MAX_BLOCK_SIZE {
                        break;
                    }
                }
            }
        }

        if self.interrupt.load(Ordering::Relaxed) {
            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the server is shutting down or the query was killed.",
            ));
        }

        let probe_block = DataBlock::take_compacted_indices(
            input,
            &probe_indexes[0..probe_indexes_len],
            occupied,
        )?;
        let build_block =
            self.row_space
                .gather(&build_indexes[0..occupied], &data_blocks, &num_rows)?;
        let merged_block = self.merge_eq_block(&build_block, &probe_block)?;

        let mut bm = match self.get_other_filters(&merged_block, other_predicate)? {
            (Some(b), _, _) => b.into_mut().right().unwrap(),
            (_, true, _) => MutableBitmap::from_len_set(merged_block.num_rows()),
            (_, _, true) => MutableBitmap::from_len_zeroed(merged_block.num_rows()),
            _ => unreachable!(),
        };

        if SEMI {
            self.fill_null_for_semi_join(&mut bm, probe_indexes, probe_indexes_len, &mut row_state);
        } else {
            self.fill_null_for_anti_join(&mut bm, probe_indexes, probe_indexes_len, &mut row_state);
        }

        let probed_data_chunk = DataBlock::filter_with_bitmap(probe_block, &bm.into())?;

        if !probed_data_chunk.is_empty() {
            probed_blocks.push(probed_data_chunk);
        }

        Ok(probed_blocks)
    }

    // modify the bm by the value row_state
    // keep the index of the first positive state
    // bitmap: [1, 1, 1] with row_state [0, 0], probe_index: [(0, 3)] => [0, 0, 0] (repeat the first element 3 times)
    // bitmap will be [1, 1, 1] -> [1, 1, 1] -> [1, 0, 1] -> [1, 0, 0]
    // row_state will be [0, 0] -> [1, 0] -> [1,0] -> [1, 0]
    fn fill_null_for_semi_join(
        &self,
        bm: &mut MutableBitmap,
        probe_indexes: &[(u32, u32)],
        probe_indexes_len: usize,
        row_state: &mut [u32],
    ) {
        let mut index = 0;
        let mut idx = 0;
        while idx < probe_indexes_len {
            let (row, cnt) = probe_indexes[idx];
            idx += 1;
            for _ in 0..cnt {
                if bm.get(index) {
                    if row_state[row as usize] == 0 {
                        row_state[row as usize] = 1;
                    } else {
                        bm.set(index, false);
                    }
                }
                index += 1;
            }
        }
    }

    // keep the index of the negative state
    // bitmap: [1, 1, 1] with row_state [3, 0], probe_index: [(0, 3)] => [0, 0, 0] (repeat the first element 3 times)
    // bitmap will be [1, 1, 1] -> [0, 1, 1] -> [0, 0, 1] -> [0, 0, 0]
    // row_state will be [3, 0] -> [3, 0] -> [3, 0] -> [3, 0]
    fn fill_null_for_anti_join(
        &self,
        bm: &mut MutableBitmap,
        probe_indexes: &[(u32, u32)],
        probe_indexes_len: usize,
        row_state: &mut [u32],
    ) {
        let mut index = 0;
        let mut idx = 0;
        while idx < probe_indexes_len {
            let (row, cnt) = probe_indexes[idx];
            idx += 1;
            for _ in 0..cnt {
                if row_state[row as usize] == 0 {
                    // if state is not matched, anti result will take one
                    bm.set(index, true);
                } else if row_state[row as usize] == 1 {
                    // if state has just one, anti reverse the result
                    row_state[row as usize] -= 1;
                    bm.set(index, !bm.get(index))
                } else if !bm.get(index) {
                    row_state[row as usize] -= 1;
                } else {
                    bm.set(index, false);
                }
                index += 1;
            }
        }
    }
}
