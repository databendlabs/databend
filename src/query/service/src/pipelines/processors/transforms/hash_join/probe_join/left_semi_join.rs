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

use crate::pipelines::processors::transforms::hash_join::HashJoinProbeState;
use crate::pipelines::processors::transforms::hash_join::ProbeState;

/// Semi join contain semi join and semi-anti join
impl HashJoinProbeState {
    pub(crate) fn left_semi_anti_join<'a, const SEMI: bool, H: HashJoinHashtableLike, IT>(
        &self,
        hash_table: &H,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        pointers: &[u64],
        input: &DataBlock,
    ) -> Result<Vec<DataBlock>>
    where
        IT: Iterator<Item = &'a H::Key> + TrustedLen,
        H::Key: 'a,
    {
        // If there is no build key, the result is input
        // Eg: select * from onecolumn as a right semi join twocolumn as b on true order by b.x
        let max_block_size = probe_state.max_block_size;
        let valids = probe_state.valids.as_ref();
        let probe_indexes = &mut probe_state.probe_indexes;
        let string_items_buf = &mut probe_state.string_items_buf;
        let mut matched_num = 0;
        let mut result_blocks = vec![];

        for (i, (key, ptr)) in keys_iter.zip(pointers).enumerate() {
            let contains = if self.hash_join_state.hash_join_desc.from_correlated_subquery
                || valids.map_or(true, |v| v.get_bit(i))
            {
                hash_table.next_contains(key, *ptr)
            } else {
                false
            };

            match (contains, SEMI) {
                (true, true) | (false, false) => {
                    probe_indexes[matched_num] = i as u32;
                    matched_num += 1;
                    if matched_num >= max_block_size {
                        if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
                            return Err(ErrorCode::AbortedQuery(
                                "Aborted query, because the server is shutting down or the query was killed.",
                            ));
                        }
                        let probe_block = DataBlock::take(input, probe_indexes, string_items_buf)?;
                        result_blocks.push(probe_block);

                        matched_num = 0;
                    }
                }
                _ => {}
            }
        }
        if matched_num == 0 {
            return Ok(result_blocks);
        }
        let probe_block = DataBlock::take(input, &probe_indexes[0..matched_num], string_items_buf)?;
        result_blocks.push(probe_block);
        Ok(result_blocks)
    }

    pub(crate) fn left_semi_anti_join_with_conjunct<
        'a,
        const SEMI: bool,
        H: HashJoinHashtableLike,
        IT,
    >(
        &self,
        hash_table: &H,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        pointers: &[u64],
        input: &DataBlock,
        is_probe_projected: bool,
    ) -> Result<Vec<DataBlock>>
    where
        IT: Iterator<Item = &'a H::Key> + TrustedLen,
        H::Key: 'a,
    {
        let max_block_size = probe_state.max_block_size;
        let valids = probe_state.valids.as_ref();
        // The semi join will return multiple data chunks of similar size.
        let mut matched_num = 0;
        let mut result_blocks = vec![];
        let probe_indexes = &mut probe_state.probe_indexes;
        let build_indexes = &mut probe_state.build_indexes;
        let build_indexes_ptr = build_indexes.as_mut_ptr();
        let string_items_buf = &mut probe_state.string_items_buf;

        let build_columns = unsafe { &*self.hash_join_state.build_columns.get() };
        let build_columns_data_type =
            unsafe { &*self.hash_join_state.build_columns_data_type.get() };
        let build_num_rows = unsafe { &*self.hash_join_state.build_num_rows.get() };
        let is_build_projected = self
            .hash_join_state
            .is_build_projected
            .load(Ordering::Relaxed);

        let other_predicate = self
            .hash_join_state
            .hash_join_desc
            .other_predicate
            .as_ref()
            .unwrap();
        // For semi join, it defaults to all.
        let mut row_state = vec![0_u32; input.num_rows()];

        // The unmatched indices are in the range [unmatched_idx, input.num_rows()).
        let mut unmatched_idx = input.num_rows();
        for (i, (key, ptr)) in keys_iter.zip(pointers).enumerate() {
            let (mut match_count, mut incomplete_ptr) =
                if self.hash_join_state.hash_join_desc.from_correlated_subquery
                    || valids.map_or(true, |v| v.get_bit(i))
                {
                    hash_table.next_probe(key, *ptr, build_indexes_ptr, matched_num, max_block_size)
                } else {
                    (0, 0)
                };

            let true_match_count = match_count;
            match match_count > 0 {
                false if SEMI => {
                    continue;
                }
                false => {
                    unmatched_idx -= 1;
                    probe_indexes[unmatched_idx] = i as u32;
                    continue;
                }
                true => (),
            };

            if true_match_count > 0 && !SEMI {
                row_state[i] += true_match_count as u32;
            }

            for _ in 0..match_count {
                probe_indexes[matched_num] = i as u32;
                matched_num += 1;
            }
            if matched_num >= max_block_size {
                loop {
                    if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
                        return Err(ErrorCode::AbortedQuery(
                            "Aborted query, because the server is shutting down or the query was killed.",
                        ));
                    }

                    let probe_block = if is_probe_projected {
                        Some(DataBlock::take(input, probe_indexes, string_items_buf)?)
                    } else {
                        None
                    };
                    let build_block = if is_build_projected {
                        Some(self.hash_join_state.row_space.gather(
                            build_indexes,
                            build_columns,
                            build_columns_data_type,
                            build_num_rows,
                            string_items_buf,
                        )?)
                    } else {
                        None
                    };
                    let result_block =
                        self.merge_eq_block(probe_block.clone(), build_block, matched_num);

                    let mut bm = match self.get_other_filters(
                        &result_block,
                        other_predicate,
                        &self.func_ctx,
                    )? {
                        (Some(b), _, _) => b.make_mut(),
                        (_, true, _) => MutableBitmap::from_len_set(result_block.num_rows()),
                        (_, _, true) => MutableBitmap::from_len_zeroed(result_block.num_rows()),
                        _ => unreachable!(),
                    };

                    if SEMI {
                        self.fill_null_for_semi_join(&mut bm, probe_indexes, &mut row_state);
                    } else {
                        self.fill_null_for_anti_join(&mut bm, probe_indexes, &mut row_state);
                    }

                    if let Some(probe_block) = probe_block {
                        let result_block = DataBlock::filter_with_bitmap(probe_block, &bm.into())?;
                        if !result_block.is_empty() {
                            result_blocks.push(result_block);
                        }
                    }
                    matched_num = 0;

                    if incomplete_ptr == 0 {
                        break;
                    }
                    (match_count, incomplete_ptr) = hash_table.next_probe(
                        key,
                        incomplete_ptr,
                        build_indexes_ptr,
                        matched_num,
                        max_block_size,
                    );
                    if match_count == 0 {
                        break;
                    }

                    if !SEMI {
                        row_state[i] += match_count as u32;
                    }

                    for _ in 0..match_count {
                        probe_indexes[matched_num] = i as u32;
                        matched_num += 1;
                    }

                    if matched_num < max_block_size {
                        break;
                    }
                }
            }
        }

        // The unmatched indices are in the range [unmatched_idx, input.num_rows()).
        if unmatched_idx < input.num_rows() {
            result_blocks.push(DataBlock::take(
                input,
                &probe_indexes[unmatched_idx..input.num_rows()],
                string_items_buf,
            )?);
        }

        if matched_num == 0 {
            return Ok(result_blocks);
        }

        if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the server is shutting down or the query was killed.",
            ));
        }

        let probe_block = if is_probe_projected {
            Some(DataBlock::take(
                input,
                &probe_indexes[0..matched_num],
                string_items_buf,
            )?)
        } else {
            None
        };
        let build_block = if is_build_projected {
            Some(self.hash_join_state.row_space.gather(
                &build_indexes[0..matched_num],
                build_columns,
                build_columns_data_type,
                build_num_rows,
                string_items_buf,
            )?)
        } else {
            None
        };
        let result_block = self.merge_eq_block(probe_block.clone(), build_block, matched_num);

        let mut bm = match self.get_other_filters(&result_block, other_predicate, &self.func_ctx)? {
            (Some(b), _, _) => b.make_mut(),
            (_, true, _) => MutableBitmap::from_len_set(result_block.num_rows()),
            (_, _, true) => MutableBitmap::from_len_zeroed(result_block.num_rows()),
            _ => unreachable!(),
        };

        if SEMI {
            self.fill_null_for_semi_join(&mut bm, &probe_indexes[0..matched_num], &mut row_state);
        } else {
            self.fill_null_for_anti_join(&mut bm, &probe_indexes[0..matched_num], &mut row_state);
        }

        if let Some(probe_block) = probe_block {
            let result_block = DataBlock::filter_with_bitmap(probe_block, &bm.into())?;
            if !result_block.is_empty() {
                result_blocks.push(result_block);
            }
        }

        Ok(result_blocks)
    }

    // modify the bm by the value row_state
    // keep the index of the first positive state
    // bitmap: [1, 1, 1] with row_state [0, 0], probe_index: [(0, 3)] => [0, 0, 0] (repeat the first element 3 times)
    // bitmap will be [1, 1, 1] -> [1, 1, 1] -> [1, 0, 1] -> [1, 0, 0]
    // row_state will be [0, 0] -> [1, 0] -> [1,0] -> [1, 0]
    fn fill_null_for_semi_join(
        &self,
        bm: &mut MutableBitmap,
        probe_indexes: &[u32],
        row_state: &mut [u32],
    ) {
        for (index, row) in probe_indexes.iter().enumerate() {
            if bm.get(index) {
                if row_state[*row as usize] == 0 {
                    row_state[*row as usize] = 1;
                } else {
                    bm.set(index, false);
                }
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
        probe_indexes: &[u32],
        row_state: &mut [u32],
    ) {
        for (index, row) in probe_indexes.iter().enumerate() {
            if row_state[*row as usize] == 0 {
                // if state is not matched, anti result will take one
                bm.set(index, true);
            } else if row_state[*row as usize] == 1 {
                // if state has just one, anti reverse the result
                row_state[*row as usize] -= 1;
                bm.set(index, !bm.get(index))
            } else if !bm.get(index) {
                row_state[*row as usize] -= 1;
            } else {
                bm.set(index, false);
            }
        }
    }
}
