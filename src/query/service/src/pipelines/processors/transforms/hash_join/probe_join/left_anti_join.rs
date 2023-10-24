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

use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::KeyAccessor;
use common_hashtable::HashJoinHashtableLike;
use common_hashtable::RowPtr;

use crate::pipelines::processors::transforms::hash_join::HashJoinProbeState;
use crate::pipelines::processors::transforms::hash_join::ProbeState;

impl HashJoinProbeState {
    pub(crate) fn left_anti_join<'a, H: HashJoinHashtableLike>(
        &self,
        input: &DataBlock,
        keys: Box<(dyn KeyAccessor<Key = H::Key>)>,
        hash_table: &H,
        probe_state: &mut ProbeState,
    ) -> Result<Vec<DataBlock>>
    where
        H::Key: 'a,
    {
        // If there is no build key, the result is input
        // Eg: select * from onecolumn as a right semi join twocolumn as b on true order by b.x
        // Probe states.
        let input_rows = input.num_rows();
        let max_block_size = probe_state.max_block_size;
        let probe_indexes = &mut probe_state.probe_indexes;
        let pointers = probe_state.hashes.as_slice();
        let string_items_buf = &mut probe_state.string_items_buf;

        // Results.
        let mut matched_num = 0;
        let mut result_blocks = vec![];

        for idx in 0..input_rows {
            let key = unsafe { keys.key_unchecked(idx) };
            let ptr = unsafe { *pointers.get_unchecked(idx) };
            if !hash_table.next_contains(key, ptr) {
                probe_indexes[matched_num] = idx as u32;
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
        }
        if matched_num == 0 {
            return Ok(result_blocks);
        }
        let probe_block = DataBlock::take(input, &probe_indexes[0..matched_num], string_items_buf)?;
        result_blocks.push(probe_block);
        Ok(result_blocks)
    }

    pub(crate) fn left_anti_join_with_conjunct<'a, H: HashJoinHashtableLike>(
        &self,
        input: &DataBlock,
        keys: Box<(dyn KeyAccessor<Key = H::Key>)>,
        hash_table: &H,
        probe_state: &mut ProbeState,
    ) -> Result<Vec<DataBlock>>
    where
        H::Key: 'a,
    {
        // Probe states.
        let input_rows = input.num_rows();
        let max_block_size = probe_state.max_block_size;
        let probe_indexes = &mut probe_state.probe_indexes;
        let build_indexes = &mut probe_state.build_indexes;
        let build_indexes_ptr = build_indexes.as_mut_ptr();

        let pointers = probe_state.hashes.as_slice();
        let is_probe_projected = probe_state.is_probe_projected;
        let string_items_buf = &mut probe_state.string_items_buf;

        // Build states.
        let build_columns = unsafe { &*self.hash_join_state.build_columns.get() };
        let build_columns_data_type =
            unsafe { &*self.hash_join_state.build_columns_data_type.get() };
        let build_num_rows = unsafe { &*self.hash_join_state.build_num_rows.get() };
        let is_build_projected = self
            .hash_join_state
            .is_build_projected
            .load(Ordering::Relaxed);

        // For semi join, it defaults to all.
        let mut row_state = vec![0_u32; input.num_rows()];
        let dummy_probed_row = RowPtr {
            chunk_index: 0,
            row_index: 0,
        };
        let other_predicate = self
            .hash_join_state
            .hash_join_desc
            .other_predicate
            .as_ref()
            .unwrap();

        // Results.
        let mut matched_num = 0;
        let mut result_blocks = vec![];

        for idx in 0..input_rows {
            let key = unsafe { keys.key_unchecked(idx) };
            let ptr = unsafe { *pointers.get_unchecked(idx) };
            let (mut match_count, mut incomplete_ptr) =
                hash_table.next_probe(key, ptr, build_indexes_ptr, matched_num, max_block_size);

            let true_match_count = match_count;
            if match_count == 0 {
                // dummy_probed_row
                unsafe { std::ptr::write(build_indexes_ptr.add(matched_num), dummy_probed_row) };
                match_count = 1;
            }

            if true_match_count > 0 {
                row_state[idx] += true_match_count as u32;
            }

            for _ in 0..match_count {
                probe_indexes[matched_num] = idx as u32;
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

                    self.fill_null_for_anti_join(&mut bm, probe_indexes, &mut row_state);

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

                    row_state[idx] += match_count as u32;

                    for _ in 0..match_count {
                        probe_indexes[matched_num] = idx as u32;
                        matched_num += 1;
                    }

                    if matched_num < max_block_size {
                        break;
                    }
                }
            }
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

        self.fill_null_for_anti_join(&mut bm, &probe_indexes[0..matched_num], &mut row_state);

        if let Some(probe_block) = probe_block {
            let result_block = DataBlock::filter_with_bitmap(probe_block, &bm.into())?;
            if !result_block.is_empty() {
                result_blocks.push(result_block);
            }
        }

        Ok(result_blocks)
    }

    // keep the index of the negative state
    // bitmap: [1, 1, 1] with row_state [3, 0], probe_index: [(0, 3)] => [0, 0, 0] (repeat the first element 3 times)
    // bitmap will be [1, 1, 1] -> [0, 1, 1] -> [0, 0, 1] -> [0, 0, 0]
    // row_state will be [3, 0] -> [3, 0] -> [3, 0] -> [3, 0]
    pub(crate) fn fill_null_for_anti_join(
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
