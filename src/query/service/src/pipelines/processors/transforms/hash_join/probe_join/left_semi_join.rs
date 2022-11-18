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

use std::iter::repeat;
use std::iter::TrustedLen;
use std::sync::atomic::Ordering;

use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::Chunk;
use common_hashtable::HashtableEntryRefLike;
use common_hashtable::HashtableLike;

use crate::pipelines::processors::transforms::hash_join::desc::JOIN_MAX_CHUNK_SIZE;
use crate::pipelines::processors::transforms::hash_join::row::RowPtr;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::JoinHashTable;

/// Semi join contain semi join and semi-anti join
impl JoinHashTable {
    pub(crate) fn probe_left_semi_join<'a, H: HashtableLike<Value = Vec<RowPtr>>, IT>(
        &self,
        hash_table: &H,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &Chunk,
    ) -> Result<Vec<Chunk>>
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

    pub(crate) fn probe_left_anti_semi_join<'a, H: HashtableLike<Value = Vec<RowPtr>>, IT>(
        &self,
        hash_table: &H,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &Chunk,
    ) -> Result<Vec<Chunk>>
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

    fn left_semi_anti_join<'a, const SEMI: bool, H: HashtableLike<Value = Vec<RowPtr>>, IT>(
        &self,
        hash_table: &H,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &Chunk,
    ) -> Result<Vec<Chunk>>
    where
        IT: Iterator<Item = &'a H::Key> + TrustedLen,
        H::Key: 'a,
    {
        let valids = &probe_state.valids;
        let mut probe_indexes = Vec::with_capacity(keys_iter.size_hint().0);

        for (i, key) in keys_iter.enumerate() {
            let probe_result_ptr = if self.hash_join_desc.from_correlated_subquery {
                hash_table.entry(key)
            } else {
                self.probe_key(hash_table, key, valids, i)
            };

            match (probe_result_ptr, SEMI) {
                (Some(_), true) | (None, false) => {
                    probe_indexes.push(i as u32);
                }
                _ => {}
            }
        }
        Ok(vec![Chunk::take(input, &probe_indexes)?])
    }

    fn left_semi_anti_join_with_other_conjunct<
        'a,
        const SEMI: bool,
        H: HashtableLike<Value = Vec<RowPtr>>,
        IT,
    >(
        &self,
        hash_table: &H,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &Chunk,
    ) -> Result<Vec<Chunk>>
    where
        IT: Iterator<Item = &'a H::Key> + TrustedLen,
        H::Key: 'a,
    {
        let valids = &probe_state.valids;
        // The semi join will return multiple data chunks of similar size
        let mut probed_chunks = vec![];
        let mut probe_indexes = Vec::with_capacity(JOIN_MAX_CHUNK_SIZE);
        let mut build_indexes = Vec::with_capacity(JOIN_MAX_CHUNK_SIZE);

        let other_predicate = self.hash_join_desc.other_predicate.as_ref().unwrap();
        // For semi join, it defaults to all
        let mut row_state = vec![0_u32; keys_iter.size_hint().0];
        let dummy_probed_rows = vec![RowPtr {
            chunk_index: 0,
            row_index: 0,
            marker: None,
        }];

        for (i, key) in keys_iter.enumerate() {
            let probe_result_ptr = match self.hash_join_desc.from_correlated_subquery {
                true => hash_table.entry(key),
                false => self.probe_key(hash_table, key, valids, i),
            };

            let probed_rows = match probe_result_ptr {
                None if SEMI => {
                    continue;
                }
                None => &dummy_probed_rows,
                Some(v) => v.get(),
            };

            if probe_result_ptr.is_some() && !SEMI {
                row_state[i] += probed_rows.len() as u32;
            }

            if probe_indexes.len() + probed_rows.len() < probe_indexes.capacity() {
                build_indexes.extend_from_slice(probed_rows);
                probe_indexes.extend(repeat(i as u32).take(probed_rows.len()));
            } else {
                let mut index = 0_usize;
                let mut remain = probed_rows.len();

                while index < probed_rows.len() {
                    if probe_indexes.len() + remain < probe_indexes.capacity() {
                        build_indexes.extend_from_slice(&probed_rows[index..]);
                        probe_indexes.extend(repeat(i as u32).take(remain));
                        index += remain;
                    } else {
                        if self.interrupt.load(Ordering::Relaxed) {
                            return Err(ErrorCode::AbortedQuery(
                                "Aborted query, because the server is shutting down or the query was killed.",
                            ));
                        }

                        let addition = probe_indexes.capacity() - probe_indexes.len();
                        let new_index = index + addition;

                        build_indexes.extend_from_slice(&probed_rows[index..new_index]);
                        probe_indexes.extend(repeat(i as u32).take(addition));

                        let probe_chunk = Chunk::take(input, &probe_indexes)?;
                        let build_chunk = self.row_space.gather(&build_indexes)?;
                        let merged_chunk = self.merge_eq_chunk(&build_chunk, &probe_chunk)?;

                        let mut bm = match self.get_other_filters(&merged_chunk, other_predicate)? {
                            (Some(b), _, _) => b.into_mut().right().unwrap(),
                            (_, true, _) => MutableBitmap::from_len_set(merged_chunk.num_rows()),
                            (_, _, true) => MutableBitmap::from_len_zeroed(merged_chunk.num_rows()),
                            _ => unreachable!(),
                        };

                        if SEMI {
                            self.fill_null_for_semi_join(&mut bm, &probe_indexes, &mut row_state);
                        } else {
                            self.fill_null_for_anti_join(&mut bm, &probe_indexes, &mut row_state);
                        }

                        let probed_data_chunk =
                            Chunk::filter_chunk_with_bitmap(probe_chunk, &bm.into())?;

                        if !probed_data_chunk.is_empty() {
                            probed_chunks.push(probed_data_chunk);
                        }

                        index = new_index;
                        remain -= addition;

                        build_indexes.clear();
                        probe_indexes.clear();
                    }
                }
            }
        }

        if self.interrupt.load(Ordering::Relaxed) {
            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the server is shutting down or the query was killed.",
            ));
        }

        let probe_chunk = Chunk::take(input, &probe_indexes)?;
        let build_chunk = self.row_space.gather(&build_indexes)?;
        let merged_chunk = self.merge_eq_chunk(&build_chunk, &probe_chunk)?;

        let mut bm = match self.get_other_filters(&merged_chunk, other_predicate)? {
            (Some(b), _, _) => b.into_mut().right().unwrap(),
            (_, true, _) => MutableBitmap::from_len_set(merged_chunk.num_rows()),
            (_, _, true) => MutableBitmap::from_len_zeroed(merged_chunk.num_rows()),
            _ => unreachable!(),
        };

        if SEMI {
            self.fill_null_for_semi_join(&mut bm, &probe_indexes, &mut row_state);
        } else {
            self.fill_null_for_anti_join(&mut bm, &probe_indexes, &mut row_state);
        }

        let probed_data_chunk = Chunk::filter_chunk_with_bitmap(probe_chunk, &bm.into())?;

        if !probed_data_chunk.is_empty() {
            probed_chunks.push(probed_data_chunk);
        }

        Ok(probed_chunks)
    }

    // modify the bm by the value row_state
    // keep the index of the first positive state
    // bitmap: [1, 1, 1] with row_state [0, 0], probe_index: [0, 0, 0] (repeat the first element 3 times)
    // bitmap will be [1, 1, 1] -> [1, 1, 1] -> [1, 0, 1] -> [1, 0, 0]
    // row_state will be [0, 0] -> [1, 0] -> [1,0] -> [1, 0]
    fn fill_null_for_semi_join(
        &self,
        bm: &mut MutableBitmap,
        probe_indexs: &[u32],
        row_state: &mut [u32],
    ) {
        for (index, row) in probe_indexs.iter().enumerate() {
            let row = *row as usize;
            if bm.get(index) {
                if row_state[row] == 0 {
                    row_state[row] = 1;
                } else {
                    bm.set(index, false);
                }
            }
        }
    }

    // keep the index of the negative state
    // bitmap: [1, 1, 1] with row_state [3, 0], probe_index: [0, 0, 0] (repeat the first element 3 times)
    // bitmap will be [1, 1, 1] -> [0, 1, 1] -> [0, 0, 1] -> [0, 0, 0]
    // row_state will be [3, 0] -> [3, 0] -> [3, 0] -> [3, 0]
    fn fill_null_for_anti_join(
        &self,
        bm: &mut MutableBitmap,
        probe_indexs: &[u32],
        row_state: &mut [u32],
    ) {
        for (index, row) in probe_indexs.iter().enumerate() {
            let row = *row as usize;
            if row_state[row] == 0 {
                // if state is not matched, anti result will take one
                bm.set(index, true);
            } else if row_state[row] == 1 {
                // if state has just one, anti reverse the result
                row_state[row] -= 1;
                bm.set(index, !bm.get(index))
            } else if !bm.get(index) {
                row_state[row] -= 1;
            } else {
                bm.set(index, false);
            }
        }
    }
}
