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

use common_arrow::arrow::bitmap::MutableBitmap;
use common_datablocks::DataBlock;
use common_datavalues::BooleanColumn;
use common_datavalues::Column;
use common_exception::Result;
use common_hashtable::HashMap;
use common_hashtable::HashTableKeyable;

use crate::pipelines::processors::transforms::hash_join::row::RowPtr;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::JoinHashTable;

/// Semi join contain semi join and semi-anti join
impl JoinHashTable {
    pub(crate) fn probe_left_semi_join<Key, IT>(
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
        if self.hash_join_desc.other_predicate.is_none() {
            Ok(vec![self.left_semi_anti_join::<true, _, _>(
                hash_table,
                probe_state,
                keys_iter,
                input,
            )?])
        } else {
            Ok(vec![
                self.left_semi_anti_join_with_other_conjunct::<true, _, _>(
                    hash_table,
                    probe_state,
                    keys_iter,
                    input,
                )?,
            ])
        }
    }

    pub(crate) fn probe_left_anti_semi_join<Key, IT>(
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
        if self.hash_join_desc.other_predicate.is_none() {
            Ok(vec![self.left_semi_anti_join::<false, _, _>(
                hash_table,
                probe_state,
                keys_iter,
                input,
            )?])
        } else {
            Ok(vec![
                self.left_semi_anti_join_with_other_conjunct::<false, _, _>(
                    hash_table,
                    probe_state,
                    keys_iter,
                    input,
                )?,
            ])
        }
    }

    fn left_semi_anti_join<const SEMI: bool, Key, IT>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &DataBlock,
    ) -> Result<DataBlock>
    where
        Key: HashTableKeyable + Clone + 'static,
        IT: Iterator<Item = Key> + TrustedLen,
    {
        let probe_indexs = &mut probe_state.probe_indexs;
        let valids = &probe_state.valids;

        for (i, key) in keys_iter.enumerate() {
            let probe_result_ptr = if self.hash_join_desc.from_correlated_subquery {
                hash_table.find_key(&key)
            } else {
                self.probe_key(hash_table, key, valids, i)
            };

            match (probe_result_ptr, SEMI) {
                (Some(_), true) | (None, false) => {
                    probe_indexs.push(i as u32);
                }
                _ => {}
            }
        }
        DataBlock::block_take_by_indices(input, probe_indexs)
    }

    fn left_semi_anti_join_with_other_conjunct<const SEMI: bool, Key, IT>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &DataBlock,
    ) -> Result<DataBlock>
    where
        Key: HashTableKeyable + Clone + 'static,
        IT: Iterator<Item = Key> + TrustedLen,
    {
        let probe_indexs = &mut probe_state.probe_indexs;
        let build_indexs = &mut probe_state.build_indexs;
        let valids = &probe_state.valids;
        let row_state = &mut probe_state.row_state;

        // For semi join, it defaults to all
        row_state.resize(keys_iter.size_hint().0, 0);

        let mut dummys = 0;

        for (i, key) in keys_iter.enumerate() {
            let probe_result_ptr = if self.hash_join_desc.from_correlated_subquery {
                hash_table.find_key(&key)
            } else {
                self.probe_key(hash_table, key, valids, i)
            };

            match (probe_result_ptr, SEMI) {
                (Some(v), _) => {
                    let probe_result_ptrs = v.get_value();
                    build_indexs.extend_from_slice(probe_result_ptrs);
                    probe_indexs.extend(repeat(i as u32).take(probe_result_ptrs.len()));

                    if !SEMI {
                        row_state[i] += probe_result_ptrs.len() as u32;
                    }
                }

                (None, false) => {
                    // dummy row ptr
                    build_indexs.push(RowPtr {
                        chunk_index: 0,
                        row_index: 0,
                        marker: None,
                    });
                    probe_indexs.push(i as u32);

                    dummys += 1;
                    // must not be filtered out， so we should not increase the row_state for anti join
                    // row_state[i] += 1;
                }
                _ => {}
            }
        }
        let probe_block = DataBlock::block_take_by_indices(input, probe_indexs)?;
        // faster path for anti join
        if dummys == probe_indexs.len() {
            return Ok(probe_block);
        }

        let build_block = self.row_space.gather(build_indexs)?;
        let merged_block = self.merge_eq_block(&build_block, &probe_block)?;

        let (bm, all_true, all_false) = self.get_other_filters(
            &merged_block,
            self.hash_join_desc.other_predicate.as_ref().unwrap(),
        )?;

        let mut bm = match (bm, all_true, all_false) {
            (Some(b), _, _) => b.into_mut().right().unwrap(),
            (_, true, _) => MutableBitmap::from_len_set(merged_block.num_rows()),
            (_, _, true) => MutableBitmap::from_len_zeroed(merged_block.num_rows()),
            // must be one of above
            _ => unreachable!(),
        };

        if SEMI {
            self.fill_null_for_semi_join(&mut bm, probe_indexs, row_state);
        } else {
            self.fill_null_for_anti_join(&mut bm, probe_indexs, row_state);
        }

        let predicate = BooleanColumn::from_arrow_data(bm.into()).arc();
        DataBlock::filter_block(probe_block, &predicate)
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
