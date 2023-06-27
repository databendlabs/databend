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

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::BooleanType;
use common_expression::types::NullableType;
use common_expression::types::ValueType;
use common_expression::DataBlock;
use common_hashtable::HashJoinHashtableLike;

use crate::pipelines::processors::transforms::hash_join::desc::JOIN_MAX_BLOCK_SIZE;
use crate::pipelines::processors::transforms::hash_join::desc::MARKER_KIND_FALSE;
use crate::pipelines::processors::transforms::hash_join::desc::MARKER_KIND_NULL;
use crate::pipelines::processors::transforms::hash_join::desc::MARKER_KIND_TRUE;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::JoinHashTable;

impl JoinHashTable {
    pub(crate) fn probe_left_mark_join<'a, H: HashJoinHashtableLike, IT>(
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
        let mut block_size = JOIN_MAX_BLOCK_SIZE;
        // `probe_column` is the subquery result column.
        // For sql: select * from t1 where t1.a in (select t2.a from t2); t2.a is the `probe_column`,
        let probe_column = input.get_by_offset(0).value.as_column().unwrap();
        // Check if there is any null in the probe column.
        if matches!(probe_column.validity().1, Some(x) if x.unset_bits() > 0) {
            let mut has_null = self.hash_join_desc.marker_join_desc.has_null.write();
            *has_null = true;
        }
        let mut occupied = 0;
        let build_index = &mut probe_state.build_indexes;
        let build_indexes_ptr = build_index.as_mut_ptr();

        // If find join partner, set the marker to true.
        let mark_scan_map = unsafe { &mut *self.mark_scan_map.get() };

        for (i, key) in keys_iter.enumerate() {
            if (i & block_size) == 0 {
                block_size <<= 1;

                if self.interrupt.load(Ordering::Relaxed) {
                    return Err(ErrorCode::AbortedQuery(
                        "Aborted query, because the server is shutting down or the query was killed.",
                    ));
                }
            }

            let (mut match_count, mut incomplete_ptr) = match self
                .hash_join_desc
                .from_correlated_subquery
            {
                true => hash_table.probe_hash_table(
                    key,
                    build_indexes_ptr,
                    occupied,
                    JOIN_MAX_BLOCK_SIZE,
                ),
                false => self.probe_key(hash_table, key, valids, i, build_indexes_ptr, occupied),
            };
            if match_count == 0 {
                continue;
            }
            occupied += match_count;
            loop {
                for probed_row in &build_index[0..occupied] {
                    mark_scan_map[probed_row.chunk_index as usize][probed_row.row_index as usize] =
                        MARKER_KIND_TRUE;
                }
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
                occupied += match_count;
            }
        }

        Ok(vec![])
    }

    pub(crate) fn probe_left_mark_join_with_conjunct<'a, H: HashJoinHashtableLike, IT>(
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

        // `probe_column` is the subquery result column.
        // For sql: select * from t1 where t1.a in (select t2.a from t2); t2.a is the `probe_column`,
        let probe_column = input.get_by_offset(0).value.as_column().unwrap();
        // Check if there is any null in the probe column.
        if matches!(probe_column.validity().1, Some(x) if x.unset_bits() > 0) {
            let mut has_null = self.hash_join_desc.marker_join_desc.has_null.write();
            *has_null = true;
        }

        let _func_ctx = self.ctx.get_function_context()?;
        let other_predicate = self.hash_join_desc.other_predicate.as_ref().unwrap();

        let mut occupied = 0;
        let mut probe_indexes_len = 0;
        let probe_indexes = &mut probe_state.probe_indexes;
        let build_indexes = &mut probe_state.build_indexes;
        let build_indexes_ptr = build_indexes.as_mut_ptr();

        let data_blocks = self.row_space.chunks.read();
        let data_blocks = data_blocks
            .iter()
            .map(|c| &c.data_block)
            .collect::<Vec<_>>();
        let num_rows = data_blocks
            .iter()
            .fold(0, |acc, chunk| acc + chunk.num_rows());

        let mark_scan_map = unsafe { &mut *self.mark_scan_map.get() };
        let _mark_scan_map_lock = self.mark_scan_map_lock.lock();

        for (i, key) in keys_iter.enumerate() {
            let (mut match_count, mut incomplete_ptr) = if self
                .hash_join_desc
                .from_correlated_subquery
            {
                hash_table.probe_hash_table(key, build_indexes_ptr, occupied, JOIN_MAX_BLOCK_SIZE)
            } else {
                self.probe_key(hash_table, key, valids, i, build_indexes_ptr, occupied)
            };
            if match_count == 0 {
                continue;
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

                    let filter = self.get_nullable_filter_column(&merged_block, other_predicate)?;
                    let filter_viewer =
                        NullableType::<BooleanType>::try_downcast_column(&filter).unwrap();
                    let validity = &filter_viewer.validity;
                    let data = &filter_viewer.column;

                    for (idx, build_index) in build_indexes.iter().enumerate() {
                        if !validity.get_bit(idx) {
                            if mark_scan_map[build_index.chunk_index as usize]
                                [build_index.row_index as usize]
                                == MARKER_KIND_FALSE
                            {
                                mark_scan_map[build_index.chunk_index as usize]
                                    [build_index.row_index as usize] = MARKER_KIND_NULL;
                            }
                        } else if data.get_bit(idx) {
                            mark_scan_map[build_index.chunk_index as usize]
                                [build_index.row_index as usize] = MARKER_KIND_TRUE;
                        }
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

                    occupied += match_count;
                    probe_indexes[probe_indexes_len] = (i as u32, match_count as u32);
                    probe_indexes_len += 1;

                    if occupied < JOIN_MAX_BLOCK_SIZE {
                        break;
                    }
                }
            }
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

        let filter = self.get_nullable_filter_column(&merged_block, other_predicate)?;
        let filter_viewer = NullableType::<BooleanType>::try_downcast_column(&filter).unwrap();
        let validity = &filter_viewer.validity;
        let data = &filter_viewer.column;

        for (idx, build_index) in (build_indexes[0..occupied]).iter().enumerate() {
            if !validity.get_bit(idx) {
                if mark_scan_map[build_index.chunk_index as usize][build_index.row_index as usize]
                    == MARKER_KIND_FALSE
                {
                    mark_scan_map[build_index.chunk_index as usize]
                        [build_index.row_index as usize] = MARKER_KIND_NULL;
                }
            } else if data.get_bit(idx) {
                mark_scan_map[build_index.chunk_index as usize][build_index.row_index as usize] =
                    MARKER_KIND_TRUE;
            }
        }

        if self.hash_join_desc.from_correlated_subquery {
            // Must be correlated ANY subquery, we won't need to check `has_null` in `mark_join_blocks`.
            // In the following, if value is Null and Marker is False, we'll set the marker to Null
            let mut has_null = self.hash_join_desc.marker_join_desc.has_null.write();
            *has_null = false;
        }

        Ok(vec![])
    }
}
