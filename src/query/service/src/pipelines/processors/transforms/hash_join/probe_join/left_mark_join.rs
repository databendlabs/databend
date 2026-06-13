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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::KeyAccessor;

use crate::pipelines::processors::transforms::hash_join::HashJoinProbeState;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::transforms::hash_join::build_state::BuildBlockGenerationState;
use crate::pipelines::processors::transforms::hash_join::desc::MARKER_KIND_FALSE;
use crate::pipelines::processors::transforms::hash_join::desc::MARKER_KIND_NULL;
use crate::pipelines::processors::transforms::hash_join::desc::MARKER_KIND_TRUE;
use crate::pipelines::processors::transforms::hash_join::probe_state::ProbeBlockGenerationState;
use crate::pipelines::processors::transforms::hash_join_table::HashJoinHashtableLike;
use crate::pipelines::processors::transforms::hash_join_table::RowPtr;

impl HashJoinProbeState {
    pub(crate) fn left_mark_join<'a, H: HashJoinHashtableLike>(
        &self,
        probe_state: &mut ProbeState,
        keys: Box<dyn KeyAccessor<Key = H::Key>>,
        hash_table: &H,
    ) -> Result<Vec<DataBlock>>
    where
        H::Key: 'a,
    {
        // Process States.
        let process_state = probe_state.process_state.as_mut().unwrap();

        // Probe states.
        let max_block_size = probe_state.max_block_size;
        // `probe_column` is the subquery result column.
        // For sql: select * from t1 where t1.a in (select t2.a from t2); t2.a is the `probe_column`,
        // Check if there is any null in the probe column.
        if process_state.probe_has_null {
            let mut has_null = self
                .hash_join_state
                .hash_join_desc
                .marker_join_desc
                .has_null
                .write();
            *has_null = true;
        }

        let mutable_indexes = &mut probe_state.mutable_indexes;
        let build_indexes = &mut mutable_indexes.build_indexes;
        let build_indexes_ptr = build_indexes.as_mut_ptr();
        let pointers = probe_state.hashes.as_slice();

        // Build states.
        // If find join partner, set the marker to true.
        let build_state = unsafe { &mut *self.hash_join_state.build_state.get() };
        let mark_scan_map = &mut build_state.mark_scan_map;

        // Results.
        let mut matched_idx = 0;

        // Probe hash table and update `mark_scan_map`.
        if probe_state.probe_with_selection {
            let selection = probe_state.selection.as_slice();
            for selection_idx in process_state.next_idx..probe_state.selection_count {
                let key_idx = unsafe { *selection.get_unchecked(selection_idx) } as usize;
                let key = unsafe { keys.key_unchecked(key_idx) };
                let ptr = unsafe { *pointers.get_unchecked(key_idx) };

                // Probe hash table and fill `build_indexes`.
                let (match_count, mut next_ptr) =
                    hash_table.next_probe(key, ptr, build_indexes_ptr, matched_idx, max_block_size);
                if match_count == 0 {
                    continue;
                }

                matched_idx += match_count;

                while matched_idx == max_block_size {
                    self.process_left_mark_join_block(build_indexes, mark_scan_map)?;
                    (matched_idx, next_ptr) =
                        hash_table.next_probe(key, next_ptr, build_indexes_ptr, 0, max_block_size);
                }
            }
        } else {
            for key_idx in process_state.next_idx..process_state.input.num_rows() {
                let key = unsafe { keys.key_unchecked(key_idx) };
                let ptr = unsafe { *pointers.get_unchecked(key_idx) };

                // Probe hash table and fill build_indexes.
                let (match_count, mut next_ptr) =
                    hash_table.next_probe(key, ptr, build_indexes_ptr, matched_idx, max_block_size);
                if match_count == 0 {
                    continue;
                }

                matched_idx += match_count;

                while matched_idx == max_block_size {
                    self.process_left_mark_join_block(build_indexes, mark_scan_map)?;
                    (matched_idx, next_ptr) =
                        hash_table.next_probe(key, next_ptr, build_indexes_ptr, 0, max_block_size);
                }
            }
        }

        if matched_idx > 0 {
            self.process_left_mark_join_block(&build_indexes[0..matched_idx], mark_scan_map)?;
        }

        probe_state.process_state = None;

        Ok(vec![])
    }

    pub(crate) fn left_mark_join_with_conjunct<'a, H: HashJoinHashtableLike>(
        &self,
        probe_state: &mut ProbeState,
        keys: Box<dyn KeyAccessor<Key = H::Key>>,
        hash_table: &H,
    ) -> Result<Vec<DataBlock>>
    where
        H::Key: 'a,
    {
        // Process States.
        let process_state = probe_state.process_state.as_mut().unwrap();

        // Probe states.
        let max_block_size = probe_state.max_block_size;
        // `probe_column` is the subquery result column.
        // For sql: select * from t1 where t1.a in (select t2.a from t2); t2.a is the `probe_column`,
        // Check if there is any null in the probe column.
        if process_state.probe_has_null {
            let mut has_null = self
                .hash_join_state
                .hash_join_desc
                .marker_join_desc
                .has_null
                .write();
            *has_null = true;
        }
        let mutable_indexes = &mut probe_state.mutable_indexes;
        let probe_indexes = &mut mutable_indexes.probe_indexes;
        let build_indexes = &mut mutable_indexes.build_indexes;
        let build_indexes_ptr = build_indexes.as_mut_ptr();
        let pointers = probe_state.hashes.as_slice();

        // Build states.
        let build_state = unsafe { &mut *self.hash_join_state.build_state.get() };
        let mark_scan_map = &mut build_state.mark_scan_map;
        let _mark_scan_map_lock = self.mark_scan_map_lock.lock();
        let other_predicate = self
            .hash_join_state
            .hash_join_desc
            .other_predicate
            .as_ref()
            .unwrap();

        // Results.
        let mut matched_idx = 0;

        // Probe hash table and generate data blocks.
        if probe_state.probe_with_selection {
            let selection = probe_state.selection.as_slice();
            for selection_idx in process_state.next_idx..probe_state.selection_count {
                let key_idx = unsafe { *selection.get_unchecked(selection_idx) };
                let key = unsafe { keys.key_unchecked(key_idx as usize) };
                let ptr = unsafe { *pointers.get_unchecked(key_idx as usize) };

                // Probe hash table and fill `build_indexes`.
                let (match_count, mut next_ptr) =
                    hash_table.next_probe(key, ptr, build_indexes_ptr, matched_idx, max_block_size);
                if match_count == 0 {
                    continue;
                }

                // Fill `probe_indexes`.
                for _ in 0..match_count {
                    unsafe { *probe_indexes.get_unchecked_mut(matched_idx) = key_idx };
                    matched_idx += 1;
                }

                while matched_idx == max_block_size {
                    self.process_left_mark_join_with_conjunct_block(
                        matched_idx,
                        &process_state.input,
                        probe_indexes,
                        build_indexes,
                        &mut probe_state.generation_state,
                        &build_state.generation_state,
                        other_predicate,
                        mark_scan_map,
                    )?;
                    (matched_idx, next_ptr) = self.fill_probe_and_build_indexes::<_, false>(
                        hash_table,
                        key,
                        next_ptr,
                        key_idx,
                        probe_indexes,
                        build_indexes_ptr,
                        max_block_size,
                    )?;
                }
            }
        } else {
            for key_idx in process_state.next_idx..process_state.input.num_rows() {
                let key = unsafe { keys.key_unchecked(key_idx) };
                let ptr = unsafe { *pointers.get_unchecked(key_idx) };

                // Probe hash table and fill build_indexes.
                let (match_count, mut next_ptr) =
                    hash_table.next_probe(key, ptr, build_indexes_ptr, matched_idx, max_block_size);
                if match_count == 0 {
                    continue;
                }

                // Fill probe_indexes.
                for _ in 0..match_count {
                    unsafe { *probe_indexes.get_unchecked_mut(matched_idx) = key_idx as u32 };
                    matched_idx += 1;
                }

                while matched_idx == max_block_size {
                    self.process_left_mark_join_with_conjunct_block(
                        matched_idx,
                        &process_state.input,
                        probe_indexes,
                        build_indexes,
                        &mut probe_state.generation_state,
                        &build_state.generation_state,
                        other_predicate,
                        mark_scan_map,
                    )?;
                    (matched_idx, next_ptr) = self.fill_probe_and_build_indexes::<_, false>(
                        hash_table,
                        key,
                        next_ptr,
                        key_idx as u32,
                        probe_indexes,
                        build_indexes_ptr,
                        max_block_size,
                    )?;
                }
            }
        }

        if self.hash_join_state.hash_join_desc.from_correlated_subquery {
            // Must be correlated ANY subquery, we won't need to check `has_null` in `mark_join_blocks`.
            // In the following, if value is Null and Marker is False, we'll set the marker to Null
            let mut has_null = self
                .hash_join_state
                .hash_join_desc
                .marker_join_desc
                .has_null
                .write();
            *has_null = false;
        }

        if matched_idx > 0 {
            self.process_left_mark_join_with_conjunct_block(
                matched_idx,
                &process_state.input,
                probe_indexes,
                build_indexes,
                &mut probe_state.generation_state,
                &build_state.generation_state,
                other_predicate,
                mark_scan_map,
            )?;
        }

        probe_state.process_state = None;

        Ok(vec![])
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn process_left_mark_join_block(
        &self,
        build_indexes: &[RowPtr],
        mark_scan_map: &mut [Vec<u8>],
    ) -> Result<()> {
        if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
            return Err(ErrorCode::aborting());
        }

        for probed_row in build_indexes.iter() {
            unsafe {
                *mark_scan_map
                    .get_unchecked_mut(probed_row.chunk_index as usize)
                    .get_unchecked_mut(probed_row.row_index as usize) = MARKER_KIND_TRUE;
            }
        }

        Ok(())
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn process_left_mark_join_with_conjunct_block(
        &self,
        matched_idx: usize,
        input: &DataBlock,
        probe_indexes: &[u32],
        build_indexes: &[RowPtr],
        probe_state: &mut ProbeBlockGenerationState,
        build_state: &BuildBlockGenerationState,
        other_predicate: &Expr,
        mark_scan_map: &mut [Vec<u8>],
    ) -> Result<()> {
        if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
            return Err(ErrorCode::aborting());
        }

        let probe_block = if probe_state.is_probe_projected {
            Some(DataBlock::take(input, &probe_indexes[0..matched_idx])?)
        } else {
            None
        };
        let build_block = if build_state.is_build_projected {
            Some(self.hash_join_state.gather(
                &build_indexes[0..matched_idx],
                &build_state.build_columns,
                &build_state.build_columns_data_type,
                &build_state.build_num_rows,
            )?)
        } else {
            None
        };

        let result_block = self.merge_eq_block(probe_block, build_block, matched_idx);

        let filter_viewer =
            self.get_nullable_filter_column(&result_block, other_predicate, &self.func_ctx)?;
        let validity = &filter_viewer.validity;
        let data = &filter_viewer.column;

        for (idx, build_index) in build_indexes[0..matched_idx].iter().enumerate() {
            unsafe {
                if !validity.get_bit_unchecked(idx) {
                    if *mark_scan_map
                        .get_unchecked(build_index.chunk_index as usize)
                        .get_unchecked(build_index.row_index as usize)
                        == MARKER_KIND_FALSE
                    {
                        *mark_scan_map
                            .get_unchecked_mut(build_index.chunk_index as usize)
                            .get_unchecked_mut(build_index.row_index as usize) = MARKER_KIND_NULL;
                    }
                } else if data.get_bit_unchecked(idx) {
                    *mark_scan_map
                        .get_unchecked_mut(build_index.chunk_index as usize)
                        .get_unchecked_mut(build_index.row_index as usize) = MARKER_KIND_TRUE;
                }
            }
        }

        Ok(())
    }
}
