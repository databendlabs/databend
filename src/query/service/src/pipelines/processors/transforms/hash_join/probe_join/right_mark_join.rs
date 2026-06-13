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
    pub(crate) fn right_mark_join<'a, H: HashJoinHashtableLike>(
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
        let has_null = *self
            .hash_join_state
            .hash_join_desc
            .marker_join_desc
            .has_null
            .read();
        let markers = probe_state.markers.as_mut().unwrap();
        let pointers = probe_state.hashes.as_slice();

        if probe_state.probe_with_selection {
            let selection = probe_state.selection.as_slice();
            for selection_idx in process_state.next_idx..probe_state.selection_count {
                let key_idx = unsafe { *selection.get_unchecked(selection_idx) };
                let key = unsafe { keys.key_unchecked(key_idx as usize) };
                let ptr = unsafe { *pointers.get_unchecked(key_idx as usize) };
                if hash_table.next_contains(key, ptr) {
                    unsafe { *markers.get_unchecked_mut(key_idx as usize) = MARKER_KIND_TRUE };
                }
            }
        } else {
            for key_idx in process_state.next_idx..process_state.input.num_rows() {
                let key = unsafe { keys.key_unchecked(key_idx) };
                let ptr = unsafe { *pointers.get_unchecked(key_idx) };
                if hash_table.next_contains(key, ptr) {
                    unsafe { *markers.get_unchecked_mut(key_idx) = MARKER_KIND_TRUE };
                }
            }
        }

        let probe_block = if probe_state.generation_state.is_probe_projected {
            Some(process_state.input.clone())
        } else {
            None
        };
        let input_num_rows = process_state.input.num_rows();
        let marker_block = Some(self.create_marker_block(has_null, markers, input_num_rows)?);

        probe_state.process_state = None;

        Ok(vec![self.merge_eq_block(
            probe_block,
            marker_block,
            input_num_rows,
        )])
    }

    pub(crate) fn right_mark_join_with_conjunct<'a, H: HashJoinHashtableLike>(
        &self,
        probe_state: &mut ProbeState,
        keys: Box<dyn KeyAccessor<Key = H::Key>>,
        hash_table: &H,
    ) -> Result<Vec<DataBlock>>
    where
        H::Key: 'a,
    {
        let has_null = *self
            .hash_join_state
            .hash_join_desc
            .marker_join_desc
            .has_null
            .read();
        let other_predicate = self
            .hash_join_state
            .hash_join_desc
            .other_predicate
            .as_ref()
            .unwrap();

        // Process States.
        let process_state = probe_state.process_state.as_mut().unwrap();

        // Probe states.
        let max_block_size = probe_state.max_block_size;
        let mutable_indexes = &mut probe_state.mutable_indexes;
        let probe_indexes = &mut mutable_indexes.probe_indexes;
        let build_indexes = &mut mutable_indexes.build_indexes;
        let build_indexes_ptr = build_indexes.as_mut_ptr();
        let pointers = probe_state.hashes.as_slice();
        let input_num_rows = process_state.input.num_rows();
        let markers = probe_state.markers.as_mut().unwrap();
        self.hash_join_state.init_markers(
            process_state.input.columns().into(),
            input_num_rows,
            markers,
        );

        // Build states.
        let build_state = unsafe { &*self.hash_join_state.build_state.get() };

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
                    self.process_right_mark_join_block(
                        matched_idx,
                        &process_state.input,
                        probe_indexes,
                        build_indexes,
                        &mut probe_state.generation_state,
                        &build_state.generation_state,
                        markers,
                        other_predicate,
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
                    self.process_right_mark_join_block(
                        matched_idx,
                        &process_state.input,
                        probe_indexes,
                        build_indexes,
                        &mut probe_state.generation_state,
                        &build_state.generation_state,
                        markers,
                        other_predicate,
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

        if matched_idx > 0 {
            self.process_right_mark_join_block(
                matched_idx,
                &process_state.input,
                probe_indexes,
                build_indexes,
                &mut probe_state.generation_state,
                &build_state.generation_state,
                markers,
                other_predicate,
            )?;
        }

        let probe_block = if probe_state.generation_state.is_probe_projected {
            Some(process_state.input.clone())
        } else {
            None
        };
        let marker_block = Some(self.create_marker_block(has_null, markers, input_num_rows)?);

        probe_state.process_state = None;

        Ok(vec![self.merge_eq_block(
            probe_block,
            marker_block,
            input_num_rows,
        )])
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn process_right_mark_join_block(
        &self,
        matched_idx: usize,
        input: &DataBlock,
        probe_indexes: &[u32],
        build_indexes: &[RowPtr],
        probe_state: &mut ProbeBlockGenerationState,
        build_state: &BuildBlockGenerationState,
        markers: &mut [u8],
        other_predicate: &Expr,
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

        for (idx, index) in probe_indexes[0..matched_idx].iter().enumerate() {
            let marker = unsafe { markers.get_unchecked_mut(*index as usize) };
            if unsafe { !validity.get_bit_unchecked(idx) } {
                if *marker == MARKER_KIND_FALSE {
                    *marker = MARKER_KIND_NULL;
                }
            } else if unsafe { data.get_bit_unchecked(idx) } {
                *marker = MARKER_KIND_TRUE;
            }
        }

        Ok(())
    }
}
