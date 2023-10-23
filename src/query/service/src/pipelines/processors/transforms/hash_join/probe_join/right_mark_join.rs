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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::BooleanType;
use common_expression::types::NullableType;
use common_expression::types::ValueType;
use common_expression::DataBlock;
use common_expression::KeyAccessor;
use common_hashtable::HashJoinHashtableLike;

use crate::pipelines::processors::transforms::hash_join::desc::MARKER_KIND_FALSE;
use crate::pipelines::processors::transforms::hash_join::desc::MARKER_KIND_NULL;
use crate::pipelines::processors::transforms::hash_join::desc::MARKER_KIND_TRUE;
use crate::pipelines::processors::transforms::hash_join::HashJoinProbeState;
use crate::pipelines::processors::transforms::hash_join::ProbeState;

impl HashJoinProbeState {
    pub(crate) fn probe_right_mark_join<'a, H: HashJoinHashtableLike>(
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
        let has_null = *self
            .hash_join_state
            .hash_join_desc
            .marker_join_desc
            .has_null
            .read();
        let markers = probe_state.markers.as_mut().unwrap();
        let pointers = probe_state.hashes.as_slice();
        let selection = &probe_state.selection.as_slice()[0..probe_state.selection_count];
        let is_probe_projected = probe_state.is_probe_projected;

        for idx in selection.iter() {
            let key = unsafe { keys.key_unchecked(*idx as usize) };
            let ptr = unsafe { *pointers.get_unchecked(*idx as usize) };
            if hash_table.next_contains(key, ptr) {
                markers[*idx as usize] = MARKER_KIND_TRUE;
            }
        }

        let probe_block = if is_probe_projected {
            Some(input.clone())
        } else {
            None
        };
        let marker_block = Some(self.create_marker_block(has_null, markers, input.num_rows())?);

        Ok(vec![self.merge_eq_block(
            probe_block,
            marker_block,
            input.num_rows(),
        )])
    }

    pub(crate) fn probe_right_mark_join_with_conjunct<'a, H: HashJoinHashtableLike>(
        &self,
        input: &DataBlock,
        keys: Box<(dyn KeyAccessor<Key = H::Key>)>,
        hash_table: &H,
        probe_state: &mut ProbeState,
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

        // Probe states.
        let max_block_size = probe_state.max_block_size;
        let probe_indexes = &mut probe_state.probe_indexes;
        let build_indexes = &mut probe_state.build_indexes;
        let build_indexes_ptr = build_indexes.as_mut_ptr();
        let pointers = probe_state.hashes.as_slice();
        let selection = &probe_state.selection.as_slice()[0..probe_state.selection_count];
        let is_probe_projected = probe_state.is_probe_projected;
        let string_items_buf = &mut probe_state.string_items_buf;
        let cols = input
            .columns()
            .iter()
            .map(|c| (c.value.as_column().unwrap().clone(), c.data_type.clone()))
            .collect::<Vec<_>>();
        let markers = probe_state.markers.as_mut().unwrap();
        self.hash_join_state
            .init_markers(&cols, input.num_rows(), markers);

        // Build states.
        let build_columns = unsafe { &*self.hash_join_state.build_columns.get() };
        let build_columns_data_type =
            unsafe { &*self.hash_join_state.build_columns_data_type.get() };
        let build_num_rows = unsafe { &*self.hash_join_state.build_num_rows.get() };
        let is_build_projected = self
            .hash_join_state
            .is_build_projected
            .load(Ordering::Relaxed);

        let mut matched_num = 0;

        for idx in selection.iter() {
            let key = unsafe { keys.key_unchecked(*idx as usize) };
            let ptr = unsafe { *pointers.get_unchecked(*idx as usize) };
            let (mut match_count, mut incomplete_ptr) =
                hash_table.next_probe(key, ptr, build_indexes_ptr, matched_num, max_block_size);

            if match_count == 0 {
                continue;
            }

            for _ in 0..match_count {
                probe_indexes[matched_num] = *idx;
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
                    let result_block = self.merge_eq_block(probe_block, build_block, matched_num);

                    let filter = self.get_nullable_filter_column(
                        &result_block,
                        other_predicate,
                        &self.func_ctx,
                    )?;
                    let filter_viewer =
                        NullableType::<BooleanType>::try_downcast_column(&filter).unwrap();
                    let validity = &filter_viewer.validity;
                    let data = &filter_viewer.column;

                    for (idx, index) in probe_indexes.iter().enumerate() {
                        let marker = &mut markers[*index as usize];
                        if !validity.get_bit(idx) {
                            if *marker == MARKER_KIND_FALSE {
                                *marker = MARKER_KIND_NULL;
                            }
                        } else if data.get_bit(idx) {
                            *marker = MARKER_KIND_TRUE;
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

                    for _ in 0..match_count {
                        probe_indexes[matched_num] = *idx;
                        matched_num += 1;
                    }

                    if matched_num < max_block_size {
                        break;
                    }
                }
            }
        }

        if matched_num > 0 {
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
            let result_block = self.merge_eq_block(probe_block, build_block, matched_num);

            let filter =
                self.get_nullable_filter_column(&result_block, other_predicate, &self.func_ctx)?;
            let filter_viewer = NullableType::<BooleanType>::try_downcast_column(&filter).unwrap();
            let validity = &filter_viewer.validity;
            let data = &filter_viewer.column;

            for (idx, index) in probe_indexes.iter().enumerate().take(matched_num) {
                let marker = &mut markers[*index as usize];
                if !validity.get_bit(idx) {
                    if *marker == MARKER_KIND_FALSE {
                        *marker = MARKER_KIND_NULL;
                    }
                } else if data.get_bit(idx) {
                    *marker = MARKER_KIND_TRUE;
                }
            }
        }

        let probe_block = if is_probe_projected {
            Some(input.clone())
        } else {
            None
        };
        let marker_block = Some(self.create_marker_block(has_null, markers, input.num_rows())?);

        Ok(vec![self.merge_eq_block(
            probe_block,
            marker_block,
            input.num_rows(),
        )])
    }
}
