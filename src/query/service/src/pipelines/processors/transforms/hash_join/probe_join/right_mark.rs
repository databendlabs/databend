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

use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::BooleanViewer;
use common_datavalues::ScalarViewer;
use common_exception::ErrorCode;
use common_exception::Result;
use common_hashtable::HashtableEntryRefLike;
use common_hashtable::HashtableLike;

use crate::pipelines::processors::transforms::hash_join::desc::MarkerKind;
use crate::pipelines::processors::transforms::hash_join::desc::JOIN_MAX_BLOCK_SIZE;
use crate::pipelines::processors::transforms::hash_join::row::RowPtr;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::JoinHashTable;

impl JoinHashTable {
    pub(crate) fn probe_right_mark_join<'a, H: HashtableLike<Value = Vec<RowPtr>>, IT>(
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
        let has_null = *self.hash_join_desc.marker_join_desc.has_null.read();
        let markers = probe_state.markers.as_mut().unwrap();
        for (i, key) in keys_iter.enumerate() {
            let probe_result_ptr = match self.hash_join_desc.from_correlated_subquery {
                true => hash_table.entry(key),
                false => self.probe_key(hash_table, key, valids, i),
            };

            if probe_result_ptr.is_some() {
                markers[i] = MarkerKind::True;
            }
        }

        Ok(vec![self.merge_eq_block(
            &self.create_marker_block(has_null, markers.clone())?,
            input,
        )?])
    }

    pub(crate) fn probe_right_mark_join_with_conjunct<
        'a,
        H: HashtableLike<Value = Vec<RowPtr>>,
        IT,
    >(
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
        let has_null = *self.hash_join_desc.marker_join_desc.has_null.read();
        let mut markers = Self::init_markers(input.columns(), input.num_rows());

        let func_ctx = self.ctx.try_get_function_context()?;
        let other_predicate = self.hash_join_desc.other_predicate.as_ref().unwrap();

        let mut probe_indexes = Vec::with_capacity(JOIN_MAX_BLOCK_SIZE);
        let mut build_indexes = Vec::with_capacity(JOIN_MAX_BLOCK_SIZE);

        for (i, key) in keys_iter.enumerate() {
            let probe_result_ptr = match self.hash_join_desc.from_correlated_subquery {
                true => hash_table.entry(key),
                false => self.probe_key(hash_table, key, valids, i),
            };

            if let Some(v) = probe_result_ptr {
                let probed_rows = v.get();

                if probe_indexes.len() + probed_rows.len() < probe_indexes.capacity() {
                    build_indexes.extend_from_slice(probed_rows);
                    probe_indexes.extend(repeat(i as u32).take(probed_rows.len()));
                } else {
                    let mut index = 0_usize;
                    let mut remain = probed_rows.len();

                    while index < probed_rows.len() {
                        if probe_indexes.len() + remain < probe_indexes.capacity() {
                            build_indexes.extend_from_slice(&probed_rows[index..]);
                            probe_indexes.extend(std::iter::repeat(i as u32).take(remain));
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

                            let probe_block =
                                DataBlock::block_take_by_indices(input, &probe_indexes)?;
                            let build_block = self.row_space.gather(&build_indexes)?;
                            let merged_block = self.merge_eq_block(&build_block, &probe_block)?;

                            let type_vector = other_predicate.eval(&func_ctx, &merged_block)?;
                            let filter_column = type_vector.vector();
                            let filter_viewer = BooleanViewer::try_create(filter_column)?;

                            for idx in 0..filter_viewer.len() {
                                let marker = &mut markers[probe_indexes[idx] as usize];
                                if !filter_viewer.valid_at(idx) {
                                    if *marker == MarkerKind::False {
                                        *marker = MarkerKind::Null;
                                    }
                                } else if filter_viewer.value_at(idx) {
                                    *marker = MarkerKind::True;
                                }
                            }

                            index = new_index;
                            remain -= addition;

                            build_indexes.clear();
                            probe_indexes.clear();
                        }
                    }
                }
            }
        }

        let probe_block = DataBlock::block_take_by_indices(input, &probe_indexes)?;
        let build_block = self.row_space.gather(&build_indexes)?;
        let merged_block = self.merge_eq_block(&build_block, &probe_block)?;
        let type_vector = other_predicate.eval(&func_ctx, &merged_block)?;
        let filter_column = type_vector.vector();
        let filter_viewer = BooleanViewer::try_create(filter_column)?;

        for idx in 0..filter_viewer.len() {
            let marker = &mut markers[probe_indexes[idx] as usize];
            if !filter_viewer.valid_at(idx) {
                if *marker == MarkerKind::False {
                    *marker = MarkerKind::Null;
                }
            } else if filter_viewer.value_at(idx) {
                *marker = MarkerKind::True;
            }
        }

        Ok(vec![self.merge_eq_block(
            &self.create_marker_block(has_null, markers)?,
            input,
        )?])
    }
}
