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
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::BooleanType;
use common_expression::types::NullableType;
use common_expression::types::ValueType;
use common_expression::Chunk;
use common_expression::Column;
use common_expression::Evaluator;
use common_functions_v2::scalars::BUILTIN_FUNCTIONS;
use common_hashtable::HashtableEntryRefLike;
use common_hashtable::HashtableLike;

use crate::pipelines::processors::transforms::hash_join::desc::MarkerKind;
use crate::pipelines::processors::transforms::hash_join::desc::JOIN_MAX_CHUNK_SIZE;
use crate::pipelines::processors::transforms::hash_join::row::RowPtr;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::JoinHashTable;

impl JoinHashTable {
    pub(crate) fn probe_left_mark_join<'a, H: HashtableLike<Value = Vec<RowPtr>>, IT>(
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
        let mut chunk_size = JOIN_MAX_CHUNK_SIZE;
        // `probe_column` is the subquery result column.
        // For sql: select * from t1 where t1.a in (select t2.a from t2); t2.a is the `probe_column`,
        let probe_column = input.column(0);
        // Check if there is any null in the probe column.
        if matches!(probe_column.validity().1, Some(x) if x.unset_bits() > 0) {
            let mut has_null = self.hash_join_desc.marker_join_desc.has_null.write();
            *has_null = true;
        }

        // If find join partner, set the marker to true.
        let mut self_row_ptrs = self.row_ptrs.write();

        for (i, key) in keys_iter.enumerate() {
            if (i & chunk_size) == 0 {
                chunk_size <<= 1;

                if self.interrupt.load(Ordering::Relaxed) {
                    return Err(ErrorCode::AbortedQuery(
                        "Aborted query, because the server is shutting down or the query was killed.",
                    ));
                }
            }

            let probe_result_ptr = match self.hash_join_desc.from_correlated_subquery {
                true => hash_table.entry(key),
                false => self.probe_key(hash_table, key, valids, i),
            };

            if let Some(v) = probe_result_ptr {
                let probed_rows = v.get();

                for probed_row in probed_rows {
                    if let Some(p) = self_row_ptrs.iter_mut().find(|p| (*p).eq(&probed_row)) {
                        p.marker = Some(MarkerKind::True);
                    }
                }
            }
        }

        Ok(vec![Chunk::empty()])
    }

    pub(crate) fn probe_left_mark_join_with_conjunct<
        'a,
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

        // `probe_column` is the subquery result column.
        // For sql: select * from t1 where t1.a in (select t2.a from t2); t2.a is the `probe_column`,
        let probe_column = input.column(0);
        // Check if there is any null in the probe column.
        if matches!(probe_column.validity().1, Some(x) if x.unset_bits() > 0) {
            let mut has_null = self.hash_join_desc.marker_join_desc.has_null.write();
            *has_null = true;
        }

        let func_ctx = self.ctx.try_get_function_context()?;
        let other_predicate = self.hash_join_desc.other_predicate.as_ref().unwrap();

        let mut row_ptrs = self.row_ptrs.write();
        let mut probe_indexes = Vec::with_capacity(JOIN_MAX_CHUNK_SIZE);
        let mut build_indexes = Vec::with_capacity(JOIN_MAX_CHUNK_SIZE);

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

                            let probe_chunk = Chunk::take(input.clone(), &probe_indexes)?;
                            let build_chunk = self.row_space.gather(&build_indexes)?;
                            let merged_chunk = self.merge_eq_chunk(&build_chunk, &probe_chunk)?;

                            let filter =
                                self.get_nullable_filter_column(&merged_chunk, other_predicate)?;
                            let filter_viewer =
                                NullableType::<BooleanType>::try_downcast_column(&filter).unwrap();
                            let validity = &filter_viewer.validity;
                            let data = &filter_viewer.column;

                            for (idx, build_index) in build_indexes.iter().enumerate() {
                                let self_row_ptr =
                                    row_ptrs.iter_mut().find(|p| (*p).eq(&build_index)).unwrap();
                                if !validity.get_bit(idx) {
                                    if self_row_ptr.marker == Some(MarkerKind::False) {
                                        self_row_ptr.marker = Some(MarkerKind::Null);
                                    }
                                } else if data.get_bit(idx) {
                                    self_row_ptr.marker = Some(MarkerKind::True);
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

        let probe_chunk = Chunk::take(input.clone(), &probe_indexes)?;
        let build_chunk = self.row_space.gather(&build_indexes)?;
        let merged_chunk = self.merge_eq_chunk(&build_chunk, &probe_chunk)?;

        let filter = self.get_nullable_filter_column(&merged_chunk, other_predicate)?;
        let filter_viewer = NullableType::<BooleanType>::try_downcast_column(&filter).unwrap();
        let validity = &filter_viewer.validity;
        let data = &filter_viewer.column;

        for (idx, build_index) in build_indexes.iter().enumerate() {
            let self_row_ptr = row_ptrs.iter_mut().find(|p| (*p).eq(&build_index)).unwrap();
            if !validity.get_bit(idx) {
                if self_row_ptr.marker == Some(MarkerKind::False) {
                    self_row_ptr.marker = Some(MarkerKind::Null);
                }
            } else if data.get_bit(idx) {
                self_row_ptr.marker = Some(MarkerKind::True);
            }
        }

        if self.hash_join_desc.from_correlated_subquery {
            // Must be correlated ANY subquery, we won't need to check `has_null` in `mark_join_chunks`.
            // In the following, if value is Null and Marker is False, we'll set the marker to Null
            let mut has_null = self.hash_join_desc.marker_join_desc.has_null.write();
            *has_null = false;
        }

        Ok(vec![Chunk::empty()])
    }
}
