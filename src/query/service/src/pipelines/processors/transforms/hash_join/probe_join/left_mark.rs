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

use std::iter::TrustedLen;

use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::BooleanViewer;
use common_datavalues::ScalarViewer;
use common_exception::Result;
use common_hashtable::HashMap;
use common_hashtable::HashtableKeyable;

use crate::pipelines::processors::transforms::hash_join::desc::MarkerKind;
use crate::pipelines::processors::transforms::hash_join::row::RowPtr;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::JoinHashTable;

impl JoinHashTable {
    pub(crate) fn probe_left_mark_join<Key, IT>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &DataBlock,
    ) -> Result<Vec<DataBlock>>
    where
        Key: HashtableKeyable + Clone + 'static,
        IT: Iterator<Item = Key> + TrustedLen,
    {
        // `probe_column` is the subquery result column.
        // For sql: select * from t1 where t1.a in (select t2.a from t2); t2.a is the `probe_column`,
        let probe_column = input.column(0);
        // Check if there is any null in the probe column.
        if let Some(validity) = probe_column.validity().1 {
            if validity.unset_bits() > 0 {
                let mut has_null = self.hash_join_desc.marker_join_desc.has_null.write();
                *has_null = true;
            }
        }
        let probe_indexs = &mut probe_state.probe_indexs;
        let build_indexs = &mut probe_state.build_indexs;
        let valids = &probe_state.valids;
        for (i, key) in keys_iter.enumerate() {
            let probe_result_ptr = if self.hash_join_desc.from_correlated_subquery {
                hash_table.entry(&key)
            } else {
                self.probe_key(hash_table, key, valids, i)
            };
            if let Some(v) = probe_result_ptr {
                let probe_result_ptrs = v.get();
                build_indexs.extend_from_slice(probe_result_ptrs);
                probe_indexs.extend(std::iter::repeat(i as u32).take(probe_result_ptrs.len()));
                for ptr in probe_result_ptrs {
                    // If has other conditions, we'll process marker later
                    if self.hash_join_desc.other_predicate.is_none() {
                        // If find join partner, set the marker to true.
                        let mut self_row_ptrs = self.row_ptrs.write();
                        if let Some(p) = self_row_ptrs.iter_mut().find(|p| (*p).eq(&ptr)) {
                            p.marker = Some(MarkerKind::True);
                        }
                    }
                }
            }
        }
        if self.hash_join_desc.other_predicate.is_none() {
            return Ok(vec![DataBlock::empty()]);
        }

        if self.hash_join_desc.from_correlated_subquery {
            // Must be correlated ANY subquery, we won't need to check `has_null` in `mark_join_blocks`.
            // In the following, if value is Null and Marker is False, we'll set the marker to Null
            let mut has_null = self.hash_join_desc.marker_join_desc.has_null.write();
            *has_null = false;
        }
        let probe_block = DataBlock::block_take_by_indices(input, probe_indexs)?;
        let build_block = self.row_space.gather(build_indexs)?;
        let merged_block = self.merge_eq_block(&build_block, &probe_block)?;
        let func_ctx = self.ctx.try_get_function_context()?;
        let type_vector = self
            .hash_join_desc
            .other_predicate
            .as_ref()
            .unwrap()
            .eval(&func_ctx, &merged_block)?;
        let filter_column = type_vector.vector();
        let boolean_viewer = BooleanViewer::try_create(filter_column)?;
        let mut row_ptrs = self.row_ptrs.write();
        for (idx, build_index) in build_indexs.iter().enumerate() {
            let self_row_ptr = row_ptrs.iter_mut().find(|p| (*p).eq(&build_index)).unwrap();
            if !boolean_viewer.valid_at(idx) {
                if self_row_ptr.marker == Some(MarkerKind::False) {
                    self_row_ptr.marker = Some(MarkerKind::Null);
                }
            } else if boolean_viewer.value_at(idx) {
                self_row_ptr.marker = Some(MarkerKind::True);
            }
        }
        Ok(vec![DataBlock::empty()])
    }
}
