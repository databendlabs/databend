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
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::BooleanColumn;
use common_datavalues::BooleanViewer;
use common_datavalues::Column;
use common_datavalues::ScalarViewer;
use common_exception::Result;
use common_hashtable::HashMap;
use common_hashtable::HashTableKeyable;

use crate::pipelines::processors::transforms::hash_join::desc::MarkerKind;
use crate::pipelines::processors::transforms::hash_join::row::RowPtr;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::JoinHashTable;

impl JoinHashTable {
    pub(crate) fn probe_right_mark_join<Key, IT>(
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
        let has_null = {
            let has_null_ref = self.hash_join_desc.marker_join_desc.has_null.read();
            *has_null_ref
        };

        let mut markers = Self::init_markers(input.columns(), input.num_rows());
        let valids = &probe_state.valids;
        if self.hash_join_desc.other_predicate.is_none() {
            for (i, key) in keys_iter.enumerate() {
                let probe_result_ptr = if self.hash_join_desc.from_correlated_subquery {
                    hash_table.find_key(&key)
                } else {
                    self.probe_key(hash_table, key, valids, i)
                };
                if probe_result_ptr.is_some() {
                    markers[i] = MarkerKind::True;
                }
            }
        } else {
            let mut probe_indexes = vec![];
            let mut build_indexes = vec![];
            for (i, key) in keys_iter.enumerate() {
                let probe_result_ptr = if self.hash_join_desc.from_correlated_subquery {
                    hash_table.find_key(&key)
                } else {
                    self.probe_key(hash_table, key, valids, i)
                };
                if let Some(v) = probe_result_ptr {
                    let probe_result_ptrs = v.get_value();
                    build_indexes.extend_from_slice(probe_result_ptrs);
                    probe_indexes.extend(std::iter::repeat(i as u32).take(probe_result_ptrs.len()));
                }
            }

            let probe_block = DataBlock::block_take_by_indices(input, &probe_indexes)?;
            let build_block = self.row_space.gather(&build_indexes)?;
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
            for idx in 0..boolean_viewer.len() {
                let marker = &mut markers[probe_indexes[idx] as usize];
                if !boolean_viewer.valid_at(idx) {
                    if *marker == MarkerKind::False {
                        *marker = MarkerKind::Null;
                    }
                } else if boolean_viewer.value_at(idx) {
                    *marker = MarkerKind::True;
                }
            }
        }

        let marker_block = self.create_marker_block(has_null, markers)?;
        let merged_block = self.merge_eq_block(&marker_block, input)?;
        Ok(vec![merged_block])
    }
}
