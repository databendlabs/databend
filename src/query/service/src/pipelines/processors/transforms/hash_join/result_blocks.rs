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

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_datablocks::DataBlock;
use common_datavalues::combine_validities_3;
use common_datavalues::wrap_nullable;
use common_datavalues::BooleanColumn;
use common_datavalues::BooleanType;
use common_datavalues::BooleanViewer;
use common_datavalues::Column;
use common_datavalues::ColumnRef;
use common_datavalues::ConstColumn;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::NullableColumn;
use common_datavalues::NullableType;
use common_datavalues::ScalarViewer;
use common_datavalues::Series;
use common_exception::ErrorCode;
use common_exception::Result;
use common_hashtable::HashMap;
use common_hashtable::HashtableEntry;
use common_hashtable::HashtableKeyable;
use common_hashtable::UnsizedHashMap;
use common_hashtable::UnsizedHashtableEntryRef;

use super::JoinHashTable;
use super::ProbeState;
use crate::evaluator::EvalNode;
use crate::pipelines::processors::transforms::hash_join::join_hash_table::MarkerKind;
use crate::pipelines::processors::transforms::hash_join::row::RowPtr;
use crate::sessions::TableContext;
use crate::sql::planner::plans::JoinType;

pub trait ResultBlocks {
    type Key: ?Sized;
    type KeyRef<'a>;

    fn result_blocks<'a, IT>(
        &self,
        join_hash_table: &JoinHashTable,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &DataBlock,
    ) -> Result<Vec<DataBlock>>
    where
        IT: Iterator<Item = Self::KeyRef<'a>> + TrustedLen;
}

impl<Key: HashtableKeyable + 'static> ResultBlocks for HashMap<Key, Vec<RowPtr>> {
    type Key = Key;
    type KeyRef<'a> = Key;

    fn result_blocks<'a, IT>(
        &self,
        join_hash_table: &JoinHashTable,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &DataBlock,
    ) -> Result<Vec<DataBlock>>
    where
        IT: Iterator<Item = Self::KeyRef<'a>> + TrustedLen,
    {
        todo!()
    }
}

impl ResultBlocks for UnsizedHashMap<[u8], Vec<RowPtr>> {
    type Key = [u8];
    type KeyRef<'a> = &'a [u8];

    fn result_blocks<'a, IT>(
        &self,
        join_hash_table: &JoinHashTable,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &DataBlock,
    ) -> Result<Vec<DataBlock>>
    where
        IT: Iterator<Item = Self::KeyRef<'a>> + TrustedLen,
    {
        todo!()
    }
}

impl JoinHashTable {
    // modify the bm by the value row_state
    // keep the index of the first positive state
    // bitmap: [1, 1, 1] with row_state [0, 0], probe_index: [0, 0, 0] (repeat the first element 3 times)
    // bitmap will be [1, 1, 1] -> [1, 1, 1] -> [1, 0, 1] -> [1, 0, 0]
    // row_state will be [0, 0] -> [1, 0] -> [1,0] -> [1, 0]
    fn fill_null_for_semi_join(
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

    // keep at least one index of the positive state and the null state
    // bitmap: [1, 0, 1] with row_state [2, 1], probe_index: [0, 0, 1]
    // bitmap will be [1, 0, 1] -> [1, 0, 1] -> [1, 0, 1] -> [1, 0, 1]
    // row_state will be [2, 1] -> [2, 1] -> [1, 1] -> [1, 1]
    fn fill_null_for_left_join(
        bm: &mut MutableBitmap,
        probe_indexs: &[u32],
        row_state: &mut [u32],
    ) {
        for (index, row) in probe_indexs.iter().enumerate() {
            let row = *row as usize;
            if row_state[row] == 0 {
                bm.set(index, true);
                continue;
            }

            if row_state[row] == 1 {
                if !bm.get(index) {
                    bm.set(index, true)
                }
                continue;
            }

            if !bm.get(index) {
                row_state[row] -= 1;
            }
        }
    }

    pub(crate) fn filter_rows_for_right_join(
        bm: &mut MutableBitmap,
        build_indexes: &[RowPtr],
        row_state: &mut std::collections::HashMap<RowPtr, usize>,
    ) {
        for (index, row) in build_indexes.iter().enumerate() {
            if row_state[row] == 1 || row_state[row] == 0 {
                if !bm.get(index) {
                    bm.set(index, true)
                }
                continue;
            }

            if !bm.get(index) {
                row_state.entry(*row).and_modify(|e| *e -= 1);
            }
        }
    }

    pub(crate) fn filter_rows_for_right_semi_join(
        &self,
        bm: &mut MutableBitmap,
        build_indexes: &[RowPtr],
        input: DataBlock,
    ) -> Result<DataBlock> {
        let mut row_state = self.hash_join_desc.right_join_desc.row_state.write();
        for (index, row) in build_indexes.iter().enumerate() {
            if row_state[row] > 1 && !bm.get(index) {
                row_state.entry(*row).and_modify(|e| *e -= 1);
            }
        }
        for (index, row) in build_indexes.iter().enumerate() {
            if row_state[row] > 1 && bm.get(index) {
                bm.set(index, false);
                row_state.entry(*row).and_modify(|e| *e -= 1);
            }
        }
        let predicate = BooleanColumn::from_arrow_data(bm.clone().into()).arc();
        DataBlock::filter_block(input, &predicate)
    }

    // return an (option bitmap, all_true, all_false)
    pub(crate) fn get_other_filters(
        &self,
        merged_block: &DataBlock,
        filter: &EvalNode,
    ) -> Result<(Option<Bitmap>, bool, bool)> {
        let func_ctx = self.ctx.try_get_function_context()?;
        // `predicate_column` contains a column, which is a boolean column.
        let filter_vector = filter.eval(&func_ctx, merged_block)?;
        let predict_boolean_nonull = DataBlock::cast_to_nonull_boolean(filter_vector.vector())?;

        // faster path for constant filter
        if predict_boolean_nonull.is_const() {
            let v = predict_boolean_nonull.get_bool(0)?;
            return Ok((None, v, !v));
        }

        let boolean_col: &BooleanColumn = Series::check_get(&predict_boolean_nonull)?;
        let rows = boolean_col.len();
        let count_zeros = boolean_col.values().unset_bits();

        Ok((
            Some(boolean_col.values().clone()),
            count_zeros == 0,
            rows == count_zeros,
        ))
    }

    pub(crate) fn set_validity(column: &ColumnRef, validity: &Bitmap) -> Result<ColumnRef> {
        if column.is_null() {
            Ok(column.clone())
        } else if column.is_const() {
            let col: &ConstColumn = Series::check_get(column)?;
            let validity = validity.clone();
            let inner = JoinHashTable::set_validity(col.inner(), &validity.slice(0, 1))?;
            Ok(ConstColumn::new(inner, col.len()).arc())
        } else if column.is_nullable() {
            let col: &NullableColumn = Series::check_get(column)?;
            // It's possible validity is longer than col.
            let diff_len = validity.len() - col.ensure_validity().len();
            let mut new_validity = MutableBitmap::with_capacity(validity.len());
            for (b1, b2) in validity.iter().zip(col.ensure_validity().iter()) {
                new_validity.push(b1 & b2);
            }
            new_validity.extend_constant(diff_len, false);
            let col = NullableColumn::wrap_inner(col.inner().clone(), Some(new_validity.into()));
            Ok(col)
        } else {
            let col = NullableColumn::wrap_inner(column.clone(), Some(validity.clone()));
            Ok(col)
        }
    }

    pub(crate) fn create_marker_block(
        &self,
        has_null: bool,
        markers: Vec<MarkerKind>,
    ) -> Result<DataBlock> {
        let mut validity = MutableBitmap::with_capacity(markers.len());
        let mut boolean_bit_map = MutableBitmap::with_capacity(markers.len());

        for m in markers {
            let marker = if m == MarkerKind::False && has_null {
                MarkerKind::Null
            } else {
                m
            };
            if marker == MarkerKind::Null {
                validity.push(false);
            } else {
                validity.push(true);
            }
            if marker == MarkerKind::True {
                boolean_bit_map.push(true);
            } else {
                boolean_bit_map.push(false);
            }
        }
        let boolean_column = BooleanColumn::from_arrow_data(boolean_bit_map.into());
        let marker_column = Self::set_validity(&boolean_column.arc(), &validity.into())?;
        let marker_schema = DataSchema::new(vec![DataField::new(
            &self
                .hash_join_desc
                .marker_join_desc
                .marker_index
                .ok_or_else(|| ErrorCode::LogicalError("Invalid mark join"))?
                .to_string(),
            NullableType::new_impl(BooleanType::new_impl()),
        )]);
        Ok(DataBlock::create(DataSchemaRef::from(marker_schema), vec![
            marker_column,
        ]))
    }

    pub(crate) fn init_markers(cols: &[ColumnRef], num_rows: usize) -> Vec<MarkerKind> {
        let mut markers = vec![MarkerKind::False; num_rows];
        if cols.iter().any(|c| c.is_nullable() || c.is_null()) {
            let mut valids = None;
            for col in cols.iter() {
                let (is_all_null, tmp_valids_option) = col.validity();
                if !is_all_null {
                    if let Some(tmp_valids) = tmp_valids_option.as_ref() {
                        if tmp_valids.unset_bits() == 0 {
                            let mut m = MutableBitmap::with_capacity(num_rows);
                            m.extend_constant(num_rows, true);
                            valids = Some(m.into());
                            break;
                        } else {
                            valids = combine_validities_3(valids, tmp_valids_option.cloned());
                        }
                    }
                }
            }
            if let Some(v) = valids {
                for (idx, marker) in markers.iter_mut().enumerate() {
                    if !v.get_bit(idx) {
                        *marker = MarkerKind::Null;
                    }
                }
            }
        }
        markers
    }
}
