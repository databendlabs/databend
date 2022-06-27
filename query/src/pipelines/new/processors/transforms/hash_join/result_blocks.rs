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

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_datablocks::DataBlock;
use common_datavalues::BooleanColumn;
use common_datavalues::Column;
use common_datavalues::NullableColumn;
use common_datavalues::Series;
use common_exception::Result;

use super::ChainingHashTable;
use crate::common::EvalNode;
use crate::common::HashMap;
use crate::common::HashTableKeyable;
use crate::pipelines::new::processors::transforms::hash_join::row::RowPtr;
use crate::sql::exec::ColumnID;
use crate::sql::planner::plans::JoinType;

impl ChainingHashTable {
    pub(crate) fn result_blocks<Key>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        keys: Vec<Key>,
        input: &DataBlock,
    ) -> Result<Vec<DataBlock>>
    where
        Key: HashTableKeyable + Clone + 'static,
    {
        let mut results: Vec<DataBlock> = vec![];
        match self.join_type {
            JoinType::Inner => {
                let mut probe_indexs = Vec::with_capacity(keys.len());
                let mut build_indexs = Vec::with_capacity(keys.len());

                for (i, key) in keys.iter().enumerate() {
                    let probe_result_ptr = hash_table.find_key(key);
                    match probe_result_ptr {
                        Some(v) => {
                            let probe_result_ptrs = v.get_value();
                            build_indexs.extend_from_slice(probe_result_ptrs);

                            for _ in probe_result_ptrs {
                                probe_indexs.push(i as u32);
                            }
                        }
                        None => continue,
                    }
                }

                let build_block = self.row_space.gather(&build_indexs)?;
                let probe_block = DataBlock::block_take_by_indices(input, &probe_indexs)?;

                results.push(self.merge_eq_block(&build_block, &probe_block)?);
            }
            JoinType::Semi => {
                let mut probe_indexs = Vec::with_capacity(keys.len());

                for (i, key) in keys.iter().enumerate() {
                    let probe_result_ptr = hash_table.find_key(key);

                    match (probe_result_ptr, &self.other_predicate) {
                        (Some(ptrs), Some(pred)) => {
                            let build_block = self.row_space.gather(ptrs.get_value())?;
                            let probe_block = DataBlock::block_take_by_indices(input, &[i as u32])?;
                            let merged = self.merge_block(&build_block, &probe_block)?;
                            let func_ctx = self.ctx.try_get_function_context()?;
                            let filter_vector = pred.eval(&func_ctx, &merged)?;

                            let has_row = DataBlock::filter_exists(filter_vector.vector())?;
                            if has_row {
                                probe_indexs.push(i as u32);
                            }
                        }
                        (Some(_), None) => {
                            // No other conditions and has matched result
                            probe_indexs.push(i as u32);
                        }
                        (None, _) => {
                            // No matched row for current probe row
                        }
                    }
                }
                let probe_block = DataBlock::block_take_by_indices(input, &probe_indexs)?;
                results.push(probe_block);
            }
            JoinType::Anti => {
                let mut probe_indexs = Vec::with_capacity(keys.len());

                for (i, key) in keys.iter().enumerate() {
                    let probe_result_ptr = hash_table.find_key(key);

                    match (probe_result_ptr, &self.other_predicate) {
                        (Some(ptrs), Some(pred)) => {
                            let build_block = self.row_space.gather(ptrs.get_value())?;
                            let probe_block = DataBlock::block_take_by_indices(input, &[i as u32])?;
                            let merged = self.merge_block(&build_block, &probe_block)?;
                            let func_ctx = self.ctx.try_get_function_context()?;
                            let filter_vector = pred.eval(&func_ctx, &merged)?;
                            let has_row = DataBlock::filter_exists(filter_vector.vector())?;

                            if !has_row {
                                probe_indexs.push(i as u32);
                            }
                        }
                        (Some(_), None) => {
                            // No other conditions and has matched result
                        }
                        (None, _) => {
                            // No matched row for current probe row
                            probe_indexs.push(i as u32);
                        }
                    }
                }

                let probe_block = DataBlock::block_take_by_indices(input, &probe_indexs)?;
                results.push(probe_block);
            }

            // probe_blocks left join build blocks
            JoinType::Left => {
                if self.other_predicate.is_none() {
                    let result = self.left_join(hash_table, keys, input)?;
                    return Ok(vec![result]);
                } else {
                    let result = self.left_join_with_other_conjunct(hash_table, keys, input)?;
                    return Ok(vec![result]);
                }
            }
            _ => unreachable!(),
        }
        Ok(results)
    }

    fn left_join<Key>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        keys: Vec<Key>,
        input: &DataBlock,
    ) -> Result<DataBlock>
    where
        Key: HashTableKeyable + Clone + 'static,
    {
        let mut probe_indexs = Vec::with_capacity(keys.len());
        let mut build_indexs = Vec::with_capacity(keys.len());

        let mut validity = MutableBitmap::new();
        for (i, key) in keys.iter().enumerate() {
            let probe_result_ptr = hash_table.find_key(key);
            match probe_result_ptr {
                Some(v) => {
                    let probe_result_ptrs = v.get_value();
                    build_indexs.extend_from_slice(probe_result_ptrs);
                    for _ in probe_result_ptrs {
                        probe_indexs.push(i as u32);
                    }
                    validity.extend_constant(probe_result_ptrs.len(), true);
                }
                None => {
                    //dummy row ptr
                    build_indexs.push(RowPtr {
                        chunk_index: 0,
                        row_index: 0,
                    });
                    probe_indexs.push(i as u32);
                    validity.push(false);
                }
            }
        }
        let validity: Bitmap = validity.into();
        let build_block = self.row_space.gather(&build_indexs)?;
        let mut nullable_columns = Vec::with_capacity(build_block.num_columns());

        for column in build_block.columns() {
            if column.is_null() {
                nullable_columns.push(column.clone());
            } else if column.is_nullable() {
                let col: &NullableColumn = Series::check_get(column)?;
                let new_validity = col.ensure_validity() & (&validity);
                nullable_columns.push(NullableColumn::wrap_inner(
                    col.inner().clone(),
                    Some(new_validity),
                ));
            } else {
                nullable_columns.push(NullableColumn::wrap_inner(
                    column.clone(),
                    Some(validity.clone()),
                ));
            }
        }
        let nullable_build_block =
            DataBlock::create(self.row_space.data_schema.clone(), nullable_columns);
        let probe_block = DataBlock::block_take_by_indices(input, &probe_indexs)?;
        self.merge_eq_block(&nullable_build_block, &probe_block)
    }

    fn left_join_with_other_conjunct<Key>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        keys: Vec<Key>,
        input: &DataBlock,
    ) -> Result<DataBlock>
    where
        Key: HashTableKeyable + Clone + 'static,
    {
        let mut probe_indexs = Vec::with_capacity(keys.len());
        let mut build_indexs = Vec::with_capacity(keys.len());

        let mut index_to_row = Vec::with_capacity(keys.len());
        let mut row_state = vec![0u32; keys.len()];

        let mut validity = MutableBitmap::new();
        for (i, key) in keys.iter().enumerate() {
            let probe_result_ptr = hash_table.find_key(key);

            match probe_result_ptr {
                Some(v) => {
                    let probe_result_ptrs = v.get_value();
                    build_indexs.extend_from_slice(probe_result_ptrs);
                    for _ in probe_result_ptrs {
                        probe_indexs.push(i as u32);
                        index_to_row.push(i);

                        row_state[i] += 1;
                    }
                    validity.extend_constant(probe_result_ptrs.len(), true);
                }
                None => {
                    //dummy row ptr
                    build_indexs.push(RowPtr {
                        chunk_index: 0,
                        row_index: 0,
                    });
                    probe_indexs.push(i as u32);
                    validity.push(false);
                    index_to_row.push(i);
                    row_state[i] += 1;
                }
            }
        }
        let validity: Bitmap = validity.into();
        let build_block = self.row_space.gather(&build_indexs)?;
        let mut nullable_columns = Vec::with_capacity(build_block.num_columns());

        for column in build_block.columns() {
            if column.is_null() {
                nullable_columns.push(column.clone());
            } else if column.is_nullable() {
                let col: &NullableColumn = Series::check_get(column)?;
                let new_validity = col.ensure_validity() & (&validity);
                nullable_columns.push(NullableColumn::wrap_inner(
                    col.inner().clone(),
                    Some(new_validity),
                ));
            } else {
                nullable_columns.push(NullableColumn::wrap_inner(
                    column.clone(),
                    Some(validity.clone()),
                ));
            }
        }
        let nullable_build_block =
            DataBlock::create(self.row_space.data_schema.clone(), nullable_columns);
        let probe_block = DataBlock::block_take_by_indices(input, &probe_indexs)?;
        let merged_block = self.merge_eq_block(&nullable_build_block, &probe_block)?;

        let (bm, all_true, all_false) =
            self.get_other_filters(&merged_block, self.other_predicate.as_ref().unwrap())?;

        if all_true {
            return Ok(merged_block);
        }
        let mut bm = match (bm, all_false) {
            (Some(b), _) => b.into_mut().right().unwrap(),
            (None, true) => MutableBitmap::from_len_zeroed(merged_block.num_rows()),
            // must be one of above
            _ => unreachable!(),
        };
        Self::fill_null_for_left_join(&mut bm, &index_to_row, &mut row_state);
        let predicate = BooleanColumn::from_arrow_data(bm.into()).arc();
        DataBlock::filter_block(merged_block, &predicate)
    }

    fn fill_null_for_left_join(
        bm: &mut MutableBitmap,
        index_to_row: &[usize],
        row_state: &mut [u32],
    ) {
        for (index, row) in index_to_row.iter().enumerate() {
            if row_state[*row] == 0 {
                bm.set(index, true);
                continue;
            }

            if row_state[*row] == 1 {
                if !bm.get(index) {
                    bm.set(index, true)
                }
                continue;
            }

            if !bm.get(index) {
                row_state[*row] -= 1;
            }
        }
    }

    // return an (option bitmap, all_true, all_false)
    fn get_other_filters(
        &self,
        merged_block: &DataBlock,
        filter: &EvalNode<ColumnID>,
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
        let count_zeros = boolean_col.values().null_count();

        Ok((
            Some(boolean_col.values().clone()),
            count_zeros == 0,
            rows == count_zeros,
        ))
    }
}
