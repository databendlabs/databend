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
use common_datavalues::BooleanType;
use common_datavalues::Column;
use common_datavalues::ColumnRef;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::NullableColumn;
use common_datavalues::NullableType;
use common_datavalues::Series;
use common_exception::ErrorCode;
use common_exception::Result;
use common_hashtable::HashMap;
use common_hashtable::HashTableKeyable;

use super::JoinHashTable;
use super::ProbeState;
use crate::common::EvalNode;
use crate::pipelines::new::processors::transforms::hash_join::join_hash_table::MarkerKind;
use crate::pipelines::new::processors::transforms::hash_join::row::RowPtr;
use crate::sql::exec::ColumnID;
use crate::sql::planner::plans::JoinType;
use crate::sql::plans::JoinType::Mark;

impl JoinHashTable {
    pub(crate) fn result_blocks<Key>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        probe_state: &mut ProbeState,
        keys: Vec<Key>,
        input: &DataBlock,
    ) -> Result<Vec<DataBlock>>
    where
        Key: HashTableKeyable + Clone + 'static,
    {
        let probe_indexs = &mut probe_state.probe_indexs;
        let build_indexs = &mut probe_state.build_indexs;

        let mut results: Vec<DataBlock> = vec![];
        match self.hash_join_desc.join_type {
            JoinType::Inner => {
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

                let build_block = self.row_space.gather(build_indexs)?;
                let probe_block = DataBlock::block_take_by_indices(input, probe_indexs)?;
                let merged_block = self.merge_eq_block(&build_block, &probe_block)?;

                match &self.hash_join_desc.other_predicate {
                    Some(other_predicate) => {
                        let func_ctx = self.ctx.try_get_function_context()?;
                        let filter_vector = other_predicate.eval(&func_ctx, &merged_block)?;
                        results.push(DataBlock::filter_block(
                            merged_block,
                            filter_vector.vector(),
                        )?);
                    }
                    None => results.push(merged_block),
                }
            }
            JoinType::Semi => {
                if self.hash_join_desc.other_predicate.is_none() {
                    let result =
                        self.semi_anti_join::<true, _>(hash_table, probe_state, keys, input)?;
                    return Ok(vec![result]);
                } else {
                    let result = self.semi_anti_join_with_other_conjunct::<true, _>(
                        hash_table,
                        probe_state,
                        keys,
                        input,
                    )?;
                    return Ok(vec![result]);
                }
            }
            JoinType::Anti => {
                if self.hash_join_desc.other_predicate.is_none() {
                    let result =
                        self.semi_anti_join::<false, _>(hash_table, probe_state, keys, input)?;
                    return Ok(vec![result]);
                } else {
                    let result = self.semi_anti_join_with_other_conjunct::<false, _>(
                        hash_table,
                        probe_state,
                        keys,
                        input,
                    )?;
                    return Ok(vec![result]);
                }
            }

            // probe_blocks left join build blocks
            JoinType::Left => {
                if self.hash_join_desc.other_predicate.is_none() {
                    let result =
                        self.left_join::<false, _>(hash_table, probe_state, keys, input)?;
                    return Ok(vec![result]);
                } else {
                    let result = self.left_join::<true, _>(hash_table, probe_state, keys, input)?;
                    return Ok(vec![result]);
                }
            }
            Mark => {
                let mut has_null = false;
                // `probe_column` is the subquery result column.
                // For sql: select * from t1 where t1.a in (select t2.a from t2); t2.a is the `probe_column`,
                let probe_column = input.column(0);
                // Check if there is any null in the probe column.
                if let Some(validity) = probe_column.validity().1 {
                    if validity.unset_bits() > 0 {
                        has_null = true;
                    }
                }
                for key in keys.iter() {
                    let probe_result_ptr = hash_table.find_key(key);
                    if let Some(v) = probe_result_ptr {
                        let probe_result_ptrs = v.get_value();
                        let mut marker = self.hash_join_desc.marker.write();
                        for ptr in probe_result_ptrs {
                            // If find join partner, set the marker to true.
                            marker[ptr.row_index as usize] = MarkerKind::True;
                        }
                    }
                }
                let mut marker = self.hash_join_desc.marker.write();
                let mut validity = MutableBitmap::new();
                let mut boolean_bit_map = MutableBitmap::new();
                for m in marker.iter_mut() {
                    if m == &mut MarkerKind::False && has_null {
                        *m = MarkerKind::Null;
                    }
                    if m == &mut MarkerKind::Null {
                        validity.push(false);
                    } else {
                        validity.push(true);
                    }
                    if m == &mut MarkerKind::True {
                        boolean_bit_map.push(true);
                    } else {
                        boolean_bit_map.push(false);
                    }
                }
                // transfer marker to a Nullable(BooleanColumn)
                let boolean_column = BooleanColumn::from_arrow_data(boolean_bit_map.into());
                let marker_column = Self::set_validity(&boolean_column.arc(), &validity.into())?;
                let marker_schema = DataSchema::new(vec![DataField::new(
                    &self
                        .hash_join_desc
                        .marker_index
                        .ok_or_else(|| ErrorCode::LogicalError("Invalid mark join"))?
                        .to_string(),
                    NullableType::new_impl(BooleanType::new_impl()),
                )]);
                let marker_block =
                    DataBlock::create(DataSchemaRef::from(marker_schema), vec![marker_column]);
                let build_indexs = &mut probe_state.build_indexs;
                for entity in hash_table.iter() {
                    build_indexs.extend_from_slice(entity.get_value());
                }
                let build_block = self.row_space.gather(build_indexs)?;
                let result = self.merge_eq_block(&marker_block, &build_block)?;
                results.push(result);
            }
            _ => unreachable!(),
        }
        Ok(results)
    }

    fn semi_anti_join<const SEMI: bool, Key>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        probe_state: &mut ProbeState,
        keys: Vec<Key>,
        input: &DataBlock,
    ) -> Result<DataBlock>
    where
        Key: HashTableKeyable + Clone + 'static,
    {
        let probe_indexs = &mut probe_state.probe_indexs;

        for (i, key) in keys.iter().enumerate() {
            let probe_result_ptr = hash_table.find_key(key);

            match (probe_result_ptr, SEMI) {
                (Some(_), true) | (None, false) => {
                    probe_indexs.push(i as u32);
                }
                _ => {}
            }
        }
        DataBlock::block_take_by_indices(input, probe_indexs)
    }

    fn semi_anti_join_with_other_conjunct<const SEMI: bool, Key>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        probe_state: &mut ProbeState,
        keys: Vec<Key>,
        input: &DataBlock,
    ) -> Result<DataBlock>
    where
        Key: HashTableKeyable + Clone + 'static,
    {
        let probe_indexs = &mut probe_state.probe_indexs;
        let build_indexs = &mut probe_state.build_indexs;
        let row_state = &mut probe_state.row_state;

        // For semi join, it defaults to all
        row_state.resize(keys.len(), 0);

        let mut dummys = 0;

        for (i, key) in keys.iter().enumerate() {
            let probe_result_ptr = hash_table.find_key(key);

            match (probe_result_ptr, SEMI) {
                (Some(v), _) => {
                    let probe_result_ptrs = v.get_value();
                    build_indexs.extend_from_slice(probe_result_ptrs);
                    probe_indexs.extend(std::iter::repeat(i as u32).take(probe_result_ptrs.len()));

                    if !SEMI {
                        row_state[i] += probe_result_ptrs.len() as u32;
                    }
                }

                (None, false) => {
                    //dummy row ptr
                    build_indexs.push(RowPtr {
                        chunk_index: 0,
                        row_index: 0,
                    });
                    probe_indexs.push(i as u32);

                    dummys += 1;
                    // must not be filtered out， so we should not increase the row_state for anti join
                    // row_state[i] += 1;
                }
                _ => {}
            }
        }
        let probe_block = DataBlock::block_take_by_indices(input, probe_indexs)?;
        // faster path for anti join
        if dummys == probe_indexs.len() {
            return Ok(probe_block);
        }

        let build_block = self.row_space.gather(build_indexs)?;
        let merged_block = self.merge_eq_block(&build_block, &probe_block)?;

        let (bm, all_true, all_false) = self.get_other_filters(
            &merged_block,
            self.hash_join_desc.other_predicate.as_ref().unwrap(),
        )?;

        let mut bm = match (bm, all_true, all_false) {
            (Some(b), _, _) => b.into_mut().right().unwrap(),
            (_, true, _) => MutableBitmap::from_len_set(merged_block.num_rows()),
            (_, _, true) => MutableBitmap::from_len_zeroed(merged_block.num_rows()),
            // must be one of above
            _ => unreachable!(),
        };

        if SEMI {
            Self::fill_null_for_semi_join(&mut bm, probe_indexs, row_state);
        } else {
            Self::fill_null_for_anti_join(&mut bm, probe_indexs, row_state);
        }

        let predicate = BooleanColumn::from_arrow_data(bm.into()).arc();
        DataBlock::filter_block(probe_block, &predicate)
    }

    fn left_join<const WITH_OTHER_CONJUNCT: bool, Key>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        probe_state: &mut ProbeState,
        keys: Vec<Key>,
        input: &DataBlock,
    ) -> Result<DataBlock>
    where
        Key: HashTableKeyable + Clone + 'static,
    {
        let probe_indexs = &mut probe_state.probe_indexs;
        let build_indexs = &mut probe_state.build_indexs;

        let row_state = &mut probe_state.row_state;

        if WITH_OTHER_CONJUNCT {
            row_state.resize(keys.len(), 0);
        }

        let mut validity = MutableBitmap::new();
        for (i, key) in keys.iter().enumerate() {
            let probe_result_ptr = hash_table.find_key(key);

            match probe_result_ptr {
                Some(v) => {
                    let probe_result_ptrs = v.get_value();
                    build_indexs.extend_from_slice(probe_result_ptrs);
                    probe_indexs.extend(std::iter::repeat(i as u32).take(probe_result_ptrs.len()));

                    if WITH_OTHER_CONJUNCT {
                        row_state[i] += probe_result_ptrs.len() as u32;
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

                    if WITH_OTHER_CONJUNCT {
                        row_state[i] += 1;
                    }
                }
            }
        }

        let validity: Bitmap = validity.into();
        let build_block = self.row_space.gather(build_indexs)?;

        let nullable_columns = build_block
            .columns()
            .iter()
            .map(|c| Self::set_validity(c, &validity))
            .collect::<Result<Vec<_>>>()?;

        let nullable_build_block =
            DataBlock::create(self.row_space.data_schema.clone(), nullable_columns.clone());
        let probe_block = DataBlock::block_take_by_indices(input, probe_indexs)?;
        let merged_block = self.merge_eq_block(&nullable_build_block, &probe_block)?;

        if !WITH_OTHER_CONJUNCT {
            return Ok(merged_block);
        }

        let (bm, all_true, all_false) = self.get_other_filters(
            &merged_block,
            self.hash_join_desc.other_predicate.as_ref().unwrap(),
        )?;

        if all_true {
            return Ok(merged_block);
        }

        let validity = match (bm, all_false) {
            (Some(b), _) => b,
            (None, true) => Bitmap::new_zeroed(merged_block.num_rows()),
            // must be one of above
            _ => unreachable!(),
        };

        let nullable_columns = nullable_columns
            .iter()
            .map(|c| Self::set_validity(c, &validity))
            .collect::<Result<Vec<_>>>()?;
        let nullable_build_block =
            DataBlock::create(self.row_space.data_schema.clone(), nullable_columns.clone());
        let merged_block = self.merge_eq_block(&nullable_build_block, &probe_block)?;

        let mut bm = validity.into_mut().right().unwrap();

        Self::fill_null_for_left_join(&mut bm, probe_indexs, row_state);
        let predicate = BooleanColumn::from_arrow_data(bm.into()).arc();
        DataBlock::filter_block(merged_block, &predicate)
    }

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
    // bitmap: [1, 0, 1] with row_state [2, 0], probe_index: [0, 0, 1]
    // bitmap will be [1, 0, 1] -> [1, 0, 1] -> [1, 0, 1] -> [1, 0, 1]
    // row_state will be [2, 0] -> [2, 0] -> [1, 0] -> [1, 0]
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
        let count_zeros = boolean_col.values().unset_bits();

        Ok((
            Some(boolean_col.values().clone()),
            count_zeros == 0,
            rows == count_zeros,
        ))
    }

    fn set_validity(column: &ColumnRef, validity: &Bitmap) -> Result<ColumnRef> {
        if column.is_null() {
            Ok(column.clone())
        } else if column.is_nullable() {
            let col: &NullableColumn = Series::check_get(column)?;
            let new_validity = col.ensure_validity() & validity;
            let col = NullableColumn::wrap_inner(col.inner().clone(), Some(new_validity));
            Ok(col)
        } else {
            let col = NullableColumn::wrap_inner(column.clone(), Some(validity.clone()));
            Ok(col)
        }
    }
}
