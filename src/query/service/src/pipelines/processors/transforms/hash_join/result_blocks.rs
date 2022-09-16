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

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_datablocks::DataBlock;
use common_datavalues::BooleanColumn;
use common_datavalues::BooleanViewer;
use common_datavalues::Column;
use common_datavalues::ColumnRef;
use common_datavalues::ConstColumn;
use common_datavalues::DataType;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
use common_datavalues::NullableColumn;
use common_datavalues::NullableType;
use common_datavalues::ScalarViewer;
use common_datavalues::Series;
use common_exception::ErrorCode;
use common_exception::Result;
use common_hashtable::HashMap;
use common_hashtable::HashTableKeyable;
use common_hashtable::KeyValueEntity;

use super::JoinHashTable;
use super::ProbeState;
use crate::evaluator::EvalNode;
use crate::pipelines::processors::transforms::hash_join::join_hash_table::MarkerKind;
use crate::pipelines::processors::transforms::hash_join::row::RowPtr;
use crate::sessions::TableContext;
use crate::sql::planner::plans::JoinType;
use crate::sql::plans::JoinType::Mark;

impl JoinHashTable {
    pub(crate) fn result_blocks<Key, IT>(
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
        let probe_indexs = &mut probe_state.probe_indexs;
        let build_indexs = &mut probe_state.build_indexs;
        let valids = &probe_state.valids;

        let mut results: Vec<DataBlock> = vec![];
        match self.hash_join_desc.join_type {
            JoinType::Inner => {
                for (i, key) in keys_iter.enumerate() {
                    // If the join is derived from correlated subquery, then null equality is safe.
                    let probe_result_ptr = if self.hash_join_desc.from_correlated_subquery {
                        hash_table.find_key(&key)
                    } else {
                        Self::probe_key(hash_table, key, valids, i)
                    };
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
                    let result = self.semi_anti_join::<true, _, _>(
                        hash_table,
                        probe_state,
                        keys_iter,
                        input,
                    )?;
                    return Ok(vec![result]);
                } else {
                    let result = self.semi_anti_join_with_other_conjunct::<true, _, _>(
                        hash_table,
                        probe_state,
                        keys_iter,
                        input,
                    )?;
                    return Ok(vec![result]);
                }
            }
            JoinType::Anti => {
                if self.hash_join_desc.other_predicate.is_none() {
                    let result = self.semi_anti_join::<false, _, _>(
                        hash_table,
                        probe_state,
                        keys_iter,
                        input,
                    )?;
                    return Ok(vec![result]);
                } else {
                    let result = self.semi_anti_join_with_other_conjunct::<false, _, _>(
                        hash_table,
                        probe_state,
                        keys_iter,
                        input,
                    )?;
                    return Ok(vec![result]);
                }
            }

            // Single join is similar to left join, but the result is a single row.
            JoinType::Left | JoinType::Single => {
                if self.hash_join_desc.other_predicate.is_none() {
                    let result = self.left_or_single_join::<false, _, _>(
                        hash_table,
                        probe_state,
                        keys_iter,
                        input,
                    )?;
                    return Ok(vec![result]);
                } else {
                    let result = self.left_or_single_join::<true, _, _>(
                        hash_table,
                        probe_state,
                        keys_iter,
                        input,
                    )?;
                    return Ok(vec![result]);
                }
            }
            Mark => {
                results.push(DataBlock::empty());
                // Three cases will produce Mark join:
                // 1. uncorrelated ANY subquery: only have one kind of join condition, equi-condition or non-equi-condition.
                // 2. correlated ANY subquery: must have two kinds of join condition, one is equi-condition and the other is non-equi-condition.
                //    equi-condition is subquery's outer columns with subquery's derived columns.
                //    non-equi-condition is subquery's child expr with subquery's output column.
                //    for example: select * from t1 where t1.a = ANY (select t2.a from t2 where t2.b = t1.b); [t1: a, b], [t2: a, b]
                //    subquery's outer columns: t1.b, and it'll derive a new column: subquery_5 when subquery cross join t1;
                //    so equi-condition is t1.b = subquery_5, and non-equi-condition is t1.a = t2.a.
                // 3. Correlated Exists subquery： only have one kind of join condition, equi-condition.
                //    equi-condition is subquery's outer columns with subquery's derived columns. (see the above example in correlated ANY subquery)
                self.mark_join(hash_table, probe_state, keys_iter, input)?;
            }
            _ => unreachable!(),
        }
        Ok(results)
    }

    fn mark_join<Key, IT>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &DataBlock,
    ) -> Result<()>
    where
        Key: HashTableKeyable + Clone + 'static,
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
                hash_table.find_key(&key)
            } else {
                Self::probe_key(hash_table, key, valids, i)
            };
            if let Some(v) = probe_result_ptr {
                let probe_result_ptrs = v.get_value();
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
            return Ok(());
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
        Ok(())
    }

    fn semi_anti_join<const SEMI: bool, Key, IT>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &DataBlock,
    ) -> Result<DataBlock>
    where
        Key: HashTableKeyable + Clone + 'static,
        IT: Iterator<Item = Key> + TrustedLen,
    {
        let probe_indexs = &mut probe_state.probe_indexs;
        let valids = &probe_state.valids;

        for (i, key) in keys_iter.enumerate() {
            let probe_result_ptr = if self.hash_join_desc.from_correlated_subquery {
                hash_table.find_key(&key)
            } else {
                Self::probe_key(hash_table, key, valids, i)
            };

            match (probe_result_ptr, SEMI) {
                (Some(_), true) | (None, false) => {
                    probe_indexs.push(i as u32);
                }
                _ => {}
            }
        }
        DataBlock::block_take_by_indices(input, probe_indexs)
    }

    fn semi_anti_join_with_other_conjunct<const SEMI: bool, Key, IT>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &DataBlock,
    ) -> Result<DataBlock>
    where
        Key: HashTableKeyable + Clone + 'static,
        IT: Iterator<Item = Key> + TrustedLen,
    {
        let probe_indexs = &mut probe_state.probe_indexs;
        let build_indexs = &mut probe_state.build_indexs;
        let valids = &probe_state.valids;
        let row_state = &mut probe_state.row_state;

        // For semi join, it defaults to all
        row_state.resize(keys_iter.size_hint().0, 0);

        let mut dummys = 0;

        for (i, key) in keys_iter.enumerate() {
            let probe_result_ptr = if self.hash_join_desc.from_correlated_subquery {
                hash_table.find_key(&key)
            } else {
                Self::probe_key(hash_table, key, valids, i)
            };

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
                    // dummy row ptr
                    build_indexs.push(RowPtr {
                        chunk_index: 0,
                        row_index: 0,
                        marker: None,
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

    fn left_or_single_join<const WITH_OTHER_CONJUNCT: bool, Key, IT>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &DataBlock,
    ) -> Result<DataBlock>
    where
        Key: HashTableKeyable + Clone + 'static,
        IT: Iterator<Item = Key> + TrustedLen,
    {
        let probe_indexs = &mut probe_state.probe_indexs;
        let build_indexs = &mut probe_state.build_indexs;
        let valids = &probe_state.valids;

        let row_state = &mut probe_state.row_state;

        if WITH_OTHER_CONJUNCT {
            row_state.resize(keys_iter.size_hint().0, 0);
        }

        let mut validity = MutableBitmap::new();
        for (i, key) in keys_iter.enumerate() {
            let probe_result_ptr = if self.hash_join_desc.from_correlated_subquery {
                hash_table.find_key(&key)
            } else {
                Self::probe_key(hash_table, key, valids, i)
            };

            match probe_result_ptr {
                Some(v) => {
                    let probe_result_ptrs = v.get_value();
                    if self.hash_join_desc.join_type == JoinType::Single
                        && probe_result_ptrs.len() > 1
                    {
                        return Err(ErrorCode::LogicalError(
                            "Scalar subquery can't return more than one row",
                        ));
                    }
                    build_indexs.extend_from_slice(probe_result_ptrs);
                    probe_indexs.extend(std::iter::repeat(i as u32).take(probe_result_ptrs.len()));

                    if WITH_OTHER_CONJUNCT {
                        row_state[i] += probe_result_ptrs.len() as u32;
                    }
                    validity.extend_constant(probe_result_ptrs.len(), true);
                }
                None => {
                    // dummy row ptr
                    build_indexs.push(RowPtr {
                        chunk_index: 0,
                        row_index: 0,
                        marker: None,
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
        let build_block = if !self.hash_join_desc.from_correlated_subquery
            && self.hash_join_desc.join_type == JoinType::Single
            && validity.unset_bits() == input.num_rows()
        {
            // Uncorrelated scalar subquery and no row was returned by subquery
            // We just construct a block with NULLs
            let build_data_schema = self.row_space.data_schema.clone();
            assert_eq!(build_data_schema.fields().len(), 1);
            let data_type = build_data_schema.field(0).data_type().clone();
            let nullable_type = if let DataTypeImpl::Nullable(..) = data_type {
                data_type
            } else {
                NullableType::new_impl(data_type)
            };
            let null_column =
                nullable_type.create_column(&vec![DataValue::Null; input.num_rows()])?;
            DataBlock::create(build_data_schema, vec![null_column])
        } else {
            self.row_space.gather(build_indexs)?
        };

        let nullable_columns = if self.row_space.datablocks().is_empty() && !build_indexs.is_empty()
        {
            build_block
                .columns()
                .iter()
                .map(|c| {
                    c.data_type()
                        .create_constant_column(&DataValue::Null, build_indexs.len())
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            build_block
                .columns()
                .iter()
                .map(|c| Self::set_validity(c, &validity))
                .collect::<Result<Vec<_>>>()?
        };
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
            let inner = Self::set_validity(col.inner(), &validity.slice(0, 1))?;
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

    #[inline]
    fn probe_key<Key: HashTableKeyable>(
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        key: Key,
        valids: &Option<Bitmap>,
        i: usize,
    ) -> Option<*mut KeyValueEntity<Key, Vec<RowPtr>>> {
        if valids.as_ref().map_or(true, |v| v.get_bit(i)) {
            return hash_table.find_key(&key);
        }
        None
    }
}
