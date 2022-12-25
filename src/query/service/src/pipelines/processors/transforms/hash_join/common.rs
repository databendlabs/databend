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
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::combine_validities_3;
use common_datavalues::BooleanColumn;
use common_datavalues::BooleanType;
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
use common_datavalues::Series;
use common_exception::ErrorCode;
use common_exception::Result;
use common_hashtable::HashtableLike;

use crate::pipelines::processors::transforms::hash_join::desc::MarkerKind;
use crate::pipelines::processors::transforms::hash_join::row::RowPtr;
use crate::pipelines::processors::JoinHashTable;
use crate::sql::evaluator::EvalNode;
use crate::sql::plans::JoinType;

/// Some common methods for hash join.
impl JoinHashTable {
    // Merge build block and probe block that have the same number of rows
    pub(crate) fn merge_eq_block(
        &self,
        build_block: &DataBlock,
        probe_block: &DataBlock,
    ) -> Result<DataBlock> {
        let mut probe_block = probe_block.clone();
        for (col, field) in build_block
            .columns()
            .iter()
            .zip(build_block.schema().fields().iter())
        {
            probe_block = probe_block.add_column(col.clone(), field.clone())?;
        }
        Ok(probe_block)
    }

    #[inline]
    pub(crate) fn probe_key<'a, H: HashtableLike<Value = Vec<RowPtr>>>(
        &self,
        hash_table: &'a H,
        key: &'a H::Key,
        valids: &Option<Bitmap>,
        i: usize,
    ) -> Option<H::EntryRef<'a>> {
        if valids.as_ref().map_or(true, |v| v.get_bit(i)) {
            return hash_table.entry(key);
        }
        None
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
                .ok_or_else(|| ErrorCode::Internal("Invalid mark join"))?
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
            if col.is_empty() {
                let ty = col.data_type();
                return ty.create_constant_column(&DataValue::Null, validity.len());
            }
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

    pub(crate) fn find_unmatched_build_indexes(
        &self,
        row_state: &[Vec<usize>],
    ) -> Result<Vec<RowPtr>> {
        // For right/full join, build side will appear at least once in the joined table
        // Find the unmatched rows in build side
        let mut unmatched_build_indexes = vec![];
        for (chunk_index, chunk) in self.row_space.chunks.read().unwrap().iter().enumerate() {
            for row_index in 0..chunk.num_rows() {
                if row_state[chunk_index][row_index] == 0 {
                    unmatched_build_indexes.push(RowPtr::new(chunk_index, row_index));
                }
            }
        }
        Ok(unmatched_build_indexes)
    }

    // For unmatched build index, the method will produce null probe block
    // Then merge null_probe_block with unmatched_build_block
    pub(crate) fn null_blocks_for_right_join(
        &self,
        unmatched_build_indexes: &Vec<RowPtr>,
    ) -> Result<DataBlock> {
        let mut unmatched_build_block = self.row_space.gather(unmatched_build_indexes)?;
        if self.hash_join_desc.join_type == JoinType::Full {
            let nullable_unmatched_build_columns = unmatched_build_block
                .columns()
                .iter()
                .map(|c| {
                    let mut probe_validity = MutableBitmap::new();
                    probe_validity.extend_constant(c.len(), true);
                    let probe_validity: Bitmap = probe_validity.into();
                    Self::set_validity(c, &probe_validity)
                })
                .collect::<Result<Vec<_>>>()?;
            unmatched_build_block =
                DataBlock::create(self.row_space.schema(), nullable_unmatched_build_columns);
        };
        // Create null block for unmatched rows in probe side
        let null_probe_block = DataBlock::create(
            self.probe_schema.clone(),
            self.probe_schema
                .fields()
                .iter()
                .map(|df| {
                    df.data_type()
                        .clone()
                        .create_constant_column(&DataValue::Null, unmatched_build_indexes.len())
                })
                .collect::<Result<Vec<_>>>()?,
        );
        self.merge_eq_block(&unmatched_build_block, &null_probe_block)
    }

    // Final row_state for right join
    // Record row in build side that is matched how many rows in probe side.
    pub(crate) fn row_state_for_right_join(&self) -> Result<Vec<Vec<usize>>> {
        let build_indexes = self.hash_join_desc.join_state.build_indexes.read();
        let chunks = self.row_space.chunks.read().unwrap();
        let mut row_state = Vec::with_capacity(chunks.len());
        for chunk in chunks.iter() {
            let mut rows = Vec::with_capacity(chunk.num_rows());
            for _row_index in 0..chunk.num_rows() {
                rows.push(0);
            }
            row_state.push(rows);
        }

        for row_ptr in build_indexes.iter() {
            if self.hash_join_desc.join_type == JoinType::Full
                && row_ptr.marker == Some(MarkerKind::False)
            {
                continue;
            }
            row_state[row_ptr.chunk_index][row_ptr.row_index] += 1;
        }
        Ok(row_state)
    }

    pub(crate) fn rest_block(&self) -> Result<DataBlock> {
        let rest_probe_blocks = self.hash_join_desc.join_state.rest_probe_blocks.read();
        if rest_probe_blocks.is_empty() {
            return Ok(DataBlock::empty());
        }
        let probe_block = DataBlock::concat_blocks(&rest_probe_blocks)?;
        let rest_build_indexes = self.hash_join_desc.join_state.rest_build_indexes.read();
        let mut build_block = self.row_space.gather(&rest_build_indexes)?;
        // For left join, wrap nullable for build block
        if matches!(
            self.hash_join_desc.join_type,
            JoinType::Left | JoinType::Single | JoinType::Full
        ) {
            let validity = self.hash_join_desc.join_state.validity.read();
            let validity = (*validity).clone().into();
            let nullable_columns = if self.row_space.datablocks().is_empty() {
                build_block
                    .columns()
                    .iter()
                    .map(|c| {
                        c.data_type()
                            .create_constant_column(&DataValue::Null, rest_build_indexes.len())
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                build_block
                    .columns()
                    .iter()
                    .map(|c| Self::set_validity(c, &validity))
                    .collect::<Result<Vec<_>>>()?
            };
            build_block =
                DataBlock::create(self.row_space.data_schema.clone(), nullable_columns.clone());
        }

        self.merge_eq_block(&build_block, &probe_block)
    }

    // Add `data_block` for build table to `row_space`
    pub(crate) fn add_build_block(&self, data_block: DataBlock) -> Result<()> {
        let func_ctx = self.ctx.try_get_function_context()?;
        let build_cols = self
            .hash_join_desc
            .build_keys
            .iter()
            .map(|expr| Ok(expr.eval(&func_ctx, &data_block)?.vector().clone()))
            .collect::<Result<Vec<ColumnRef>>>()?;
        self.row_space.push_cols(data_block, build_cols)
    }
}
