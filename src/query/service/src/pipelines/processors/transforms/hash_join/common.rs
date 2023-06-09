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

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::arrow::constant_bitmap;
use common_expression::arrow::or_validities;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::AnyType;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::Scalar;
use common_expression::Value;
use common_functions::BUILTIN_FUNCTIONS;
use common_hashtable::HashJoinHashtableLike;
use common_hashtable::MarkerKind;
use common_hashtable::RowPtr;
use common_sql::executor::cast_expr_to_non_null_boolean;

use super::HashJoinState;
use crate::pipelines::processors::transforms::hash_join::desc::JOIN_MAX_BLOCK_SIZE;
use crate::pipelines::processors::transforms::hash_join::row::Chunk;
use crate::pipelines::processors::JoinHashTable;
use crate::sql::plans::JoinType;

/// Some common methods for hash join.
impl JoinHashTable {
    // Merge build chunk and probe chunk that have the same number of rows
    pub(crate) fn merge_eq_block(
        &self,
        build_block: &DataBlock,
        probe_block: &DataBlock,
    ) -> Result<DataBlock> {
        let mut probe_block = probe_block.clone();
        for col in build_block.columns() {
            probe_block.add_column(col.clone());
        }
        Ok(probe_block)
    }

    #[inline]
    pub(crate) fn contains<'a, H: HashJoinHashtableLike>(
        &self,
        hash_table: &'a H,
        key: &'a H::Key,
        valids: &Option<Bitmap>,
        i: usize,
    ) -> bool {
        if valids.as_ref().map_or(true, |v| v.get_bit(i)) {
            return hash_table.contains(key);
        }
        false
    }

    #[inline]
    pub(crate) fn probe_key<'a, H: HashJoinHashtableLike>(
        &self,
        hash_table: &'a H,
        key: &'a H::Key,
        valids: &Option<Bitmap>,
        i: usize,
        vec_ptr: *mut RowPtr,
        occupied: usize,
    ) -> (usize, u64) {
        if valids.as_ref().map_or(true, |v| v.get_bit(i)) {
            return hash_table.probe_hash_table(key, vec_ptr, occupied, JOIN_MAX_BLOCK_SIZE);
        }
        (0, 0)
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
        let boolean_column = Column::Boolean(boolean_bit_map.into());
        let marker_column = Column::Nullable(Box::new(NullableColumn {
            column: boolean_column,
            validity: validity.into(),
        }));
        Ok(DataBlock::new_from_columns(vec![marker_column]))
    }

    pub(crate) fn init_markers(cols: &[(Column, DataType)], num_rows: usize) -> Vec<MarkerKind> {
        let mut markers = vec![MarkerKind::False; num_rows];
        if cols
            .iter()
            .any(|(c, _)| matches!(c, Column::Null { .. } | Column::Nullable(_)))
        {
            let mut valids = None;
            for (col, _) in cols.iter() {
                match col {
                    Column::Nullable(c) => {
                        let bitmap = &c.validity;
                        if bitmap.unset_bits() == 0 {
                            let mut m = MutableBitmap::with_capacity(num_rows);
                            m.extend_constant(num_rows, true);
                            valids = Some(m.into());
                            break;
                        } else {
                            valids = or_validities(valids, Some(bitmap.clone()));
                        }
                    }
                    Column::Null { .. } => {}
                    _c => {
                        let mut m = MutableBitmap::with_capacity(num_rows);
                        m.extend_constant(num_rows, true);
                        valids = Some(m.into());
                        break;
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

    pub(crate) fn set_validity(
        column: &BlockEntry,
        num_rows: usize,
        validity: &Bitmap,
    ) -> BlockEntry {
        let (value, data_type) = (&column.value, &column.data_type);
        let col = value.convert_to_full_column(data_type, num_rows);

        if matches!(col, Column::Null { .. }) {
            column.clone()
        } else if let Some(col) = col.as_nullable() {
            if col.len() == 0 {
                return BlockEntry::new(data_type.clone(), Value::Scalar(Scalar::Null));
            }
            // It's possible validity is longer than col.
            let diff_len = validity.len() - col.validity.len();
            let mut new_validity = MutableBitmap::with_capacity(validity.len());
            for (b1, b2) in validity.iter().zip(col.validity.iter()) {
                new_validity.push(b1 & b2);
            }
            new_validity.extend_constant(diff_len, false);
            let col = Column::Nullable(Box::new(NullableColumn {
                column: col.column.clone(),
                validity: new_validity.into(),
            }));
            BlockEntry::new(data_type.clone(), Value::Column(col))
        } else {
            let col = Column::Nullable(Box::new(NullableColumn {
                column: col.clone(),
                validity: validity.clone(),
            }));
            BlockEntry::new(data_type.wrap_nullable(), Value::Column(col))
        }
    }

    // return an (option bitmap, all_true, all_false)
    pub(crate) fn get_other_filters(
        &self,
        merged_block: &DataBlock,
        filter: &Expr,
    ) -> Result<(Option<Bitmap>, bool, bool)> {
        let filter = cast_expr_to_non_null_boolean(filter.clone())?;

        let func_ctx = self.ctx.get_function_context()?;
        let evaluator = Evaluator::new(merged_block, &func_ctx, &BUILTIN_FUNCTIONS);
        let predicates = evaluator
            .run(&filter)?
            .try_downcast::<BooleanType>()
            .unwrap();

        match predicates {
            Value::Scalar(v) => Ok((None, v, !v)),
            Value::Column(s) => {
                let count_zeros = s.unset_bits();
                let all_false = s.len() == count_zeros;
                Ok((Some(s), count_zeros == 0, all_false))
            }
        }
    }

    pub(crate) fn get_nullable_filter_column(
        &self,
        merged_block: &DataBlock,
        filter: &Expr,
    ) -> Result<Column> {
        let func_ctx = self.ctx.get_function_context()?;
        let evaluator = Evaluator::new(merged_block, &func_ctx, &BUILTIN_FUNCTIONS);

        let filter_vector: Value<AnyType> = evaluator.run(filter)?;
        let filter_vector =
            filter_vector.convert_to_full_column(filter.data_type(), merged_block.num_rows());

        match filter_vector {
            Column::Nullable(_) => Ok(filter_vector),
            other => Ok(Column::Nullable(Box::new(NullableColumn {
                validity: constant_bitmap(true, other.len()).into(),
                column: other,
            }))),
        }
    }

    // Add `data_block` for build table to `row_space`
    pub(crate) fn add_build_block(&self, data_block: DataBlock) -> Result<()> {
        let func_ctx = self.ctx.get_function_context()?;
        let mut data_block = data_block;
        if matches!(
            self.hash_join_desc.join_type,
            JoinType::Left | JoinType::Full
        ) {
            let mut validity = MutableBitmap::new();
            validity.extend_constant(data_block.num_rows(), true);
            let validity: Bitmap = validity.into();

            let nullable_columns = data_block
                .columns()
                .iter()
                .map(|c| Self::set_validity(c, validity.len(), &validity))
                .collect::<Vec<_>>();
            data_block = DataBlock::new(nullable_columns, data_block.num_rows());
        }
        let evaluator = Evaluator::new(&data_block, &func_ctx, &BUILTIN_FUNCTIONS);

        let build_cols = self
            .hash_join_desc
            .build_keys
            .iter()
            .map(|expr| {
                let return_type = expr.data_type();
                Ok((
                    evaluator
                        .run(expr)?
                        .convert_to_full_column(return_type, data_block.num_rows()),
                    return_type.clone(),
                ))
            })
            .collect::<Result<_>>()?;

        let chunk = Chunk {
            data_block,
            cols: build_cols,
            keys_state: None,
        };

        {
            // Acquire write lock in current scope
            let mut chunks = self.row_space.chunks.write();
            if self.need_outer_scan() {
                let outer_scan_bitmap = unsafe { &mut *self.outer_scan_bitmap.get() };
                outer_scan_bitmap.push(MutableBitmap::from_len_zeroed(chunk.num_rows()));
            }
            chunks.push(chunk);
        }
        Ok(())
    }
}
