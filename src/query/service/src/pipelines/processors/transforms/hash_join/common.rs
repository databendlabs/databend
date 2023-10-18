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
use common_exception::Result;
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
use common_expression::FunctionContext;
use common_expression::Value;
use common_functions::BUILTIN_FUNCTIONS;
use common_sql::executor::cast_expr_to_non_null_boolean;

use super::desc::MARKER_KIND_FALSE;
use super::desc::MARKER_KIND_NULL;
use super::desc::MARKER_KIND_TRUE;
use super::HashJoinState;
use crate::pipelines::processors::transforms::hash_join::HashJoinProbeState;

/// Some common methods for hash join.

impl HashJoinProbeState {
    // Merge build chunk and probe chunk that have the same number of rows
    pub(crate) fn merge_eq_block(
        &self,
        probe_block: Option<DataBlock>,
        build_block: Option<DataBlock>,
        num_rows: usize,
    ) -> DataBlock {
        match (probe_block, build_block) {
            (Some(mut probe_block), Some(build_block)) => {
                probe_block.merge_block(build_block);
                probe_block
            }
            (Some(probe_block), None) => probe_block,
            (None, Some(build_block)) => build_block,
            (None, None) => DataBlock::new(vec![], num_rows),
        }
    }

    pub(crate) fn create_marker_block(
        &self,
        has_null: bool,
        markers: &mut [u8],
        num_rows: usize,
    ) -> Result<DataBlock> {
        let mut validity = MutableBitmap::with_capacity(num_rows);
        let mut boolean_bit_map = MutableBitmap::with_capacity(num_rows);
        let mut row_index = 0;
        while row_index < num_rows {
            let marker = if markers[row_index] == MARKER_KIND_FALSE && has_null {
                MARKER_KIND_NULL
            } else {
                markers[row_index]
            };
            if marker == MARKER_KIND_NULL {
                validity.push(false);
            } else {
                validity.push(true);
            }
            if marker == MARKER_KIND_TRUE {
                boolean_bit_map.push(true);
            } else {
                boolean_bit_map.push(false);
            }
            markers[row_index] = MARKER_KIND_FALSE;
            row_index += 1;
        }
        let boolean_column = Column::Boolean(boolean_bit_map.into());
        let marker_column = Column::Nullable(Box::new(NullableColumn {
            column: boolean_column,
            validity: validity.into(),
        }));
        Ok(DataBlock::new_from_columns(vec![marker_column]))
    }

    // return an (option bitmap, all_true, all_false).
    pub(crate) fn get_other_filters(
        &self,
        merged_block: &DataBlock,
        filter: &Expr,
        func_ctx: &FunctionContext,
    ) -> Result<(Option<Bitmap>, bool, bool)> {
        let filter = cast_expr_to_non_null_boolean(filter.clone())?;
        let evaluator = Evaluator::new(merged_block, func_ctx, &BUILTIN_FUNCTIONS);
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
        func_ctx: &FunctionContext,
    ) -> Result<Column> {
        let evaluator = Evaluator::new(merged_block, func_ctx, &BUILTIN_FUNCTIONS);
        let filter_vector: Value<AnyType> = evaluator.run(filter)?;
        let filter_vector =
            filter_vector.convert_to_full_column(filter.data_type(), merged_block.num_rows());

        match filter_vector {
            Column::Nullable(_) => Ok(filter_vector),
            other => Ok(Column::Nullable(Box::new(NullableColumn {
                validity: Bitmap::new_constant(true, other.len()),
                column: other,
            }))),
        }
    }
}

impl HashJoinState {
    pub(crate) fn init_markers(
        &self,
        cols: &[(Column, DataType)],
        num_rows: usize,
        markers: &mut [u8],
    ) {
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
                            valids = Some(Bitmap::new_constant(true, num_rows));
                            break;
                        } else {
                            valids = or_validities(valids, Some(bitmap.clone()));
                        }
                    }
                    Column::Null { .. } => {}
                    _c => {
                        valids = Some(Bitmap::new_constant(true, num_rows));
                        break;
                    }
                }
            }
            if let Some(v) = valids {
                let mut idx = 0;
                while idx < num_rows {
                    if !v.get_bit(idx) {
                        markers[idx] = MARKER_KIND_NULL;
                    }
                    idx += 1;
                }
            }
        }
    }
}

pub(crate) fn set_true_validity(
    column: &BlockEntry,
    num_rows: usize,
    true_validity: &Bitmap,
) -> BlockEntry {
    let (value, data_type) = (&column.value, &column.data_type);
    let col = value.convert_to_full_column(data_type, num_rows);
    if matches!(col, Column::Null { .. }) || col.as_nullable().is_some() {
        column.clone()
    } else {
        let mut validity = true_validity.clone();
        validity.slice(0, num_rows);
        let col = Column::Nullable(Box::new(NullableColumn {
            column: col,
            validity,
        }));
        BlockEntry::new(data_type.wrap_nullable(), Value::Column(col))
    }
}
