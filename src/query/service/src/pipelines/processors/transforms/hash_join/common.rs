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

use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FilterExecutor;
use databend_common_expression::FunctionContext;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::arrow::or_validities;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::HashJoinState;
use super::desc::MARKER_KIND_FALSE;
use super::desc::MARKER_KIND_NULL;
use super::desc::MARKER_KIND_TRUE;
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
        let marker_column = NullableColumn::new_column(boolean_column, validity.into());
        Ok(DataBlock::new_from_columns(vec![marker_column]))
    }

    // return (result data block, filtered indices, all_true, all_false).
    pub(crate) fn get_other_predicate_result_block<'a>(
        &self,
        filter_executor: &'a mut FilterExecutor,
        data_block: DataBlock,
    ) -> Result<(DataBlock, &'a [u32], bool, bool)> {
        let origin_count = data_block.num_rows();
        let result_block = filter_executor.filter(data_block)?;
        let result_count = result_block.num_rows();
        Ok((
            result_block,
            &filter_executor.true_selection()[0..result_count],
            result_count == origin_count,
            result_count == 0,
        ))
    }

    // return (result data block, filtered indices, all_true, all_false).
    pub(crate) fn get_other_predicate_selection<'a>(
        &self,
        filter_executor: &'a mut FilterExecutor,
        data_block: &DataBlock,
    ) -> Result<(&'a [u32], bool, bool)> {
        let origin_count = data_block.num_rows();
        let result_count = filter_executor.select(data_block)?;
        Ok((
            &filter_executor.true_selection()[0..result_count],
            result_count == origin_count,
            result_count == 0,
        ))
    }

    pub(crate) fn get_nullable_filter_column(
        &self,
        merged_block: &DataBlock,
        filter: &Expr,
        func_ctx: &FunctionContext,
    ) -> Result<NullableColumn<BooleanType>> {
        let evaluator = Evaluator::new(merged_block, func_ctx, &BUILTIN_FUNCTIONS);
        let filter_vector = evaluator
            .run(filter)?
            .convert_to_full_column(filter.data_type(), merged_block.num_rows());

        match filter_vector {
            Column::Nullable(_) => {
                Ok(NullableType::<BooleanType>::try_downcast_column(&filter_vector).unwrap())
            }
            other => {
                let validity = Bitmap::new_constant(true, other.len());
                let column = NullableColumn::new(other, validity);
                Ok(column.try_downcast().unwrap())
            }
        }
    }
}

impl HashJoinState {
    /// if all cols in the same row are all null, we mark this row as null.
    pub(crate) fn init_markers(&self, cols: ProjectedBlock, num_rows: usize, markers: &mut [u8]) {
        if cols
            .iter()
            .any(|entry| entry.data_type().is_nullable_or_null())
        {
            let mut valids = None;
            for entry in cols.iter() {
                match entry.to_column() {
                    Column::Nullable(c) => {
                        let bitmap = &c.validity;
                        if bitmap.null_count() == 0 {
                            valids = Some(Bitmap::new_constant(true, num_rows));
                            break;
                        } else {
                            valids = or_validities(valids, Some(bitmap.clone()));
                        }
                    }
                    Column::Null { .. } => {}
                    _ => {
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

pub fn wrap_true_validity(
    entry: &BlockEntry,
    num_rows: usize,
    true_validity: &Bitmap,
) -> BlockEntry {
    let col = entry.to_column();
    if col.is_null() || col.is_nullable() {
        entry.clone()
    } else {
        let mut validity = true_validity.clone();
        validity.slice(0, num_rows);
        NullableColumn::new_column(col, validity).into()
    }
}

pub fn wrap_nullable_block(input: &DataBlock) -> DataBlock {
    let input_num_rows = input.num_rows();
    let true_validity = Bitmap::new_constant(true, input_num_rows);
    let nullable_columns = input
        .columns()
        .iter()
        .map(|c| wrap_true_validity(c, input_num_rows, &true_validity))
        .collect::<Vec<_>>();
    DataBlock::new(nullable_columns, input_num_rows)
}
