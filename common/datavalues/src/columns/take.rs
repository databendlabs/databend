// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::types::Index;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;

pub type RowIndex = (usize, usize);
pub type ChunkRowIndex = (usize, usize, usize);

impl Series {
    pub fn take<I: Index>(column: &ColumnRef, indices: &[I]) -> Result<ColumnRef> {
        if column.is_const() || column.is_null() {
            Ok(column.slice(0, indices.len()))
        } else if column.is_nullable() {
            let nullable_c: &NullableColumn = unsafe { Series::static_cast(column) };
            let inner_result = Self::take(nullable_c.inner(), indices)?;

            let values = nullable_c.ensure_validity();
            let values = indices.iter().map(|index| values.get_bit(index.to_usize()));
            let validity_result = Bitmap::from_trusted_len_iter(values);

            Ok(NullableColumn::wrap_inner(
                inner_result,
                Some(validity_result),
            ))
        } else {
            let type_id = column.data_type_id().to_physical_type();

            with_match_scalar_type!(type_id, |$T| {
                let col: &<$T as Scalar>::ColumnType = Series::check_get(column)?;
                type Builder = <<$T as Scalar>::ColumnType as ScalarColumn>::Builder;
                let mut builder = Builder::with_capacity_meta(indices.len(), col.column_meta());

                for index in indices {
                    builder.push(col.get_data(index.to_usize()));
                }

                let result = builder.finish();
                Ok(Arc::new(result))
            },
            {
                Err(ErrorCode::BadDataValueType(format!(
                    "Column with type: {:?} does not support take",
                    type_id
                )))
            })
        }
    }
    
    pub fn take_row_indices(column: &ColumnRef, indices: &[RowIndex]) -> Result<ColumnRef> {
        if column.is_const() || column.is_null() {
            Ok(column.slice(0, indices.len()))
        } else if column.is_nullable() {
            let nullable_c: &NullableColumn = unsafe { Series::static_cast(column) };
            let inner_result = Self::take_row_indices(nullable_c.inner(), indices)?;

            let values = nullable_c.ensure_validity();
            let bool_column = BooleanColumn::from_arrow_data(values.clone()).arc();
            let bool_column_result = Self::take_row_indices(&bool_column, indices)?;
            let bool_column_result: &BooleanColumn =  unsafe { Series::static_cast(&bool_column_result) };
          
            Ok(NullableColumn::wrap_inner(
                inner_result,
                Some(bool_column_result.values().clone()),
            ))
        } else {
            let type_id = column.data_type_id().to_physical_type();

            with_match_scalar_type!(type_id, |$T| {
                let col: &<$T as Scalar>::ColumnType = Series::check_get(column)?;
                type Builder = <<$T as Scalar>::ColumnType as ScalarColumn>::Builder;
                let mut builder = Builder::with_capacity_meta(indices.len(), col.column_meta());

                for (row, size) in indices.iter()  {
                    if *size == 1 {
                        builder.push(col.get_data(*row));
                    } else {
                        builder.pushs(col.get_data(*row), *size);
                    }
                }

                let result = builder.finish();
                Ok(Arc::new(result))
            },
            {
                Err(ErrorCode::BadDataValueType(format!(
                    "Column with type: {:?} does not support take",
                    type_id
                )))
            })
        }
    }
    
    
    pub fn take_chunk_indices(columns: &[ColumnRef], indices: &[ChunkRowIndex]) -> Result<ColumnRef> {
        debug_assert!(!columns.is_empty());
        let columns: Vec<ColumnRef> = columns.iter().map(|c| c.convert_full_column()).collect();
        
        if columns[0].is_nullable() {
            let mut nonull_cols = Vec::with_capacity(columns.len());
            let mut bool_cols = Vec::with_capacity(columns.len());
            
            for col in columns.iter() {
                let nullable_c: &NullableColumn = unsafe { Series::static_cast(col) };
                nonull_cols.push(nullable_c.inner().clone());
                
                let values = nullable_c.ensure_validity();
                let bool_column = BooleanColumn::from_arrow_data(values.clone()).arc();
                bool_cols.push(bool_column);
            }
            let inner_result = Self::take_chunk_indices( &nonull_cols, indices)?;
            let bool_column_result = Self::take_chunk_indices( &bool_cols, indices)?;
            let bool_column_result: &BooleanColumn =  unsafe { Series::static_cast(&bool_column_result) };

            Ok(NullableColumn::wrap_inner(
                inner_result,
                Some(bool_column_result.values().clone()),
            ))
        } else {
            let type_id = columns[0].data_type_id().to_physical_type();
            with_match_scalar_type!(type_id, |$T| {
                let mut cols = Vec::with_capacity(columns.len());
                for col in columns.iter() {
                    let col: &<$T as Scalar>::ColumnType = Series::check_get(col)?;
                    cols.push(col);
                }
                
                type Builder = <<$T as Scalar>::ColumnType as ScalarColumn>::Builder;
                let mut builder = Builder::with_capacity_meta(indices.len(), columns[0].column_meta());

                for (chunk, row, size) in indices.iter() {
                    if *size == 1 {
                        builder.push(cols[*chunk].get_data(*row));
                    } else {
                        builder.pushs(cols[*chunk].get_data(*row), *size);
                    }
                }
                let result = builder.finish();
                Ok(Arc::new(result))
            },
            {
                Err(ErrorCode::BadDataValueType(format!(
                    "Column with type: {:?} does not support take",
                    type_id
                )))
            })
        }
    }
}
