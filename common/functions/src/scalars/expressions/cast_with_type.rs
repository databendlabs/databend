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

use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::compute::cast;
use common_arrow::arrow::compute::cast::CastOptions as ArrowOption;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use super::cast_from_datetimes::cast_from_date16;
use super::cast_from_datetimes::cast_from_date32;
use super::cast_from_string::cast_from_string;
use crate::scalars::expressions::cast_from_datetimes::cast_from_datetime32;
use crate::scalars::expressions::cast_from_datetimes::cast_from_datetime64;

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct CastOptions {
    pub exception_mode: ExceptionMode,
    pub parsing_mode: ParsingMode,
}

pub const DEFAULT_CAST_OPTIONS: CastOptions = CastOptions {
    exception_mode: ExceptionMode::Throw,
    parsing_mode: ParsingMode::Strict,
};

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum ExceptionMode {
    /// Throw exception if value cannot be parsed.
    Throw,
    /// Fill with zero or default if value cannot be parsed.
    Zero,
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum ParsingMode {
    Strict,
    Partial,
}

impl CastOptions {
    fn as_arrow(&self) -> ArrowOption {
        ArrowOption {
            wrapped: true,
            partial: self.parsing_mode == ParsingMode::Partial,
        }
    }
}

pub fn cast_column_field(
    column_with_field: &ColumnWithField,
    data_type: &DataTypePtr,
) -> Result<ColumnRef> {
    cast_with_type(
        column_with_field.column(),
        column_with_field.data_type(),
        data_type,
        &DEFAULT_CAST_OPTIONS,
    )
}

// No logical type is specified
// Use Default options
pub fn default_column_cast(column: &ColumnRef, data_type: &DataTypePtr) -> Result<ColumnRef> {
    cast_with_type(
        column,
        &column.data_type(),
        data_type,
        &DEFAULT_CAST_OPTIONS,
    )
}

pub fn cast_with_type(
    column: &ColumnRef,
    from_type: &DataTypePtr,
    data_type: &DataTypePtr,
    cast_options: &CastOptions,
) -> Result<ColumnRef> {
    // they are pyhsically the same type
    if &column.data_type() == data_type {
        return Ok(column.clone());
    }

    if data_type.data_type_id() == TypeID::Null {
        return Ok(Arc::new(NullColumn::new(column.len())));
    }

    if from_type.data_type_id() == TypeID::Null {
        //all is null
        if data_type.is_nullable() {
            return data_type.create_constant_column(&DataValue::Null, column.len());
        } else if data_type.data_type_id() == TypeID::Boolean {
            return data_type.create_constant_column(&DataValue::Boolean(false), column.len());
        }
        return Err(ErrorCode::BadDataValueType(
            "Can't cast column from null into non-nullable type".to_string(),
        ));
    }

    if column.is_const() {
        let col: &ConstColumn = Series::check_get(column)?;
        let inner = col.inner();
        let res = cast_with_type(inner, from_type, data_type, cast_options)?;
        return Ok(ConstColumn::new(res, column.len()).arc());
    }

    let nonull_from_type = remove_nullable(from_type);
    let nonull_data_type = remove_nullable(data_type);

    let (result, valids) = match nonull_from_type.data_type_id() {
        TypeID::String => cast_from_string(column, &nonull_data_type, cast_options),
        TypeID::Date16 => cast_from_date16(column, &nonull_data_type, cast_options),
        TypeID::Date32 => cast_from_date32(column, &nonull_data_type, cast_options),
        TypeID::DateTime32 => cast_from_datetime32(column, &nonull_data_type, cast_options),
        TypeID::DateTime64 => {
            cast_from_datetime64(column, &nonull_from_type, &nonull_data_type, cast_options)
        }
        // TypeID::Interval => arrow_cast_compute(column, &nonull_data_type, cast_options),
        _ => arrow_cast_compute(column, &nonull_data_type, cast_options),
    }?;

    let (all_nulls, source_valids) = column.validity();
    let bitmap = combine_validities_2(source_valids.cloned(), valids);

    if data_type.is_nullable() {
        return Ok(Arc::new(NullableColumn::new_from_opt(result, bitmap)));
    }

    if let Some(bitmap) = bitmap {
        let null_cnt = bitmap.null_count();
        let source_null_cnt = match (all_nulls, source_valids) {
            (true, _) => column.len(),
            (false, None) => 0,
            (false, Some(b)) => b.null_count(),
        };

        if cast_options.exception_mode == ExceptionMode::Throw
            && (from_type.is_nullable() && null_cnt > source_null_cnt)
            || (!from_type.is_nullable() && null_cnt > 0)
        {
            // TODO get the data to error msg
            return Err(ErrorCode::BadDataValueType(format!(
                "Cast error happens in casting from {} to {}",
                from_type.name(),
                data_type.name()
            )));
        }
    }

    Ok(result)
}

// cast using arrow's cast compute
pub fn arrow_cast_compute(
    column: &ColumnRef,
    data_type: &DataTypePtr,
    cast_options: &CastOptions,
) -> Result<(ColumnRef, Option<Bitmap>)> {
    let arrow_array = column.as_arrow_array();
    let arrow_options = cast_options.as_arrow();
    let result = cast::cast(arrow_array.as_ref(), &data_type.arrow_type(), arrow_options)?;
    let result: ArrayRef = Arc::from(result);
    let bitmap = result.validity().cloned();
    Ok((result.into_column(), bitmap))
}

pub fn new_mutable_bitmap(size: usize, valid: bool) -> MutableBitmap {
    let mut bitmap = MutableBitmap::with_capacity(size);
    bitmap.extend_constant(size, valid);

    bitmap
}
