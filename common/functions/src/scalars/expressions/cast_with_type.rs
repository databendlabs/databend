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

use std::fmt;

use common_arrow::arrow::temporal_conversions::EPOCH_DAYS_FROM_CE;
use common_datavalues2::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

fn cast_column_field(
    column_with_field: &ColumnWithField,
    data_type: &DataTypePtr,
) -> Result<ColumnRef> {
    cast_with_type(
        column_with_field.column(),
        column_with_field.data_type(),
        data_type,
    )
}

fn cast_with_type(
    column: &ColumnRef,
    from_type: &DataTypePtr,
    to_type: &DataTypePtr,
) -> Result<ColumnRef> {
    // they are pyhsically the same type
    if &column.data_type() == to_type {
        return Ok(column.clone());
    }

    if to_type.data_type_id() == TypeID::Null {
        return Ok(Arc::new(NullColumn::new(column.len())));
    }

    match from_type.data_type_id() {
        TypeID::Null => todo!(),
        TypeID::Nullable => todo!(),
        TypeID::Boolean => todo!(),
        TypeID::UInt8 => todo!(),
        TypeID::UInt16 => todo!(),
        TypeID::UInt32 => todo!(),
        TypeID::UInt64 => todo!(),
        TypeID::Int8 => todo!(),
        TypeID::Int16 => todo!(),
        TypeID::Int32 => todo!(),
        TypeID::Int64 => todo!(),
        TypeID::Float32 => todo!(),
        TypeID::Float64 => todo!(),
        TypeID::String => todo!(),
        TypeID::Date16 => todo!(),
        TypeID::Date32 => todo!(),
        TypeID::DateTime32 => todo!(),
        TypeID::DateTime64 => todo!(),
        TypeID::Interval => todo!(),
        TypeID::Array => todo!(),
        TypeID::Struct => todo!(),
    }
}

#[inline]
fn cast_from_null(column: &ColumnRef, data_type: &DataTypePtr) -> Result<ColumnRef> {
    //all is null
    if data_type.is_nullable() {
        return data_type.create_constant_column(&DataValue::Null, column.len());
    }
    Err(ErrorCode::BadDataValueType(
        "Can't cast column from null into non-nullable type".to_string(),
    ))
}

#[inline]
fn cast_from_nullable(column: &ColumnRef, data_type: &DataTypePtr) -> Result<ColumnRef> {
    //all is null
    if data_type.is_nullable() {
        return data_type.create_constant_column(&DataValue::Null, column.len());
    }
    Err(ErrorCode::BadDataValueType(
        "Can't cast column from null into non-nullable type".to_string(),
    ))
}
