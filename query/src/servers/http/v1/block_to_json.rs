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

use common_datablocks::DataBlock;
use common_datavalues::arrays::DFPrimitiveArray;
use common_datavalues::chrono::TimeZone;
use common_datavalues::chrono::Utc;
use common_datavalues::DFPrimitiveType;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;
use serde::Serialize;
use serde_json::Value as JsonValue;

const DATE_FMT: &str = "%Y-%m-%d";
const TIME_FMT: &str = "%Y-%m-%d %H:%M:%S";

pub(crate) type JsonBlock = Vec<Vec<JsonValue>>;
pub(crate) type JsonBlockRef = Arc<JsonBlock>;

fn to_json_value<T>(v: T) -> JsonValue
where T: Serialize {
    serde_json::to_value(v).unwrap()
}

fn primitive_array_to_json<T>(array: &DFPrimitiveArray<T>) -> Vec<JsonValue>
where T: DFPrimitiveType + Serialize {
    array.into_iter().map(to_json_value).collect()
}

fn primitive_array_to_json_not_null<T>(array: &DFPrimitiveArray<T>) -> Vec<JsonValue>
where T: DFPrimitiveType + Serialize {
    array
        .inner()
        .values()
        .iter()
        .map(|v| serde_json::to_value(v).unwrap())
        .collect()
}

fn transpose(col_table: Vec<Vec<JsonValue>>) -> Vec<Vec<JsonValue>> {
    let num_row = col_table[0].len();
    let mut row_table = Vec::with_capacity(num_row);
    for _ in 0..num_row {
        row_table.push(Vec::with_capacity(col_table.len()));
    }
    for col in col_table {
        for (row_index, row) in row_table.iter_mut().enumerate() {
            row.push(col[row_index].clone());
        }
    }
    row_table
}

fn date_number_to_string(x: i64, fmt: &str) -> String {
    Utc.timestamp(x * 24 * 3600, 0_u32).format(fmt).to_string()
}

fn date_array_to_string_array<T>(array: &DFPrimitiveArray<T>, fmt: &str) -> Vec<JsonValue>
where T: DFPrimitiveType + Serialize + Into<i64> {
    array
        .into_iter()
        .map(|o| o.map(|x| date_number_to_string(Into::<i64>::into(*x), fmt)))
        .map(to_json_value)
        .collect()
}

fn date_array_to_string_array_not_null<T>(
    array: &DFPrimitiveArray<T>,
    fmt: &str,
) -> Vec<JsonValue>
where
    T: DFPrimitiveType + Serialize + Into<i64>,
{
    array
        .into_no_null_iter()
        .map(|x| date_number_to_string(Into::<i64>::into(*x), fmt))
        .map(to_json_value)
        .collect()
}

fn bad_type(data_type: &DataType) -> ErrorCode {
    ErrorCode::BadDataValueType(format!("Unsupported column type:{:?}", data_type))
}

pub fn block_to_json(block: &DataBlock) -> Result<Vec<Vec<JsonValue>>> {
    let mut col_table = Vec::new();
    let columns_size = block.columns().len();
    for col_index in 0..columns_size {
        let column = block.column(col_index);
        let series = column.to_array()?;

        let field = block.schema().field(col_index);
        let data_type = field.data_type();
        let json_column: Vec<JsonValue> = match field.is_nullable() {
            true => match data_type.remove_nullable() {
                DataType::Int8 => primitive_array_to_json(series.i8()?),
                DataType::Int16 => primitive_array_to_json(series.i16()?),
                DataType::Int32 => primitive_array_to_json(series.i32()?),
                DataType::Int64 => primitive_array_to_json(series.i64()?),
                DataType::UInt8 => primitive_array_to_json(series.u8()?),
                DataType::UInt16 => primitive_array_to_json(series.u16()?),
                DataType::UInt32 => primitive_array_to_json(series.u32()?),
                DataType::UInt64 => primitive_array_to_json(series.u64()?),
                DataType::Float32 => primitive_array_to_json(series.f32()?),
                DataType::Float64 => primitive_array_to_json(series.f64()?),
                DataType::String => series
                    .string()?
                    .collect_values()
                    .iter()
                    .map(|o| o.as_ref().map(|v| String::from_utf8(v.clone()).unwrap()))
                    .map(to_json_value)
                    .collect(),
                DataType::Boolean => series.bool()?.into_iter().map(to_json_value).collect(),
                DataType::Date16 => date_array_to_string_array(series.u16()?, DATE_FMT),
                DataType::Date32 => date_array_to_string_array(series.i32()?, DATE_FMT),
                // TODO(youngsofun): add time zone?
                DataType::DateTime32(_) => date_array_to_string_array(series.i32()?, TIME_FMT),
                // TODO(youngsofun): support other DataType
                _ => return Err(bad_type(data_type)),
            },
            false => match data_type {
                DataType::Int8 => primitive_array_to_json_not_null(series.i8()?),
                DataType::Int16 => primitive_array_to_json_not_null(series.i16()?),
                DataType::Int32 => primitive_array_to_json_not_null(series.i32()?),
                DataType::Int64 => primitive_array_to_json_not_null(series.i64()?),
                DataType::UInt8 => primitive_array_to_json_not_null(series.u8()?),
                DataType::UInt16 => primitive_array_to_json_not_null(series.u16()?),
                DataType::UInt32 => primitive_array_to_json_not_null(series.u32()?),
                DataType::UInt64 => primitive_array_to_json_not_null(series.u64()?),
                DataType::Float32 => primitive_array_to_json_not_null(series.f32()?),
                DataType::Float64 => primitive_array_to_json_not_null(series.f64()?),
                DataType::Boolean => series
                    .bool()?
                    .into_no_null_iter()
                    .map(to_json_value)
                    .collect(),
                DataType::String => series
                    .string()?
                    .into_no_null_iter()
                    .map(|v| String::from_utf8(v.to_vec()).unwrap())
                    .map(to_json_value)
                    .collect(),
                DataType::Date16 => date_array_to_string_array_not_null(series.u16()?, DATE_FMT),
                DataType::Date32 => date_array_to_string_array_not_null(series.i32()?, DATE_FMT),
                DataType::DateTime32(_) => {
                    date_array_to_string_array_not_null(series.i32()?, TIME_FMT)
                }
                _ => return Err(bad_type(data_type)),
            },
        };
        col_table.push(json_column);
    }

    Ok(transpose(col_table))
}
