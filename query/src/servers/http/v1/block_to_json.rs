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

use common_datablocks::DataBlock;
use common_datavalues::arrays::DFPrimitiveArray;
use common_datavalues::DFPrimitiveType;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;
use serde::Serialize;
use serde_json::Value as JsonValue;

fn vec_to_json<T>(column: Vec<T>) -> Vec<JsonValue>
where T: Serialize {
    column
        .iter()
        .map(|v| serde_json::to_value(v).unwrap())
        .collect()
}

fn primitive_column_to_json_nullable<T>(array: &DFPrimitiveArray<T>) -> Vec<JsonValue>
where T: DFPrimitiveType + Serialize {
    vec_to_json(array.collect_values())
}

fn primitive_column_to_json<T>(array: &DFPrimitiveArray<T>) -> Vec<JsonValue>
where T: DFPrimitiveType + Serialize {
    vec_to_json(array.inner().values().as_slice().to_vec())
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

fn bad_type(data_type: &DataType) -> ErrorCode {
    ErrorCode::BadDataValueType(format!("Unsupported column type:{:?}", data_type))
}

pub(crate) fn block_to_json(block: DataBlock) -> Result<Vec<Vec<JsonValue>>> {
    let mut col_table = Vec::new();
    let columns_size = block.columns().len();
    for col_index in 0..columns_size {
        let column = block.column(col_index);
        let series = column.to_array()?;

        let field = block.schema().field(col_index);
        let data_type = field.data_type();
        let json_column: Vec<JsonValue> = match field.is_nullable() {
            true => match data_type {
                DataType::Int8 => primitive_column_to_json_nullable(series.i8()?),
                DataType::Int16 => primitive_column_to_json_nullable(series.i16()?),
                DataType::Int32 => primitive_column_to_json_nullable(series.i32()?),
                DataType::Int64 => primitive_column_to_json_nullable(series.i64()?),
                DataType::UInt8 => primitive_column_to_json_nullable(series.u8()?),
                DataType::UInt16 => primitive_column_to_json_nullable(series.u16()?),
                DataType::UInt32 => primitive_column_to_json_nullable(series.u32()?),
                DataType::UInt64 => primitive_column_to_json_nullable(series.u64()?),
                DataType::Float32 => primitive_column_to_json_nullable(series.f32()?),
                DataType::Float64 => primitive_column_to_json_nullable(series.f64()?),
                DataType::String => vec_to_json(
                    series
                        .string()?
                        .collect_values()
                        .iter()
                        .map(|o| o.as_ref().map(|v| String::from_utf8(v.clone()).unwrap()))
                        .collect(),
                ),
                DataType::Boolean => vec_to_json(series.bool()?.collect_values()),
                // TODO(youngsofun): support other DataType
                _ => return Err(bad_type(data_type)),
            },
            false => match data_type {
                DataType::Int8 => primitive_column_to_json(series.i8()?),
                DataType::Int16 => primitive_column_to_json(series.i16()?),
                DataType::Int32 => primitive_column_to_json(series.i32()?),
                DataType::Int64 => primitive_column_to_json(series.i64()?),
                DataType::UInt8 => primitive_column_to_json(series.u8()?),
                DataType::UInt16 => primitive_column_to_json(series.u16()?),
                DataType::UInt32 => primitive_column_to_json(series.u32()?),
                DataType::UInt64 => primitive_column_to_json(series.u64()?),
                DataType::Float32 => primitive_column_to_json(series.f32()?),
                DataType::Float64 => primitive_column_to_json(series.f64()?),
                DataType::Boolean => vec_to_json(series.bool()?.into_no_null_iter().collect()),
                DataType::String => vec_to_json(
                    series
                        .string()?
                        .into_no_null_iter()
                        .map(|v| String::from_utf8(v.to_vec()).unwrap())
                        .collect(),
                ),
                // TODO(youngsofun): support other DataType
                _ => return Err(bad_type(data_type)),
            },
        };
        col_table.push(json_column);
    }

    Ok(transpose(col_table))
}
