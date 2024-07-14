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

#![feature(box_patterns)]
#![feature(try_blocks)]

use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;

extern crate core;

mod block;
mod column;
mod common;
mod decimal;
mod fill_field_default_value;
mod group_by;
mod input_columns;
mod kernel;
mod meta_scalar;
mod row;
mod schema;
mod serde;
mod sort;
mod types;

fn rand_block_for_all_types(num_rows: usize) -> DataBlock {
    let types = get_all_test_data_types();
    let mut columns = Vec::with_capacity(types.len());
    for data_type in types.iter() {
        columns.push(Column::random(data_type, num_rows, None));
    }

    let block = DataBlock::new_from_columns(columns);
    block.check_valid().unwrap();

    block
}

fn rand_block_for_simple_types(num_rows: usize) -> DataBlock {
    let types = get_simple_types();
    let mut columns = Vec::with_capacity(types.len());
    for data_type in types.iter() {
        columns.push(Column::random(data_type, num_rows, None));
    }

    let block = DataBlock::new_from_columns(columns);
    block.check_valid().unwrap();

    block
}

fn get_simple_types() -> Vec<DataType> {
    vec![
        DataType::Null,
        DataType::Timestamp,
        DataType::Date,
        DataType::Number(NumberDataType::UInt8),
        DataType::Number(NumberDataType::UInt16),
        DataType::Number(NumberDataType::UInt32),
        DataType::Number(NumberDataType::UInt64),
        DataType::Number(NumberDataType::Int8),
        DataType::Number(NumberDataType::Int16),
        DataType::Number(NumberDataType::Int32),
        DataType::Number(NumberDataType::Int64),
        DataType::Number(NumberDataType::Float32),
        DataType::Number(NumberDataType::Float64),
        DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
            precision: 10,
            scale: 2,
        })),
        DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
            precision: 35,
            scale: 3,
        })),
        DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt32))),
        DataType::Nullable(Box::new(DataType::String)),
    ]
}

fn get_all_test_data_types() -> Vec<DataType> {
    vec![
        DataType::Null,
        DataType::EmptyArray,
        DataType::EmptyMap,
        DataType::Boolean,
        DataType::Binary,
        DataType::String,
        DataType::Bitmap,
        DataType::Variant,
        DataType::Timestamp,
        DataType::Date,
        DataType::Number(NumberDataType::UInt8),
        DataType::Number(NumberDataType::UInt16),
        DataType::Number(NumberDataType::UInt32),
        DataType::Number(NumberDataType::UInt64),
        DataType::Number(NumberDataType::Int8),
        DataType::Number(NumberDataType::Int16),
        DataType::Number(NumberDataType::Int32),
        DataType::Number(NumberDataType::Int64),
        DataType::Number(NumberDataType::Float32),
        DataType::Number(NumberDataType::Float64),
        DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
            precision: 10,
            scale: 2,
        })),
        DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
            precision: 35,
            scale: 3,
        })),
        DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt32))),
        DataType::Nullable(Box::new(DataType::String)),
        DataType::Array(Box::new(DataType::Number(NumberDataType::UInt32))),
        DataType::Map(Box::new(DataType::Tuple(vec![
            DataType::Number(NumberDataType::UInt64),
            DataType::String,
        ]))),
    ]
}
