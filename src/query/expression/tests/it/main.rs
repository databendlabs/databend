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
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;

extern crate core;

mod arrow;
mod block;
mod block_thresholds;
mod common;
mod decimal;
mod display;
mod fill_field_default_value;
mod group_by;
mod hilbert;
mod kernel;
mod meta_scalar;

mod schema;
mod serde;
mod sort;
mod types;

mod like_pattern_escape;

fn rand_block_for_all_types(num_rows: usize, filter: DataTypeFilter) -> DataBlock {
    let types = get_all_test_data_types(filter);
    let mut columns = Vec::with_capacity(types.len());
    for data_type in types.iter() {
        columns.push(Column::random(data_type, num_rows, None));
    }

    let block = DataBlock::new_from_columns(columns);
    block.check_valid().unwrap();

    block
}

#[derive(Debug, Clone, Copy)]
pub enum DataTypeFilter {
    Simple,
    Legacy,
    All,
}

fn get_all_test_data_types(filter: DataTypeFilter) -> Vec<DataType> {
    let mut result = Vec::new();

    let basic_types = [
        DataType::Boolean,
        DataType::String,
        DataType::Timestamp,
        DataType::Date,
    ];

    let zero_size = [DataType::Null, DataType::EmptyArray, DataType::EmptyMap];

    let binary_types = [DataType::Binary, DataType::Bitmap];

    let numbers = [
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
    ];

    let opaque_types = [
        DataType::Opaque(1),
        DataType::Opaque(2),
        DataType::Opaque(3),
    ];

    let decimals = [
        DataType::Decimal(DecimalSize::new_unchecked(10, 2)),
        DataType::Decimal(DecimalSize::new_unchecked(35, 3)),
    ];

    let nullable_types = [
        DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt32))),
        DataType::Nullable(Box::new(DataType::String)),
    ];

    let complex_types = [
        DataType::Array(Box::new(DataType::Number(NumberDataType::UInt32))),
        DataType::Map(Box::new(DataType::Tuple(vec![
            DataType::Number(NumberDataType::UInt64),
            DataType::String,
        ]))),
    ];

    match filter {
        DataTypeFilter::Legacy => {
            result.extend(zero_size);
            result.extend(basic_types);
            result.extend(numbers);
            result.extend(decimals);
            result.extend(binary_types);
            result.extend([DataType::Variant]);
            result.extend(nullable_types);
            result.extend(complex_types);
        }
        DataTypeFilter::All => {
            result.extend(zero_size);
            result.extend(basic_types);
            result.extend(numbers);
            result.extend(decimals);
            result.extend(binary_types);
            result.extend([DataType::Variant]);
            result.extend(nullable_types);
            result.extend(complex_types);
            result.extend(opaque_types);
            result.extend([DataType::Geometry, DataType::Interval, DataType::Geography]);
            // Tuple(Vec<DataType>),
            // Vector(VectorDataType),
        }
        DataTypeFilter::Simple => {
            result.extend([DataType::Null]);
            result.extend(basic_types);
            result.extend(numbers);
            result.extend(decimals);
            result.extend(nullable_types);
        }
    }

    result
}
