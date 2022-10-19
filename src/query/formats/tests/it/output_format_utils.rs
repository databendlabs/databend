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

use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::Result;
use common_expression::from_date_data;
use common_expression::from_date_data_with_validity;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::Chunk;
use common_expression::Column;
use common_expression::ColumnFrom;
use common_expression::DataField;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;

pub fn get_simple_chunk(is_nullable: bool) -> Result<(DataSchemaRef, Chunk)> {
    let schema = match is_nullable {
        false => DataSchemaRefExt::create(vec![
            DataField::new("c1", DataType::Number(NumberDataType::Int32)),
            DataField::new("c2", DataType::String),
            DataField::new("c3", DataType::Boolean),
            DataField::new("c4", DataType::Number(NumberDataType::Float64)),
            DataField::new("c5", DataType::Date),
        ]),
        true => DataSchemaRefExt::create(vec![
            DataField::new(
                "c1",
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int32))),
            ),
            DataField::new("c2", DataType::Nullable(Box::new(DataType::String))),
            DataField::new("c3", DataType::Nullable(Box::new(DataType::Boolean))),
            DataField::new(
                "c4",
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Float64))),
            ),
            DataField::new("c5", DataType::Nullable(Box::new(DataType::Date))),
        ]),
    };

    let chunk = match is_nullable {
        false => Chunk::new(
            vec![
                (
                    Value::Column(Column::from_data(vec![1i32, 2, 3])),
                    DataType::Number(NumberDataType::Int32),
                ),
                (
                    Value::Column(Column::from_data(vec!["a", "b\"", "c'"])),
                    DataType::String,
                ),
                (
                    Value::Column(Column::from_data(vec![true, true, false])),
                    DataType::Boolean,
                ),
                (
                    Value::Column(Column::from_data(vec![1.1f64, 2.2, 3.3])),
                    DataType::Number(NumberDataType::Float64),
                ),
                (from_date_data(vec![1_i32, 2, 3]), DataType::Date),
            ],
            3,
        ),
        true => Chunk::new(
            vec![
                (
                    Value::Column(Column::from_data_with_validity(vec![1i32, 2, 3], vec![
                        true, true, true,
                    ])),
                    DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int32))),
                ),
                (
                    Value::Column(Column::from_data_with_validity(
                        vec!["a", "b\"", "c'"],
                        vec![true, true, true],
                    )),
                    DataType::Nullable(Box::new(DataType::String)),
                ),
                (
                    Value::Column(Column::from_data_with_validity(
                        vec![true, true, false],
                        vec![true, true, true],
                    )),
                    DataType::Nullable(Box::new(DataType::Boolean)),
                ),
                (
                    Value::Column(Column::from_data_with_validity(
                        vec![1.1f64, 2.2, 3.3],
                        vec![true, true, true],
                    )),
                    DataType::Nullable(Box::new(DataType::Number(NumberDataType::Float64))),
                ),
                (
                    from_date_data_with_validity(vec![1_i32, 2, 3], vec![true, true, true]),
                    DataType::Nullable(Box::new(DataType::Date)),
                ),
            ],
            3,
        ),
    };

    Ok((schema, chunk))
}
