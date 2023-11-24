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

use common_arrow::arrow::bitmap::Bitmap;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::number::Float64Type;
use common_expression::types::number::Int32Type;
use common_expression::types::BooleanType;
use common_expression::types::DateType;
use common_expression::types::NumberDataType;
use common_expression::types::StringType;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::FromData;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRef;
use common_expression::TableSchemaRefExt;

pub fn gen_schema_and_block(
    fields: Vec<TableField>,
    columns: Vec<Column>,
) -> (TableSchemaRef, DataBlock) {
    assert!(!columns.is_empty() && columns.len() == fields.len());
    let block = DataBlock::new_from_columns(columns);
    (TableSchemaRefExt::create(fields), block)
}

pub fn get_simple_block(is_nullable: bool) -> (TableSchemaRef, DataBlock) {
    let columns = vec![
        (
            TableDataType::Number(NumberDataType::Int32),
            Int32Type::from_data(vec![1i32, 2, 3]),
        ),
        (
            TableDataType::String,
            StringType::from_data(vec!["a", "b\"", "c'"]),
        ),
        (
            TableDataType::Boolean,
            BooleanType::from_data(vec![true, true, false]),
        ),
        (
            TableDataType::Number(NumberDataType::Float64),
            Float64Type::from_data(vec![1.1f64, 2.2, f64::NAN]),
        ),
        (
            TableDataType::Date,
            DateType::from_data(vec![1_i32, 2_i32, 3_i32]),
        ),
    ];

    let (columns, fields) = if !is_nullable {
        columns
            .into_iter()
            .enumerate()
            .map(|(idx, (data_type, c))| (c, TableField::new(&format!("c{}", idx + 1), data_type)))
            .unzip::<_, _, Vec<_>, Vec<_>>()
    } else {
        columns
            .into_iter()
            .enumerate()
            .map(|(idx, (data_type, c))| {
                let validity = Bitmap::new_constant(true, c.len());
                (
                    Column::Nullable(Box::new(NullableColumn {
                        column: c,
                        validity,
                    })),
                    TableField::new(&format!("c{}", idx + 1), data_type.wrap_nullable()),
                )
            })
            .unzip::<_, _, Vec<_>, Vec<_>>()
    };

    gen_schema_and_block(fields, columns)
}
