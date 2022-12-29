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
use common_expression::from_date_data;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::NumberDataType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::ColumnFrom;
use common_expression::DataBlock;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRef;
use common_expression::TableSchemaRefExt;
use common_expression::Value;

pub fn gen_schema_and_block(
    fields: Vec<TableField>,
    columns: Vec<Column>,
) -> (TableSchemaRef, DataBlock) {
    assert!(!columns.is_empty() && columns.len() == fields.len());

    let num_rows = columns[0].len();
    let block_entries = fields
        .iter()
        .zip(columns.into_iter())
        .map(|(f, c)| BlockEntry {
            data_type: f.data_type().into(),
            value: Value::Column(c),
        })
        .collect();

    let block = DataBlock::new(block_entries, num_rows);
    (TableSchemaRefExt::create(fields), block)
}

pub fn get_simple_block(is_nullable: bool) -> (TableSchemaRef, DataBlock) {
    let columns = vec![
        (
            TableDataType::Number(NumberDataType::Int32),
            Column::from_data(vec![1i32, 2, 3]),
        ),
        (
            TableDataType::String,
            Column::from_data(vec!["a", "b\"", "c'"]),
        ),
        (
            TableDataType::Boolean,
            Column::from_data(vec![true, true, false]),
        ),
        (
            TableDataType::Number(NumberDataType::Float64),
            Column::from_data(vec![1.1f64, 2.2, f64::NAN]),
        ),
        (
            TableDataType::Date,
            from_date_data(vec![1_i32, 2_i32, 3_i32]),
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
                let mut validity = MutableBitmap::new();
                validity.extend_constant(c.len(), true);
                (
                    Column::Nullable(Box::new(NullableColumn {
                        column: c,
                        validity: validity.into(),
                    })),
                    TableField::new(&format!("c{}", idx + 1), data_type.wrap_nullable()),
                )
            })
            .unzip::<_, _, Vec<_>, Vec<_>>()
    };

    gen_schema_and_block(fields, columns)
}
