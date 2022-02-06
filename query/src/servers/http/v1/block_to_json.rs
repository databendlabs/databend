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
use common_exception::Result;
use serde_json::Value as JsonValue;

pub(crate) type JsonBlock = Vec<Vec<JsonValue>>;
pub(crate) type JsonBlockRef = Arc<JsonBlock>;

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

pub fn block_to_json(block: &DataBlock) -> Result<Vec<Vec<JsonValue>>> {
    let mut col_table = Vec::new();
    let columns_size = block.columns().len();
    for col_index in 0..columns_size {
        let column = block.column(col_index);
        let column = column.convert_full_column();
        let field = block.schema().field(col_index);
        let data_type = field.data_type();
        let serializer = data_type.create_serializer();
        col_table.push(serializer.serialize_json(&column)?);
    }

    Ok(transpose(col_table))
}
