// Copyright 2021 Datafuse Labs
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

use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_formats::field_encoder::FieldEncoderValues;
use databend_common_io::prelude::FormatSettings;
use serde_json::Value as JsonValue;

#[derive(Debug, Clone)]
pub struct JsonBlock {
    pub(crate) data: Vec<Vec<JsonValue>>,
}

pub type JsonBlockRef = Arc<JsonBlock>;

pub fn block_to_json_value(
    block: &DataBlock,
    format: &FormatSettings,
) -> Result<Vec<Vec<JsonValue>>> {
    if block.is_empty() {
        return Ok(vec![]);
    }
    let rows_size = block.num_rows();
    let columns: Vec<Column> = block
        .convert_to_full()
        .columns()
        .iter()
        .map(|column| column.value.clone().into_column().unwrap())
        .collect();

    let mut res = Vec::new();
    let encoder =
        FieldEncoderValues::create_for_http_handler(format.timezone, format.geometry_format);
    let mut buf = vec![];
    for row_index in 0..rows_size {
        let mut row: Vec<JsonValue> = Vec::with_capacity(block.num_columns());
        for column in &columns {
            buf.clear();
            encoder.write_field(column, row_index, &mut buf, false);
            row.push(serde_json::to_value(String::from_utf8_lossy(&buf))?);
        }
        res.push(row)
    }
    Ok(res)
}

impl JsonBlock {
    pub fn empty() -> Self {
        Self { data: vec![] }
    }

    pub fn new(block: &DataBlock, format: &FormatSettings) -> Result<Self> {
        Ok(JsonBlock {
            data: block_to_json_value(block, format)?,
        })
    }

    pub fn concat(blocks: Vec<JsonBlock>) -> Self {
        if blocks.is_empty() {
            return Self::empty();
        }
        let results = blocks.into_iter().map(|b| b.data).collect::<Vec<_>>();
        let data = results.concat();
        Self { data }
    }

    pub fn num_rows(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn data(&self) -> &Vec<Vec<JsonValue>> {
        &self.data
    }
}

impl From<JsonBlock> for Vec<Vec<JsonValue>> {
    fn from(block: JsonBlock) -> Self {
        block.data
    }
}
