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

#[derive(Debug, Clone, Default)]
pub struct StringBlock {
    pub(crate) data: Vec<Vec<Option<String>>>,
}

pub type StringBlockRef = Arc<StringBlock>;

fn data_is_null(column: &Column, row_index: usize) -> bool {
    match column {
        Column::Null { .. } => true,
        Column::Nullable(box inner) => !inner.validity.get_bit(row_index),
        _ => false,
    }
}

pub fn block_to_strings(
    block: &DataBlock,
    format: &FormatSettings,
) -> Result<Vec<Vec<Option<String>>>> {
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
        let mut row: Vec<Option<String>> = Vec::with_capacity(block.num_columns());
        for column in &columns {
            if !format.format_null_as_str && data_is_null(column, row_index) {
                row.push(None);
                continue;
            }
            buf.clear();
            encoder.write_field(column, row_index, &mut buf, false);
            row.push(Some(String::from_utf8_lossy(&buf).into_owned()));
        }
        res.push(row)
    }
    Ok(res)
}

impl StringBlock {
    pub fn empty() -> Self {
        Self { data: vec![] }
    }

    pub fn new(block: &DataBlock, format: &FormatSettings) -> Result<Self> {
        Ok(StringBlock {
            data: block_to_strings(block, format)?,
        })
    }

    pub fn concat(blocks: Vec<StringBlock>) -> Self {
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

    pub fn as_data<'n>(&'n self, null_as: &'n str) -> Vec<Vec<&'n str>> {
        self.data
            .iter()
            .map(|row| {
                row.iter()
                    .map(|v| match v {
                        Some(v) => v,
                        None => null_as,
                    })
                    .collect()
            })
            .collect()
    }
}

impl From<StringBlock> for Vec<Vec<Option<String>>> {
    fn from(block: StringBlock) -> Self {
        block.data
    }
}
