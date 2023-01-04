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

use common_expression::DataBlock;
use common_expression::TableSchemaRef;
use common_io::prelude::FormatSettings;
use serde_json::Value as JsonValue;

use crate::field_encoder::FieldEncoderJSON;
use crate::output_format::OutputFormat;
use crate::FileFormatOptionsExt;

pub struct JSONOutputFormat {
    schema: TableSchemaRef,
    field_encoder: FieldEncoderJSON,
    first_block: bool,
    first_row: bool,
    format_settings: FormatSettings,
}

impl JSONOutputFormat {
    pub fn create(schema: TableSchemaRef, options: &FileFormatOptionsExt) -> Self {
        let field_encoder = FieldEncoderJSON::create(options);
        Self {
            schema,
            field_encoder,
            first_block: true,
            first_row: true,
            format_settings: FormatSettings {
                timezone: options.timezone,
            },
        }
    }

    fn format_schema(&self) -> common_exception::Result<Vec<u8>> {
        let fields = self.schema.fields();
        if fields.is_empty() {
            return Ok(b"\"meta\":[]".to_vec());
        }
        let mut res = b"\"meta\":[".to_vec();
        for field in fields {
            res.push(b'{');
            res.extend_from_slice(b"\"name\":\"");
            res.extend_from_slice(field.name().as_bytes());
            res.extend_from_slice(b"\",\"type\":\"");
            res.extend_from_slice(field.data_type().name().as_bytes());
            res.extend_from_slice(b"\"}");
            res.push(b',');
        }
        res.pop();
        res.extend_from_slice(b"]");
        Ok(res)
    }
}

fn transpose(col_table: Vec<Vec<JsonValue>>) -> Vec<Vec<JsonValue>> {
    if col_table.is_empty() {
        return vec![];
    }
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

impl OutputFormat for JSONOutputFormat {
    fn serialize_block(&mut self, _data_block: &DataBlock) -> common_exception::Result<Vec<u8>> {
        let _res = if self.first_block {
            self.first_block = false;
            let mut buf = b"{".to_vec();
            buf.extend_from_slice(self.format_schema()?.as_ref());
            buf.extend_from_slice(b",\"data\":[".as_ref());
            buf
        } else {
            vec![]
        };

        todo!("expression");
        // let mut cols: Vec<Vec<JsonValue>> = vec![];
        // let serializers = data_block.get_serializers()?;
        // for s in serializers {
        //     cols.push(s.serialize_json_values(&self.format_settings)?)
        // }

        // let rows = transpose(cols);
        // let n_col = self.schema.fields().len();
        // let names = self
        //     .schema
        //     .fields()
        //     .iter()
        //     .map(|f| f.name().to_string())
        //     .collect::<Vec<String>>();
        // let columns: Vec<Column> = data_block
        //     .convert_to_full()
        //     .columns()
        //     .iter()
        //     .map(|column| column.value.clone().into_column().unwrap())
        //     .collect();

        // for r in &rows {
        //     if self.first_row {
        //         self.first_row = false;
        //     } else {
        //         res.push(b',');
        //     }
        //     res.push(b'{');
        //     for c in 0..n_col {
        //         res.push(b'\"');
        //         res.extend_from_slice(names[c].as_bytes());
        //         res.push(b'\"');

        //         res.push(b':');

        //         res.extend_from_slice(r[c].to_string().as_bytes());

        //         if c != n_col - 1 {
        //             res.push(b',');
        //         }
        //     }
        //     res.push(b'}');
        // }
        // Ok(res)
    }

    fn finalize(&mut self) -> common_exception::Result<Vec<u8>> {
        if self.first_row {
            let mut buf = b"{".to_vec();
            buf.extend_from_slice(self.format_schema()?.as_ref());
            buf.extend_from_slice(b",\"data\":[]}\n".as_ref());
            Ok(buf)
        } else {
            Ok(b"]}\n".to_vec())
        }
    }
}
