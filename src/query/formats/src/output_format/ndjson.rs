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

use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;

use crate::field_encoder::FieldEncoderJSON;
use crate::output_format::OutputFormat;
use crate::FileFormatOptionsExt;

pub struct NDJSONOutputFormatBase<
    const STRINGS: bool,
    const COMPACT: bool,
    const WITH_NAMES: bool,
    const WITH_TYPES: bool,
> {
    schema: TableSchemaRef,
    field_encoder: FieldEncoderJSON,
}

impl<const STRINGS: bool, const COMPACT: bool, const WITH_NAMES: bool, const WITH_TYPES: bool>
    NDJSONOutputFormatBase<STRINGS, COMPACT, WITH_NAMES, WITH_TYPES>
{
    pub fn create(schema: TableSchemaRef, options: &FileFormatOptionsExt) -> Self {
        let field_encoder = FieldEncoderJSON::create(options);
        Self {
            schema,
            field_encoder,
        }
    }

    fn serialize_strings(&self, values: Vec<String>) -> Vec<u8> {
        assert!(COMPACT);
        let mut buf = vec![b'['];
        for (col_index, v) in values.iter().enumerate() {
            if col_index != 0 {
                buf.push(b',');
            }
            self.field_encoder.write_string(v.as_bytes(), &mut buf);
        }
        buf.extend_from_slice(b"]\n");
        buf
    }
}

impl<const STRINGS: bool, const COMPACT: bool, const WITH_NAMES: bool, const WITH_TYPES: bool>
    OutputFormat for NDJSONOutputFormatBase<STRINGS, COMPACT, WITH_NAMES, WITH_TYPES>
{
    fn serialize_block(&mut self, block: &DataBlock) -> Result<Vec<u8>> {
        let rows_size = block.num_rows();

        let mut buf = Vec::with_capacity(block.memory_size());
        let field_names: Vec<_> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name().as_bytes())
            .collect();

        let columns: Vec<Column> = block
            .convert_to_full()
            .columns()
            .iter()
            .map(|column| column.value.clone().into_column().unwrap())
            .collect();

        for row_index in 0..rows_size {
            if COMPACT {
                buf.push(b'[');
            } else {
                buf.push(b'{');
            }
            for (col_index, column) in columns.iter().enumerate() {
                if col_index != 0 {
                    buf.push(b',');
                }
                if !COMPACT {
                    buf.push(b'"');
                    buf.extend_from_slice(field_names[col_index]);
                    buf.push(b'"');

                    buf.push(b':');
                }

                if STRINGS {
                    let mut tmp = vec![];
                    self.field_encoder.write_field(column, row_index, &mut tmp);
                    if !tmp.is_empty() && tmp[0] == b'\"' {
                        buf.extend_from_slice(&tmp);
                    } else {
                        buf.push(b'"');
                        buf.extend_from_slice(&tmp);
                        buf.push(b'"');
                    }
                } else {
                    self.field_encoder.write_field(column, row_index, &mut buf)
                }
            }
            if COMPACT {
                buf.extend_from_slice("]\n".as_bytes());
            } else {
                buf.extend_from_slice("}\n".as_bytes());
            }
        }
        Ok(buf)
    }

    fn serialize_prefix(&self) -> Result<Vec<u8>> {
        let mut buf = vec![];
        if WITH_NAMES {
            assert!(COMPACT);
            let names = self
                .schema
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect::<Vec<_>>();
            buf.extend_from_slice(&self.serialize_strings(names));
            if WITH_TYPES {
                let types = self
                    .schema
                    .fields()
                    .iter()
                    .map(|f| f.data_type().to_string())
                    .collect::<Vec<_>>();
                buf.extend_from_slice(&self.serialize_strings(types));
            }
        }
        Ok(buf)
    }

    fn finalize(&mut self) -> Result<Vec<u8>> {
        Ok(vec![])
    }
}
