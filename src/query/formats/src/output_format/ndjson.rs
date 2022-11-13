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

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_exception::Result;

use crate::field_encoder::FieldEncoderJSON;
use crate::field_encoder::FieldEncoderRowBased;
use crate::output_format::OutputFormat;
use crate::FileFormatOptionsExt;

pub struct NDJSONOutputFormatBase<
    const STRINGS: bool,
    const COMPACT: bool,
    const WITH_NAMES: bool,
    const WITH_TYPES: bool,
> {
    schema: DataSchemaRef,
    field_encoder: FieldEncoderJSON,
}

impl<const STRINGS: bool, const COMPACT: bool, const WITH_NAMES: bool, const WITH_TYPES: bool>
    NDJSONOutputFormatBase<STRINGS, COMPACT, WITH_NAMES, WITH_TYPES>
{
    pub fn create(schema: DataSchemaRef, options: &FileFormatOptionsExt) -> Self {
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
            self.field_encoder
                .write_string_inner(v.as_bytes(), &mut buf, false);
        }
        buf.extend_from_slice(b"]\n");
        buf
    }
}

impl<const STRINGS: bool, const COMPACT: bool, const WITH_NAMES: bool, const WITH_TYPES: bool>
    OutputFormat for NDJSONOutputFormatBase<STRINGS, COMPACT, WITH_NAMES, WITH_TYPES>
{
    fn serialize_block(&mut self, block: &DataBlock) -> Result<Vec<u8>> {
        let rows_size = block.column(0).len();

        let mut buf = Vec::with_capacity(block.memory_size());
        let serializers = block.get_serializers()?;
        let field_names: Vec<_> = block
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_bytes())
            .collect();

        for row_index in 0..rows_size {
            if COMPACT {
                buf.push(b'[');
            } else {
                buf.push(b'{');
            }
            for (col_index, serializer) in serializers.iter().enumerate() {
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
                    buf.push(b'"');
                    self.field_encoder
                        .write_field(serializer, row_index, &mut buf, true);
                    buf.push(b'"');
                } else {
                    self.field_encoder
                        .write_field(serializer, row_index, &mut buf, false)
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
                    .map(|f| f.data_type().name())
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
