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
use common_datavalues::serializations::write_escaped_string;
use common_datavalues::serializations::write_json_string;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::TypeSerializer;
use common_exception::Result;
use common_io::prelude::FormatSettings;

use crate::formats::output_format::OutputFormat;
pub type JsonEachRowOutputFormat = JsonEachRowOutputFormatBase<false, false, false, false>;
pub type JsonStringsEachRowOutputFormat = JsonEachRowOutputFormatBase<true, false, false, false>;
pub type JsonCompactEachRowOutputFormat = JsonEachRowOutputFormatBase<false, true, false, false>;
pub type JsonCompactStringsEachRowOutputFormat =
    JsonEachRowOutputFormatBase<true, true, false, false>;

pub type JsonCompactEachRowWithNamesOutputFormat =
    JsonEachRowOutputFormatBase<false, true, true, false>;
pub type JsonCompactEachRowWithNamesAndTypesOutputFormat =
    JsonEachRowOutputFormatBase<false, true, true, true>;
pub type JsonCompactStringsEachRowWithNamesOutputFormat =
    JsonEachRowOutputFormatBase<true, true, true, false>;
pub type JsonCompactStringsEachRowWithNamesAndTypesOutputFormat =
    JsonEachRowOutputFormatBase<true, true, true, true>;

#[derive(Default)]
pub struct JsonEachRowOutputFormatBase<
    const STRINGS: bool,
    const COMPACT: bool,
    const WITH_NAMES: bool,
    const WITH_TYPES: bool,
> {
    schema: DataSchemaRef,
    format_settings: FormatSettings,
}

impl<const STRINGS: bool, const COMPACT: bool, const WITH_NAMES: bool, const WITH_TYPES: bool>
    JsonEachRowOutputFormatBase<STRINGS, COMPACT, WITH_NAMES, WITH_TYPES>
{
    pub fn create(schema: DataSchemaRef, format_settings: FormatSettings) -> Self {
        let format_settings = if STRINGS {
            format_settings
        } else if format_settings.json_quote_denormals {
            FormatSettings {
                null_bytes: vec![b'n', b'u', b'l', b'l'],
                inf_bytes: vec![b'\"', b'i', b'n', b'f', b'\"'],
                nan_bytes: vec![b'\"', b'n', b'a', b'n', b'\"'],
                ..format_settings
            }
        } else {
            FormatSettings {
                null_bytes: vec![b'n', b'u', b'l', b'l'],
                inf_bytes: vec![b'n', b'u', b'l', b'l'],
                nan_bytes: vec![b'n', b'u', b'l', b'l'],
                ..format_settings
            }
        };

        Self {
            schema,
            format_settings,
        }
    }

    fn serialize_strings(&self, values: Vec<String>, format: &FormatSettings) -> Vec<u8> {
        assert!(COMPACT);
        let mut buf = vec![b'['];
        for (col_index, v) in values.iter().enumerate() {
            if col_index != 0 {
                buf.push(b',');
            }
            buf.push(b'"');
            if STRINGS {
                write_escaped_string(v.as_bytes(), &mut buf, b'\"');
            } else {
                write_json_string(v.as_bytes(), &mut buf, format);
            }
            buf.push(b'"');
        }
        buf.extend_from_slice(b"]\n");
        buf
    }
}

impl<const STRINGS: bool, const COMPACT: bool, const WITH_NAMES: bool, const WITH_TYPES: bool>
    OutputFormat for JsonEachRowOutputFormatBase<STRINGS, COMPACT, WITH_NAMES, WITH_TYPES>
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
                    serializer.write_field_escaped(
                        row_index,
                        &mut buf,
                        &self.format_settings,
                        b'\"',
                    );
                    buf.push(b'"');
                } else {
                    serializer.write_field_json(row_index, &mut buf, &self.format_settings);
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
        let format_settings = &self.format_settings;

        let mut buf = vec![];
        if WITH_NAMES {
            assert!(COMPACT);
            let names = self
                .schema
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect::<Vec<_>>();
            buf.extend_from_slice(&self.serialize_strings(names, format_settings));
            if WITH_TYPES {
                let types = self
                    .schema
                    .fields()
                    .iter()
                    .map(|f| f.data_type().name())
                    .collect::<Vec<_>>();
                buf.extend_from_slice(&self.serialize_strings(types, format_settings));
            }
        }
        Ok(buf)
    }

    fn finalize(&mut self) -> Result<Vec<u8>> {
        Ok(vec![])
    }
}
