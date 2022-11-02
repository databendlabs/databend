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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::serializations::write_escaped_string;
use common_expression::serializations::write_csv_string;
use common_expression::Chunk;
use common_expression::DataSchemaRef;
use common_expression::TypeSerializer;
use common_io::prelude::FormatSettings;

use crate::output_format::OutputFormat;

const FIELD_DELIMITER: u8 = b'\t';
const ROW_DELIMITER: u8 = b'\n';

pub type TSVOutputFormat = TCSVOutputFormat<true, false, false>;
pub type CSVOutputFormat = TCSVOutputFormat<false, false, false>;
pub type TSVWithNamesOutputFormat = TCSVOutputFormat<true, true, false>;
pub type CSVWithNamesOutputFormat = TCSVOutputFormat<false, true, false>;
pub type TSVWithNamesAndTypesOutputFormat = TCSVOutputFormat<true, true, true>;
pub type CSVWithNamesAndTypesOutputFormat = TCSVOutputFormat<false, true, true>;

#[derive(Default)]
pub struct TCSVOutputFormat<const TSV: bool, const WITH_NAMES: bool, const WITH_TYPES: bool> {
    schema: DataSchemaRef,
    format_settings: FormatSettings,
}

impl<const TSV: bool, const WITH_NAMES: bool, const WITH_TYPES: bool>
    TCSVOutputFormat<TSV, WITH_NAMES, WITH_TYPES>
{
    pub fn create(schema: DataSchemaRef, format_settings: FormatSettings) -> Self {
        Self {
            schema,
            format_settings,
        }
    }

    fn serialize_strings(&self, values: Vec<String>, format: &FormatSettings) -> Vec<u8> {
        let mut buf = vec![];
        let fd = format.field_delimiter[0];

        for (col_index, v) in values.iter().enumerate() {
            if col_index != 0 {
                buf.push(fd);
            }
            if TSV {
                write_escaped_string(v.as_bytes(), &mut buf, b'\'');
            } else {
                write_csv_string(v.as_bytes(), &mut buf, b'\"');
            };
        }

        buf.push(format.record_delimiter[0]);
        buf
    }
}

impl<const TSV: bool, const WITH_NAMES: bool, const WITH_TYPES: bool> OutputFormat
    for TCSVOutputFormat<TSV, WITH_NAMES, WITH_TYPES>
{
    fn serialize_block(&mut self, chunk: &Chunk) -> Result<Vec<u8>> {
        let rows_size = chunk.num_rows();
        let format_settings = &self.format_settings;

        let mut buf = Vec::with_capacity(chunk.memory_size());
        let serializers = chunk
            .get_serializers()
            .map_err(|err| ErrorCode::UnknownColumn(err))?;

        let fd = if TSV {
            FIELD_DELIMITER
        } else {
            format_settings.field_delimiter[0]
        };

        let rd = if TSV {
            ROW_DELIMITER
        } else {
            format_settings.record_delimiter[0]
        };

        for row_index in 0..rows_size {
            for (col_index, serializer) in serializers.iter().enumerate() {
                if col_index != 0 {
                    buf.push(fd);
                }
                if TSV {
                    serializer.write_field_tsv(row_index, &mut buf, &self.format_settings);
                } else {
                    serializer.write_field_csv(row_index, &mut buf, &self.format_settings)
                };
            }
            buf.push(rd)
        }
        Ok(buf)
    }

    fn serialize_prefix(&self) -> Result<Vec<u8>> {
        let format_settings = &self.format_settings;

        let mut buf = vec![];
        if WITH_NAMES {
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
                    .map(|f| f.data_type().to_string())
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
