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
use databend_common_meta_app::principal::CsvFileFormatParams;

use crate::field_encoder::CsvQuoteSettings;
use crate::field_encoder::FieldEncoderCSV;
use crate::field_encoder::write_csv_string_maybe_quoted;
use crate::output_format::OutputFormat;

pub struct CSVOutputFormat {
    schema: TableSchemaRef,
    field_encoder: FieldEncoderCSV,
    field_delimiter: u8,
    record_delimiter: Vec<u8>,
    quote_settings: CsvQuoteSettings,

    headers: u8,
}

impl CSVOutputFormat {
    pub fn create(
        schema: TableSchemaRef,
        params: &CsvFileFormatParams,
        field_encoder: FieldEncoderCSV,
        headers: u8,
    ) -> Self {
        Self {
            schema,
            field_encoder,
            field_delimiter: params.field_delimiter.as_bytes()[0],
            record_delimiter: params.record_delimiter.as_bytes().to_vec(),
            quote_settings: CsvQuoteSettings::from_params(params),
            headers,
        }
    }

    fn serialize_strings(&self, values: Vec<String>) -> Vec<u8> {
        let mut buf = vec![];
        let fd = self.field_delimiter;

        for (col_index, v) in values.iter().enumerate() {
            if col_index != 0 {
                buf.push(fd);
            }
            write_csv_string_maybe_quoted(v.as_bytes(), &mut buf, &self.quote_settings);
        }

        buf.extend_from_slice(&self.record_delimiter);
        buf
    }
}

impl OutputFormat for CSVOutputFormat {
    fn serialize_block(&mut self, block: &DataBlock) -> Result<Vec<u8>> {
        let rows_size = block.num_rows();
        let mut buf = Vec::with_capacity(block.estimate_block_size(block.num_columns()));

        let fd = self.field_delimiter;
        let rd = &self.record_delimiter;

        let columns: Vec<Column> = block
            .convert_to_full()
            .columns()
            .iter()
            .map(|val| val.as_column().unwrap().clone().decode_owned())
            .collect();

        for row_index in 0..rows_size {
            for (col_index, column) in columns.iter().enumerate() {
                if col_index != 0 {
                    buf.push(fd);
                }
                self.field_encoder
                    .write_field(column, row_index, &mut buf)?;
            }
            buf.extend_from_slice(rd)
        }
        Ok(buf)
    }

    fn serialize_prefix(&self) -> Result<Vec<u8>> {
        let mut buf = vec![];
        if self.headers > 0 {
            let names = self
                .schema
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect::<Vec<_>>();
            buf.extend_from_slice(&self.serialize_strings(names));
            if self.headers > 1 {
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

#[cfg(test)]
mod tests {
    use databend_common_exception::Result;
    use databend_common_expression::FromData;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::types::StringType;
    use databend_common_meta_app::principal::CsvFileFormatParams;
    use databend_common_meta_app::principal::CsvQuoteStyle;
    use databend_common_meta_app::principal::FileFormatParams;
    use databend_common_meta_app::tenant::Tenant;
    use databend_common_settings::Settings;

    use crate::get_output_format;
    use crate::output_format::test_utils::gen_schema_and_block;

    #[test]
    fn test_csv_output_ignores_escape() -> Result<()> {
        let (schema, block) = gen_schema_and_block(
            vec![TableField::new(r"path\name", TableDataType::String)],
            vec![StringType::from_data(vec![
                r"a\tb", r"a\b", "tail\\", "a,b\\", "a\"b\\", "a\nb\\", r"\N", "",
            ])],
        );
        let settings = Settings::create(Tenant::new_literal("default"));
        let cases = [
            (
                CsvQuoteStyle::QuoteMinimal,
                "path\\name\n",
                concat!(
                    "a\\tb\n",
                    "a\\b\n",
                    "tail\\\n",
                    "\"a,b\\\"\n",
                    "\"a\"\"b\\\"\n",
                    "\"a\nb\\\"\n",
                    "\\N\n",
                    "\n",
                ),
            ),
            (
                CsvQuoteStyle::QuoteNotNull,
                "\"path\\name\"\n",
                concat!(
                    "\"a\\tb\"\n",
                    "\"a\\b\"\n",
                    "\"tail\\\"\n",
                    "\"a,b\\\"\n",
                    "\"a\"\"b\\\"\n",
                    "\"a\nb\\\"\n",
                    "\"\\N\"\n",
                    "\"\"\n",
                ),
            ),
        ];
        for (quote_style, expected_header, expected_data) in cases {
            for escape in ["", "\\"] {
                let params = CsvFileFormatParams {
                    escape: escape.to_string(),
                    quote_style,
                    output_header: true,
                    ..Default::default()
                };
                let mut output = get_output_format(
                    schema.clone(),
                    FileFormatParams::Csv(params),
                    settings.get_output_format_settings()?,
                )?;
                assert_eq!(output.serialize_prefix()?, expected_header.as_bytes());
                assert_eq!(output.serialize_block(&block)?, expected_data.as_bytes());
            }
        }
        Ok(())
    }
}
