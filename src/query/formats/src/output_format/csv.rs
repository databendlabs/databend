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
    use databend_common_expression::Column;
    use databend_common_expression::FromData;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::types::BinaryType;
    use databend_common_expression::types::BooleanType;
    use databend_common_expression::types::DateType;
    use databend_common_expression::types::DecimalDataType;
    use databend_common_expression::types::DecimalSize;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::decimal::Decimal128Type;
    use databend_common_expression::types::number::Float64Type;
    use databend_common_expression::types::number::Int32Type;
    use databend_common_meta_app::principal::BinaryFormat;
    use databend_common_meta_app::principal::CsvFileFormatParams;
    use databend_common_meta_app::principal::CsvQuoteStyle;
    use databend_common_meta_app::principal::FileFormatParams;
    use databend_common_meta_app::tenant::Tenant;
    use databend_common_settings::Settings;

    use crate::get_output_format;
    use crate::output_format::test_utils::gen_schema_and_block;

    #[test]
    fn test_csv_quote_styles_and_null_display() -> Result<()> {
        let size = DecimalSize::new_unchecked(20, 2);
        let cases = [
            (
                TableDataType::Number(NumberDataType::Int32),
                Int32Type::from_data(vec![123]),
                "123",
                true,
            ),
            (
                TableDataType::Number(NumberDataType::Float64),
                Float64Type::from_data(vec![1.5f64]),
                "1.5",
                true,
            ),
            (
                TableDataType::Decimal(DecimalDataType::Decimal128(size)),
                Decimal128Type::from_data_with_size(vec![120i128], Some(size)),
                "1.2",
                true,
            ),
            (
                TableDataType::Boolean,
                BooleanType::from_data(vec![true]),
                "true",
                false,
            ),
            (
                TableDataType::Date,
                DateType::from_data(vec![0]),
                "1970-01-01",
                false,
            ),
            (
                TableDataType::Binary,
                BinaryType::from_data(vec![b"\xff".as_slice()]),
                "FF",
                false,
            ),
            (
                TableDataType::String,
                StringType::from_data(vec!["123"]),
                "123",
                false,
            ),
            (
                TableDataType::EmptyArray,
                Column::EmptyArray { len: 1 },
                "[]",
                false,
            ),
            (
                TableDataType::EmptyMap,
                Column::EmptyMap { len: 1 },
                "{}",
                false,
            ),
        ];
        let settings = Settings::create(Tenant::new_literal("default"));
        for (data_type, column, rendered, is_numeric) in cases {
            let (schema, block) = gen_schema_and_block(
                vec![
                    TableField::new("value", data_type),
                    TableField::new("null", TableDataType::Null),
                ],
                vec![column, Column::Null { len: 1 }],
            );
            for quote_style in [
                CsvQuoteStyle::QuoteNotNull,
                CsvQuoteStyle::QuoteNonNumeric,
                CsvQuoteStyle::QuoteMinimal,
            ] {
                for null_display in ["\\N", rendered] {
                    let params = CsvFileFormatParams {
                        quote_style,
                        null_display: null_display.to_string(),
                        ..Default::default()
                    };
                    let mut output = get_output_format(
                        schema.clone(),
                        FileFormatParams::Csv(params),
                        settings.get_output_format_settings()?,
                    )?;
                    let quoted = quote_style == CsvQuoteStyle::QuoteNotNull
                        || (quote_style == CsvQuoteStyle::QuoteNonNumeric && !is_numeric)
                        || rendered == null_display;
                    let value = if quoted {
                        format!("\"{rendered}\"")
                    } else {
                        rendered.to_string()
                    };
                    assert_eq!(
                        output.serialize_block(&block)?,
                        format!("{value},{null_display}\n").as_bytes(),
                        "{quote_style}, value={rendered}, null_display={null_display}"
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_csv_numeric_custom_delimiters() -> Result<()> {
        let (schema, block) = gen_schema_and_block(
            vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::Float64).wrap_nullable(),
            )],
            vec![Float64Type::from_opt_data(vec![Some(1.5f64), None])],
        );
        let settings = Settings::create(Tenant::new_literal("default"));
        for quote_style in [
            CsvQuoteStyle::QuoteNotNull,
            CsvQuoteStyle::QuoteNonNumeric,
            CsvQuoteStyle::QuoteMinimal,
        ] {
            let params = CsvFileFormatParams {
                quote_style,
                quote: "`".to_string(),
                field_delimiter: ".".to_string(),
                record_delimiter: ";".to_string(),
                ..Default::default()
            };
            let mut output = get_output_format(
                schema.clone(),
                FileFormatParams::Csv(params),
                settings.get_output_format_settings()?,
            )?;
            assert_eq!(output.serialize_block(&block)?, b"`1.5`;\\N;");
        }
        Ok(())
    }

    #[test]
    fn test_csv_output_empty_null_display() -> Result<()> {
        let (schema, block) = gen_schema_and_block(
            vec![
                TableField::new("string", TableDataType::String.wrap_nullable()),
                TableField::new("binary", TableDataType::Binary.wrap_nullable()),
            ],
            vec![
                StringType::from_opt_data(vec![Some(""), None]),
                BinaryType::from_opt_data(vec![Some(b"".as_slice()), None]),
            ],
        );
        let settings = Settings::create(Tenant::new_literal("default"));
        for quote_style in [
            CsvQuoteStyle::QuoteNotNull,
            CsvQuoteStyle::QuoteNonNumeric,
            CsvQuoteStyle::QuoteMinimal,
        ] {
            let params = CsvFileFormatParams {
                quote_style,
                null_display: String::new(),
                ..Default::default()
            };
            let mut output = get_output_format(
                schema.clone(),
                FileFormatParams::Csv(params),
                settings.get_output_format_settings()?,
            )?;
            assert_eq!(output.serialize_block(&block)?, b"\"\",\"\"\n,\n");
        }
        Ok(())
    }

    #[test]
    fn test_csv_output_empty_values() -> Result<()> {
        let (schema, block) = gen_schema_and_block(
            vec![
                TableField::new("binary", TableDataType::Binary.wrap_nullable()),
                TableField::new("string", TableDataType::String.wrap_nullable()),
                TableField::new("binary_not_null", TableDataType::Binary),
                TableField::new("string_not_null", TableDataType::String),
            ],
            vec![
                BinaryType::from_opt_data(vec![
                    Some(b"".as_slice()),
                    None,
                    Some(b"\xff".as_slice()),
                ]),
                StringType::from_opt_data(vec![Some(""), None, Some("x")]),
                BinaryType::from_data(vec![b"".as_slice(); 3]),
                StringType::from_data(vec![""; 3]),
            ],
        );
        let settings = Settings::create(Tenant::new_literal("default"));
        for (binary_format, binary_value) in
            [(BinaryFormat::Hex, "FF"), (BinaryFormat::Base64, "/w==")]
        {
            for quote_style in [
                CsvQuoteStyle::QuoteNotNull,
                CsvQuoteStyle::QuoteNonNumeric,
                CsvQuoteStyle::QuoteMinimal,
            ] {
                for quote in ['"', '\'', '`'] {
                    let params = CsvFileFormatParams {
                        binary_format,
                        quote_style,
                        quote: quote.to_string(),
                        ..Default::default()
                    };
                    let mut output = get_output_format(
                        schema.clone(),
                        FileFormatParams::Csv(params),
                        settings.get_output_format_settings()?,
                    )?;
                    let empty = format!("{quote}{quote}");
                    let string_value = if quote_style.is_quote_minimal() {
                        "x".to_string()
                    } else {
                        format!("{quote}x{quote}")
                    };
                    let binary_value = if quote_style.is_quote_minimal() {
                        binary_value.to_string()
                    } else {
                        format!("{quote}{binary_value}{quote}")
                    };
                    let expected = format!(
                        "{empty},{empty},{empty},{empty}\n\\N,\\N,{empty},{empty}\n{binary_value},{string_value},{empty},{empty}\n"
                    );
                    assert_eq!(output.serialize_block(&block)?, expected.as_bytes());
                }
            }
        }
        Ok(())
    }

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
                    "\"\\N\"\n",
                    "\"\"\n",
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
