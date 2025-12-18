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

use crate::FileFormatOptionsExt;
use crate::field_encoder::FieldEncoderJSON;
use crate::output_format::OutputFormat;

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
            .map(|column| column.value().into_column().unwrap())
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

#[cfg(test)]
mod test {

    use databend_common_exception::Result;
    use databend_common_expression::FromData;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::number::Float32Type;
    use databend_common_expression::types::number::Int32Type;
    use pretty_assertions::assert_eq;

    use crate::output_format::utils::gen_schema_and_block;
    use crate::output_format::utils::get_output_format_clickhouse;
    use crate::output_format::utils::test_data_block;

    #[test]
    fn test_null() -> Result<()> {
        let (schema, block) = gen_schema_and_block(
            vec![
                TableField::new(
                    "c1",
                    TableDataType::Number(NumberDataType::Int32).wrap_nullable(),
                ),
                TableField::new(
                    "c2",
                    TableDataType::Number(NumberDataType::Int32).wrap_nullable(),
                ),
            ],
            vec![
                Int32Type::from_opt_data(vec![Some(1i32), None, Some(3)]),
                Int32Type::from_opt_data(vec![None, Some(2i32), None]),
            ],
        );

        {
            let mut formatter = get_output_format_clickhouse("ndjson", schema)?;
            let buffer = formatter.serialize_block(&block)?;

            let tsv_block = String::from_utf8(buffer)?;
            let expect = r#"{"c1":1,"c2":null}
{"c1":null,"c2":2}
{"c1":3,"c2":null}
"#;
            assert_eq!(&tsv_block, expect);
        }
        Ok(())
    }

    #[ignore]
    #[test]
    fn test_denormal() -> Result<()> {
        let (schema, block) = gen_schema_and_block(
            vec![
                TableField::new("c1", TableDataType::Number(NumberDataType::Float32)),
                TableField::new("c2", TableDataType::Number(NumberDataType::Float32)),
            ],
            vec![
                Float32Type::from_data(vec![1f32, f32::NAN]),
                Float32Type::from_data(vec![f32::INFINITY, f32::NEG_INFINITY]),
            ],
        );

        {
            let mut formatter = get_output_format_clickhouse("ndjson", schema)?;
            let buffer = formatter.serialize_block(&block)?;

            let tsv_block = String::from_utf8(buffer)?;
            let expect = r#"{"c1":1.0,"c2":null}
{"c1":null,"c2":null}
"#;
            assert_eq!(&tsv_block, expect);
        }
        // todo(youngsofun): enable it after add the setting quote_denormal
        //     {
        //         let fmt = StageFileFormatType::NdJson;
        //         let mut formatter = get_output_format(fmt, schema.clone());
        //         let buffer = formatter.serialize_block(&block)?;
        //
        //         let json_block = String::from_utf8(buffer)?;
        //         let expect = r#"{"c1":1.0,"c2":"inf"}
        // {"c1":"nan","c2":"inf"}
        // "#;
        //         assert_eq!(&json_block, expect);
        //     }

        Ok(())
    }

    #[test]
    fn test_string_escape() -> Result<()> {
        let (schema, block) =
            gen_schema_and_block(vec![TableField::new("c1", TableDataType::String)], vec![
                StringType::from_data(vec!["\0"]),
            ]);

        {
            let mut formatter = get_output_format_clickhouse("ndjson", schema)?;
            let buffer = formatter.serialize_block(&block)?;

            let expect = b"{\"c1\":\"\\u0000\"}\n";
            assert_eq!(&buffer, expect);
        }

        Ok(())
    }

    #[test]
    fn test_data_block_nullable() -> Result<()> {
        test_data_block(true)
    }

    #[test]
    fn test_data_block_not_nullable() -> Result<()> {
        test_data_block(false)
    }
}
