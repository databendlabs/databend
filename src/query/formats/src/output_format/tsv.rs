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
use databend_common_meta_app::principal::TsvFileFormatParams;

use crate::field_encoder::FieldEncoderCSV;
use crate::field_encoder::helpers::write_tsv_escaped_string;
use crate::output_format::OutputFormat;

pub struct TSVOutputFormat {
    schema: TableSchemaRef,
    field_encoder: FieldEncoderCSV,
    field_delimiter: u8,
    record_delimiter: Vec<u8>,

    headers: u8,
}

impl TSVOutputFormat {
    pub fn create(
        schema: TableSchemaRef,
        params: &TsvFileFormatParams,
        field_encoder: FieldEncoderCSV,
        headers: u8,
    ) -> Self {
        Self {
            schema,
            field_encoder,
            field_delimiter: params.field_delimiter.as_bytes()[0],
            record_delimiter: params.record_delimiter.as_bytes().to_vec(),
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
            write_tsv_escaped_string(v.as_bytes(), &mut buf, self.field_delimiter);
        }

        buf.extend_from_slice(&self.record_delimiter);
        buf
    }
}

impl OutputFormat for TSVOutputFormat {
    fn serialize_block(&mut self, block: &DataBlock) -> Result<Vec<u8>> {
        let rows_size = block.num_rows();
        let mut buf = Vec::with_capacity(block.memory_size());

        let fd = self.field_delimiter;
        let rd = &self.record_delimiter;

        let columns: Vec<Column> = block
            .convert_to_full()
            .columns()
            .iter()
            .map(|column| column.value().into_column().unwrap())
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
mod test {

    use std::collections::BTreeMap;

    use databend_common_exception::Result;
    use databend_common_expression::FromData;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::number::Int32Type;
    use databend_common_meta_app::principal::FileFormatOptionsReader;
    use databend_common_meta_app::principal::FileFormatParams;
    use databend_common_meta_app::tenant::Tenant;
    use databend_common_settings::Settings;
    use pretty_assertions::assert_eq;

    use crate::get_output_format;
    use crate::output_format::test_utils::gen_schema_and_block;
    use crate::output_format::test_utils::get_output_format_clickhouse;
    use crate::output_format::test_utils::get_simple_block;

    fn test_data_block(is_nullable: bool) -> Result<()> {
        let (schema, block) = get_simple_block(is_nullable);

        {
            let mut formatter = get_output_format_clickhouse("tsv", schema.clone())?;
            let buffer = formatter.serialize_block(&block)?;

            let tsv_block = String::from_utf8(buffer)?;
            let expect = "1\ta\t1\t1.1\t1970-01-02\n\
                            2\tb\"\t1\t2.2\t1970-01-03\n\
                            3\tc\'\t0\tNaN\t1970-01-04\n";
            assert_eq!(&tsv_block, expect);

            let formatter = get_output_format_clickhouse("TsvWithNames", schema.clone())?;
            let buffer = formatter.serialize_prefix()?;
            let tsv_block = String::from_utf8(buffer)?;
            let names = "c1\tc2\tc3\tc4\tc5\n".to_string();
            assert_eq!(tsv_block, names);

            let formatter = get_output_format_clickhouse("TsvWithNamesAndTypes", schema.clone())?;
            let buffer = formatter.serialize_prefix()?;
            let tsv_block = String::from_utf8(buffer)?;

            let types = if is_nullable {
                "Int32 NULL\tString NULL\tBoolean NULL\tFloat64 NULL\tDate NULL\n".to_string()
            } else {
                "Int32\tString\tBoolean\tFloat64\tDate\n".to_string()
            };
            assert_eq!(tsv_block, names + &types);
        }

        {
            let settings = Settings::create(Tenant::new_literal("default"));
            let mut options = BTreeMap::<String, String>::new();
            options.insert("type".to_string(), "csv".to_string());
            options.insert("field_delimiter".to_string(), "$".to_string());
            options.insert("record_delimiter".to_string(), "\r\n".to_string());

            let params = FileFormatParams::try_from_reader(
                FileFormatOptionsReader::from_map(options.clone()),
                false,
            )?;
            let mut output_format =
                get_output_format(schema, params, settings.get_output_format_settings()?, None)?;
            let buffer = output_format.serialize_block(&block)?;

            let csv_block = String::from_utf8(buffer)?;
            let expect = "1$\"a\"$true$1.1$\"1970-01-02\"\r\n2$\"b\"\"\"$true$2.2$\"1970-01-03\"\r\n3$\"c'\"$false$NaN$\"1970-01-04\"\r\n";
            assert_eq!(&csv_block, expect);
        }
        Ok(())
    }

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
            let mut formatter = get_output_format_clickhouse("tsv", schema)?;
            let buffer = formatter.serialize_block(&block)?;

            let tsv_block = String::from_utf8(buffer)?;
            let expect = "1\t\\N\n\\N\t2\n3\t\\N\n";
            assert_eq!(&tsv_block, expect);
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

    #[test]
    fn test_field_delimiter_with_ascii_control_code() -> Result<()> {
        let (schema, block) = get_simple_block(false);

        let settings = Settings::create(Tenant::new_literal("default"));
        let mut options = BTreeMap::<String, String>::new();
        options.insert("type".to_string(), "csv".to_string());
        options.insert("field_delimiter".to_string(), "\x01".to_string());
        options.insert("record_delimiter".to_string(), "\r\n".to_string());
        let params = FileFormatParams::try_from_reader(
            FileFormatOptionsReader::from_map(options.clone()),
            false,
        )?;
        let mut output_format =
            get_output_format(schema, params, settings.get_output_format_settings()?, None)?;

        let buffer = output_format.serialize_block(&block)?;

        let csv_block = String::from_utf8(buffer)?;
        let expect = "1\x01\"a\"\x01true\x011.1\x01\"1970-01-02\"\r\n2\x01\"b\"\"\"\x01true\x012.2\x01\"1970-01-03\"\r\n3\x01\"c'\"\x01false\x01NaN\x01\"1970-01-04\"\r\n";
        assert_eq!(&csv_block, expect);

        Ok(())
    }
}
