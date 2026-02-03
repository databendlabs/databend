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
use databend_common_expression::DataBlock;
pub mod csv;
pub mod json;
pub mod ndjson;
pub mod parquet;
pub mod tsv;

pub use csv::CSVOutputFormat;
pub use json::JSONOutputFormat;
pub use ndjson::NDJSONOutputFormatBase;
pub use parquet::ParquetOutputFormat;
pub use tsv::TSVOutputFormat;

pub trait OutputFormat: Send {
    fn serialize_block(&mut self, data_block: &DataBlock) -> Result<Vec<u8>>;

    fn serialize_prefix(&self) -> Result<Vec<u8>> {
        Ok(vec![])
    }

    fn buffer_size(&mut self) -> usize {
        0
    }

    fn finalize(&mut self) -> Result<Vec<u8>>;
}

#[cfg(test)]
mod test_utils {
    use databend_common_exception::Result;
    use databend_common_expression::Column;
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchemaRef;
    use databend_common_expression::TableSchemaRefExt;
    use databend_common_expression::types::Bitmap;
    use databend_common_expression::types::BooleanType;
    use databend_common_expression::types::DateType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::nullable::NullableColumn;
    use databend_common_expression::types::number::Float64Type;
    use databend_common_expression::types::number::Int32Type;
    use databend_common_meta_app::principal::FileFormatParams;
    use databend_common_meta_app::tenant::Tenant;
    use databend_common_settings::Settings;

    use super::OutputFormat;
    use crate::ClickhouseFormatType;
    use crate::get_output_format;

    pub fn gen_schema_and_block(
        fields: Vec<TableField>,
        columns: Vec<Column>,
    ) -> (TableSchemaRef, DataBlock) {
        assert!(!columns.is_empty() && columns.len() == fields.len());
        let block = DataBlock::new_from_columns(columns);
        (TableSchemaRefExt::create(fields), block)
    }

    pub fn get_simple_block(is_nullable: bool) -> (TableSchemaRef, DataBlock) {
        let columns = vec![
            (
                TableDataType::Number(NumberDataType::Int32),
                Int32Type::from_data(vec![1i32, 2, 3]),
            ),
            (
                TableDataType::String,
                StringType::from_data(vec!["a", "b\"", "c'"]),
            ),
            (
                TableDataType::Boolean,
                BooleanType::from_data(vec![true, true, false]),
            ),
            (
                TableDataType::Number(NumberDataType::Float64),
                Float64Type::from_data(vec![1.1f64, 2.2, f64::NAN]),
            ),
            (
                TableDataType::Date,
                DateType::from_data(vec![1_i32, 2_i32, 3_i32]),
            ),
        ];

        let (columns, fields) = if !is_nullable {
            columns
                .into_iter()
                .enumerate()
                .map(|(idx, (data_type, c))| {
                    (c, TableField::new(&format!("c{}", idx + 1), data_type))
                })
                .unzip::<_, _, Vec<_>, Vec<_>>()
        } else {
            columns
                .into_iter()
                .enumerate()
                .map(|(idx, (data_type, c))| {
                    let validity = Bitmap::new_constant(true, c.len());
                    (
                        NullableColumn::new_column(c, validity),
                        TableField::new(&format!("c{}", idx + 1), data_type.wrap_nullable()),
                    )
                })
                .unzip::<_, _, Vec<_>, Vec<_>>()
        };

        gen_schema_and_block(fields, columns)
    }

    pub fn get_output_format_clickhouse(
        format_name: &str,
        schema: TableSchemaRef,
    ) -> Result<Box<dyn OutputFormat>> {
        let format = ClickhouseFormatType::parse_clickhouse_format(format_name)?;
        let settings =
            Settings::create(Tenant::new_literal("default")).get_output_format_settings()?;
        let params = FileFormatParams::default_by_type(format.typ.clone())?;
        get_output_format(schema, params, settings, Some(format.suffixes))
    }

    pub fn test_data_block(is_nullable: bool) -> Result<()> {
        let (schema, block) = get_simple_block(is_nullable);

        {
            let mut formatter = get_output_format_clickhouse("ndjson", schema)?;
            let buffer = formatter.serialize_block(&block)?;

            let tsv_block = String::from_utf8(buffer)?;
            let expect = r#"{"c1":1,"c2":"a","c3":true,"c4":1.1,"c5":"1970-01-02"}
{"c1":2,"c2":"b\"","c3":true,"c4":2.2,"c5":"1970-01-03"}
{"c1":3,"c2":"c'","c3":false,"c4":null,"c5":"1970-01-04"}
"#;
            assert_eq!(&tsv_block, expect);
        }

        Ok(())
    }
}
