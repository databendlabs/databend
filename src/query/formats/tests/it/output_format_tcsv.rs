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

use std::collections::BTreeMap;

use databend_common_exception::Result;
use databend_common_expression::types::number::Int32Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_formats::FileFormatOptionsExt;
use databend_common_meta_app::principal::FileFormatOptionsAst;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::tenant::Tenant;
use databend_common_settings::Settings;
use pretty_assertions::assert_eq;

use crate::get_output_format_clickhouse;
use crate::output_format_utils::gen_schema_and_block;
use crate::output_format_utils::get_simple_block;

fn test_data_block(is_nullable: bool) -> Result<()> {
    let (schema, block) = get_simple_block(is_nullable);

    {
        let mut formatter = get_output_format_clickhouse("tsv", schema.clone())?;
        let buffer = formatter.serialize_block(&block)?;

        let tsv_block = String::from_utf8(buffer)?;
        let expect = "1\ta\t1\t1.1\t1970-01-02\n\
                            2\tb\"\t1\t2.2\t1970-01-03\n\
                            3\tc\'\t0\tnan\t1970-01-04\n";
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

        let params =
            FileFormatParams::try_from_ast(FileFormatOptionsAst::new(options.clone()), false)?;
        let mut options = FileFormatOptionsExt::create_from_settings(&settings, false)?;
        let mut output_format = options.get_output_format(schema, params)?;
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
    let params = FileFormatParams::try_from_ast(FileFormatOptionsAst::new(options.clone()), false)?;
    let mut options = FileFormatOptionsExt::create_from_settings(&settings, false)?;
    let mut output_format = options.get_output_format(schema, params)?;
    let buffer = output_format.serialize_block(&block)?;

    let csv_block = String::from_utf8(buffer)?;
    let expect = "1\x01\"a\"\x01true\x011.1\x01\"1970-01-02\"\r\n2\x01\"b\"\"\"\x01true\x012.2\x01\"1970-01-03\"\r\n3\x01\"c'\"\x01false\x01NaN\x01\"1970-01-04\"\r\n";
    assert_eq!(&csv_block, expect);

    Ok(())
}
