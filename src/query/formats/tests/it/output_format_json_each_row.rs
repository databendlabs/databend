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

use common_exception::Result;
use common_expression::from_date_data;
use common_expression::from_date_data_with_validity;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::Chunk;
use common_expression::Column;
use common_expression::ColumnFrom;
use common_expression::DataField;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_formats::output_format::OutputFormatType;
use common_io::prelude::FormatSettings;
use pretty_assertions::assert_eq;

use crate::output_format_utils::get_simple_chunk;

fn test_chunk(is_nullable: bool) -> Result<()> {
    let (schema, chunk) = get_simple_chunk(is_nullable)?;
    let format_setting = FormatSettings::default();

    {
        let fmt = OutputFormatType::JsonEachRow;
        let mut formatter = fmt.create_format(schema, format_setting);
        let buffer = formatter.serialize_block(&block)?;

        let tsv_block = String::from_utf8(buffer)?;
        let expect = r#"{"c1":1,"c2":"a","c3":1,"c4":1.1,"c5":"1970-01-02"}
{"c1":2,"c2":"b\"","c3":1,"c4":2.2,"c5":"1970-01-03"}
{"c1":3,"c2":"c'","c3":0,"c4":3.3,"c5":"1970-01-04"}
"#;
        assert_eq!(&tsv_block, expect);
    }

    Ok(())
}

#[test]
fn test_null() -> Result<()> {
    let format_setting = FormatSettings::default();

    let schema = DataSchemaRefExt::create(vec![
        DataField::new(
            "c1",
            DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int32))),
        ),
        DataField::new(
            "c2",
            DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int32))),
        ),
    ]);
    let chunk = Chunk::new(vec![
        (
            Value::Column(Column::from_data_with_validity(vec![1i32, 0, 3], vec![
                true, false, true,
            ])),
            DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int32))),
        ),
        (
            Value::Column(Column::from_data_with_validity(vec![0i32, 2, 0], vec![
                false, true, false,
            ])),
            DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int32))),
        ),
    ]);

    {
        let fmt = OutputFormatType::JsonEachRow;
        let mut formatter = fmt.create_format(schema, format_setting);
        let buffer = formatter.serialize_block(&chunk)?;

        let tsv_block = String::from_utf8(buffer)?;
        let expect = r#"{"c1":1,"c2":null}
{"c1":null,"c2":2}
{"c1":3,"c2":null}
"#;
        assert_eq!(&tsv_block, expect);
    }
    Ok(())
}

#[test]
fn test_denormal() -> Result<()> {
    let format_setting = FormatSettings::default();

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("c1", DataType::Number(NumberDataType::Float32)),
        DataField::new("c2", DataType::Number(NumberDataType::Float32)),
    ]);
    let chunk = Chunk::new(vec![
        (
            Value::Column(Column::from_data(vec![1f32, f32::NAN])),
            DataType::Number(NumberDataType::Float32),
        ),
        (
            Value::Column(Column::from_data(vec![f32::INFINITY, f32::NEG_INFINITY])),
            DataType::Number(NumberDataType::Float32),
        ),
    ]);

    {
        let fmt = OutputFormatType::JsonEachRow;
        let mut formatter = fmt.create_format(schema.clone(), format_setting);
        let buffer = formatter.serialize_block(&chunk)?;

        let tsv_block = String::from_utf8(buffer)?;
        let expect = r#"{"c1":1.0,"c2":null}
{"c1":null,"c2":null}
"#;
        assert_eq!(&tsv_block, expect);
    }

    {
        let format_setting = FormatSettings {
            json_quote_denormals: true,
            ..FormatSettings::default()
        };
        let fmt = OutputFormatType::JsonEachRow;
        let mut formatter = fmt.create_format(schema, format_setting);
        let buffer = formatter.serialize_block(&chunk)?;

        let json_block = String::from_utf8(buffer)?;
        let expect = r#"{"c1":1.0,"c2":"inf"}
{"c1":"nan","c2":"inf"}
"#;
        assert_eq!(&json_block, expect);
    }

    Ok(())
}

#[test]
fn test_string_escape() -> Result<()> {
    let format_setting = FormatSettings::default();

    let schema = DataSchemaRefExt::create(vec![DataField::new("c1", DataType::String)]);
    let chunk = Chunk::new(vec![(
        Value::Column(Column::from_data(vec!["\0"])),
        DataType::String,
    )]);

    {
        let fmt = OutputFormatType::JsonEachRow;
        let mut formatter = fmt.create_format(schema, format_setting);
        let buffer = formatter.serialize_block(&chunk)?;

        let expect = b"{\"c1\":\"\\u0000\"}\n";
        assert_eq!(&buffer, expect);
    }

    Ok(())
}

#[test]
fn test_chunk_nullable() -> Result<()> {
    test_chunk(true)
}

#[test]
fn test_chunk_not_nullable() -> Result<()> {
    test_chunk(false)
}
