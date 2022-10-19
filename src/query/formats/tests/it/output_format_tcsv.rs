// Copyright 2021 Datafuse Labs.
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
    let mut format_setting = FormatSettings::default();

    {
        let fmt = OutputFormatType::TSV;
        let mut formatter = fmt.create_format(schema.clone(), format_setting.clone());
        let buffer = formatter.serialize_block(&chunk)?;

        let tsv_block = String::from_utf8(buffer)?;
        let expect = "1\ta\t1\t1.1\t1970-01-02\n\
                            2\tb\"\t1\t2.2\t1970-01-03\n\
                            3\tc\\'\t0\t3.3\t1970-01-04\n";
        assert_eq!(&tsv_block, expect);

        let fmt = OutputFormatType::TSVWithNames;
        let formatter = fmt.create_format(schema.clone(), format_setting.clone());
        let buffer = formatter.serialize_prefix()?;
        let tsv_block = String::from_utf8(buffer)?;
        let names = "c1\tc2\tc3\tc4\tc5\n".to_string();
        assert_eq!(tsv_block, names);

        let fmt = OutputFormatType::TSVWithNamesAndTypes;
        let formatter = fmt.create_format(schema.clone(), format_setting.clone());
        let buffer = formatter.serialize_prefix()?;
        let tsv_block = String::from_utf8(buffer)?;

        let types = if is_nullable {
            "Nullable(Int32)\tNullable(String)\tNullable(Boolean)\tNullable(Float64)\tNullable(Date)\n"
                .to_string()
        } else {
            "Int32\tString\tBoolean\tFloat64\tDate\n".to_string()
        };
        assert_eq!(tsv_block, names + &types);
    }

    {
        format_setting.record_delimiter = vec![b'%'];
        format_setting.field_delimiter = vec![b'$'];

        let fmt = OutputFormatType::CSV;
        let mut formatter = fmt.create_format(schema, format_setting);
        let buffer = formatter.serialize_block(&chunk)?;

        let csv_block = String::from_utf8(buffer)?;
        let expect = "1$\"a\"$1$1.1$\"1970-01-02\"%\
                            2$\"b\\\"\"$1$2.2$\"1970-01-03\"%\
                            3$\"c'\"$0$3.3$\"1970-01-04\"%";
        assert_eq!(&csv_block, expect);
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
        let fmt = OutputFormatType::TSV;
        let mut formatter = fmt.create_format(schema, format_setting);
        let buffer = formatter.serialize_block(&chunk)?;

        let tsv_block = String::from_utf8(buffer)?;
        let expect = "1\t\\N\n\\N\t2\n3\t\\N\n";
        assert_eq!(&tsv_block, expect);
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
