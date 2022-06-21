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
use common_datavalues::prelude::*;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use databend_query::formats::output_format::OutputFormatType;
use pretty_assertions::assert_eq;

use crate::formats::output_format_utils::get_simple_block;

fn test_data_block(is_nullable: bool) -> Result<()> {
    let block = get_simple_block(is_nullable)?;
    let schema = block.schema().clone();
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
        DataField::new_nullable("c1", i32::to_data_type()),
        DataField::new_nullable("c2", i32::to_data_type()),
    ]);

    let columns = vec![
        Series::from_data(vec![Some(1i32), None, Some(3)]),
        Series::from_data(vec![None, Some(2i32), None]),
    ];

    let block = DataBlock::create(schema.clone(), columns);

    {
        let fmt = OutputFormatType::JsonEachRow;
        let mut formatter = fmt.create_format(schema, format_setting);
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

#[test]
fn test_denormal() -> Result<()> {
    let format_setting = FormatSettings::default();
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("c1", f32::to_data_type()),
        DataField::new("c2", f32::to_data_type()),
    ]);

    let columns = vec![
        Series::from_data(vec![1f32, f32::NAN]),
        Series::from_data(vec![f32::INFINITY, f32::NEG_INFINITY]),
    ];

    let block = DataBlock::create(schema.clone(), columns);

    {
        let fmt = OutputFormatType::JsonEachRow;
        let mut formatter = fmt.create_format(schema.clone(), format_setting);
        let buffer = formatter.serialize_block(&block)?;

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
        let buffer = formatter.serialize_block(&block)?;

        let tsv_block = String::from_utf8(buffer)?;
        let expect = r#"{"c1":1.0,"c2":"inf"}
{"c1":"nan","c2":"inf"}
"#;
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
