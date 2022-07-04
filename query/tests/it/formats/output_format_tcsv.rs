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
    let mut format_setting = FormatSettings::default();

    {
        let fmt = OutputFormatType::TSV;
        let mut formatter = fmt.create_format(schema.clone(), format_setting.clone());
        let buffer = formatter.serialize_block(&block)?;

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
        let buffer = formatter.serialize_block(&block)?;

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
        DataField::new_nullable("c1", i32::to_data_type()),
        DataField::new_nullable("c2", i32::to_data_type()),
    ]);

    let columns = vec![
        Series::from_data(vec![Some(1i32), None, Some(3)]),
        Series::from_data(vec![None, Some(2i32), None]),
    ];

    let block = DataBlock::create(schema.clone(), columns);

    {
        let fmt = OutputFormatType::TSV;
        let mut formatter = fmt.create_format(schema, format_setting);
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
