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

use common_arrow::bitmap::MutableBitmap;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use databend_query::servers::http::formats::tsv_output::block_to_tsv;
use pretty_assertions::assert_eq;

fn test_data_block(is_nullable: bool) -> Result<()> {
    let schema = match is_nullable {
        false => DataSchemaRefExt::create(vec![
            DataField::new("c1", i32::to_data_type()),
            DataField::new("c2", Vu8::to_data_type()),
            DataField::new("c3", bool::to_data_type()),
            DataField::new("c4", f64::to_data_type()),
            DataField::new("c5", DateType::new_impl()),
        ]),
        true => DataSchemaRefExt::create(vec![
            DataField::new_nullable("c1", i32::to_data_type()),
            DataField::new_nullable("c2", Vu8::to_data_type()),
            DataField::new_nullable("c3", bool::to_data_type()),
            DataField::new_nullable("c4", f64::to_data_type()),
            DataField::new_nullable("c5", DateType::new_impl()),
        ]),
    };

    let block = DataBlock::create(schema.clone(), vec![
        Series::from_data(vec![1, 2, 3]),
        Series::from_data(vec!["a", "b", "c"]),
        Series::from_data(vec![true, true, false]),
        Series::from_data(vec![1.1, 2.2, 3.3]),
        Series::from_data(vec![1_i32, 2_i32, 3_i32]),
    ]);

    let block = if is_nullable {
        let columns = block
            .columns()
            .iter()
            .map(|c| {
                let mut validity = MutableBitmap::new();
                validity.extend_constant(c.len(), true);
                NullableColumn::wrap_inner(c.clone(), Some(validity.into()))
            })
            .collect();
        DataBlock::create(schema, columns)
    } else {
        block
    };
    let format = FormatSettings::default();
    let json_block = String::from_utf8(block_to_tsv(&block, &format)?)?;
    let expect = "1\ta\t1\t1.1\t1970-01-02\n\
                        2\tb\t1\t2.2\t1970-01-03\n\
                        3\tc\t0\t3.3\t1970-01-04\n";

    assert_eq!(&json_block, expect);
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
