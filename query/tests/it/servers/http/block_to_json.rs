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

use common_arrow::arrow::bitmap::MutableBitmap;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use databend_query::servers::http::v1::block_to_json::block_to_json;
use pretty_assertions::assert_eq;
use serde::Serialize;
use serde_json::to_value;
use serde_json::Value;

fn val<T>(v: T) -> Value
where T: Serialize {
    to_value(v).unwrap()
}

fn test_data_block(is_nullable: bool) -> Result<()> {
    let schema = match is_nullable {
        false => DataSchemaRefExt::create(vec![
            DataField::new("c1", i32::to_data_type()),
            DataField::new("c2", Vu8::to_data_type()),
            DataField::new("c3", bool::to_data_type()),
            DataField::new("c4", f64::to_data_type()),
            DataField::new("c5", Date16Type::arc()),
        ]),
        true => DataSchemaRefExt::create(vec![
            DataField::new_nullable("c1", i32::to_data_type()),
            DataField::new_nullable("c2", Vu8::to_data_type()),
            DataField::new_nullable("c3", bool::to_data_type()),
            DataField::new_nullable("c4", f64::to_data_type()),
            DataField::new_nullable("c5", Date16Type::arc()),
        ]),
    };

    let block = DataBlock::create(schema.clone(), vec![
        Series::from_data(vec![1, 2, 3]),
        Series::from_data(vec!["a", "b", "c"]),
        Series::from_data(vec![true, true, false]),
        Series::from_data(vec![1.1, 2.2, 3.3]),
        Series::from_data(vec![1_u16, 2_u16, 3_u16]),
    ]);

    let block = if is_nullable {
        let columns = block
            .columns()
            .iter()
            .map(|c| {
                let mut validity = MutableBitmap::new();
                validity.extend_constant(c.len(), true);
                NullableColumn::new(c.clone(), validity.into()).arc()
            })
            .collect();
        DataBlock::create(schema, columns)
    } else {
        block
    };

    let json_block = block_to_json(&block)?;
    let expect = vec![
        vec![val(1), val("a"), val(true), val(1.1), val("1970-01-02")],
        vec![val(2), val("b"), val(true), val(2.2), val("1970-01-03")],
        vec![val(3), val("c"), val(false), val(3.3), val("1970-01-04")],
    ];

    assert_eq!(json_block, expect);
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
