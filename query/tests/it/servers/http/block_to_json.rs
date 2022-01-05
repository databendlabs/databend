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

use std::collections::BTreeMap;

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
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("c1", DataType::Int32, is_nullable),
        DataField::new("c2", DataType::String, is_nullable),
        DataField::new("c3", DataType::Boolean, is_nullable),
        DataField::new("c4", DataType::Float64, is_nullable),
        DataField::new("c5", DataType::Date16, is_nullable),
    ]);

    let block = DataBlock::create_by_array(schema, vec![
        Series::new(vec![1, 2, 3]),
        Series::new(vec!["a", "b", "c"]),
        Series::new(vec![true, true, false]),
        Series::new(vec![1.1, 2.2, 3.3]),
        Series::new(vec![1_u16, 2_u16, 3_u16])
            .cast_with_type(&DataType::Date16)
            .unwrap(),
    ]);
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

#[test]
fn test_enum_block() -> Result<()> {
    let test_enum8_type = DataType::Enum8(BTreeMap::from([
        (String::from("small"), 0_u8),
        (String::from("medium"), 1_u8),
        (String::from("large"), 2_u8),
    ]));

    let test_enum16_type = DataType::Enum16(BTreeMap::from([
        (String::from("small"), 0_u16),
        (String::from("medium"), 1_u16),
        (String::from("large"), 2_u16),
    ]));
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("c1", test_enum8_type, false),
        DataField::new("c2", test_enum16_type, false),
    ]);

    let block = DataBlock::create_by_array(schema, vec![
        Series::new(vec![0_u8, 1_u8, 2_u8]),
        Series::new(vec![0_u16, 1_u16, 2_u16]),
    ]);
    let json_block = block_to_json(&block)?;
    let expect = vec![
        vec![val("small"), val("small")],
        vec![val("medium"), val("medium")],
        vec![val("large"), val("large")],
    ];

    assert_eq!(json_block, expect);
    Ok(())
}
