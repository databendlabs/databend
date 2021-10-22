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
use pretty_assertions::assert_eq;
use serde::Serialize;
use serde_json::to_value;
use serde_json::Value;

use super::block_to_json::block_to_json;

fn val<T>(v: T) -> Value
where T: Serialize {
    to_value(v).unwrap()
}

// TODO(youngsofun): test null, Int8, dates
fn test_data_block(is_nullable: bool) -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("c1", DataType::Int32, is_nullable),
        DataField::new("c2", DataType::String, is_nullable),
        DataField::new("c3", DataType::Boolean, is_nullable),
        DataField::new("c4", DataType::Float64, is_nullable),
    ]);

    let block = DataBlock::create_by_array(schema, vec![
        Series::new(vec![1, 2, 3]),
        Series::new(vec!["a", "b", "c"]),
        Series::new(vec![true, true, false]),
        Series::new(vec![1.1, 2.2, 3.3]),
    ]);
    let json_block = block_to_json(block)?;
    let expect = vec![
        vec![val(1), val("a"), val(true), val(1.1)],
        vec![val(2), val("b"), val(true), val(2.2)],
        vec![val(3), val("c"), val(false), val(3.3)],
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
