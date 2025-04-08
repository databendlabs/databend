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

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::number::Float64Type;
use databend_common_expression::types::number::Int32Type;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::StringType;
use databend_common_expression::FromData;
use databend_common_io::prelude::FormatSettings;
use databend_query::servers::http::v1::BlocksSerializer;
use pretty_assertions::assert_eq;

fn test_data_block(is_nullable: bool) -> Result<()> {
    let mut columns = vec![
        Int32Type::from_data(vec![1, 2, 3]),
        StringType::from_data(vec!["a", "b", "c"]),
        BooleanType::from_data(vec![true, true, false]),
        Float64Type::from_data(vec![1.1, 2.2, 3.3]),
        DateType::from_data(vec![1_i32, 2_i32, 3_i32]),
    ];

    if is_nullable {
        columns = columns
            .iter()
            .map(|c| NullableColumn::new_column(c.clone(), Bitmap::new_constant(true, c.len())))
            .collect();
    }

    let format = FormatSettings::default();
    let mut serializer = BlocksSerializer::new(Some(format));
    serializer.append(columns, 3);
    let expect = [
        vec!["1", "a", "1", "1.1", "1970-01-02"],
        vec!["2", "b", "1", "2.2", "1970-01-03"],
        vec!["3", "c", "0", "3.3", "1970-01-04"],
    ]
    .iter()
    .map(|r| r.iter().map(|v| v.to_string()).collect::<Vec<_>>())
    .collect::<Vec<_>>();

    assert_eq!(
        serde_json::to_string(&serializer)?,
        serde_json::to_string(&expect)?
    );
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
fn test_empty_block() -> Result<()> {
    let format = FormatSettings::default();
    let serializer = BlocksSerializer::new(Some(format));
    assert!(serializer.is_empty());
    Ok(())
}
