// Copyright 2023 Datafuse Labs.
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

use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use fastrace::func_name;

use crate::common;

// These bytes are built when a new version in introduced,
// and are kept for backward compatibility test.
//
// *************************************************************
// * These messages should never be updated,                   *
// * only be added when a new version is added,                *
// * or be removed when an old version is no longer supported. *
// *************************************************************
//
// The message bytes are built from the output of `test_pb_from_to()`
#[test]
fn test_decode_v29_schema() -> anyhow::Result<()> {
    let schema_v29 = [
        10, 28, 10, 1, 97, 26, 17, 154, 2, 8, 34, 0, 160, 6, 29, 168, 6, 24, 160, 6, 29, 168, 6,
        24, 160, 6, 29, 168, 6, 24, 10, 104, 10, 1, 98, 26, 91, 202, 2, 82, 10, 2, 98, 49, 10, 2,
        98, 50, 18, 47, 202, 2, 38, 10, 3, 98, 49, 49, 10, 3, 98, 49, 50, 18, 9, 138, 2, 0, 160, 6,
        29, 168, 6, 24, 18, 9, 146, 2, 0, 160, 6, 29, 168, 6, 24, 160, 6, 29, 168, 6, 24, 160, 6,
        29, 168, 6, 24, 18, 17, 154, 2, 8, 66, 0, 160, 6, 29, 168, 6, 24, 160, 6, 29, 168, 6, 24,
        160, 6, 29, 168, 6, 24, 160, 6, 29, 168, 6, 24, 32, 1, 160, 6, 29, 168, 6, 24, 10, 30, 10,
        1, 99, 26, 17, 154, 2, 8, 34, 0, 160, 6, 29, 168, 6, 24, 160, 6, 29, 168, 6, 24, 32, 4,
        160, 6, 29, 168, 6, 24, 10, 49, 10, 10, 100, 101, 99, 105, 109, 97, 108, 49, 50, 56, 26,
        27, 218, 2, 18, 10, 10, 8, 18, 16, 3, 160, 6, 29, 168, 6, 24, 160, 6, 29, 168, 6, 24, 160,
        6, 29, 168, 6, 24, 32, 5, 160, 6, 29, 168, 6, 24, 10, 49, 10, 10, 100, 101, 99, 105, 109,
        97, 108, 50, 53, 54, 26, 27, 218, 2, 18, 18, 10, 8, 46, 16, 6, 160, 6, 29, 168, 6, 24, 160,
        6, 29, 168, 6, 24, 160, 6, 29, 168, 6, 24, 32, 6, 160, 6, 29, 168, 6, 24, 10, 30, 10, 9,
        101, 109, 112, 116, 121, 95, 109, 97, 112, 26, 9, 226, 2, 0, 160, 6, 29, 168, 6, 24, 32, 7,
        160, 6, 29, 168, 6, 24, 24, 8, 160, 6, 29, 168, 6, 24,
    ];

    let b1 = TableDataType::Tuple {
        fields_name: vec!["b11".to_string(), "b12".to_string()],
        fields_type: vec![TableDataType::Boolean, TableDataType::String],
    };
    let b = TableDataType::Tuple {
        fields_name: vec!["b1".to_string(), "b2".to_string()],
        fields_type: vec![b1, TableDataType::Number(NumberDataType::Int64)],
    };
    let fields = vec![
        TableField::new("a", TableDataType::Number(NumberDataType::UInt64)),
        TableField::new("b", b),
        TableField::new("c", TableDataType::Number(NumberDataType::UInt64)),
        TableField::new(
            "decimal128",
            TableDataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
                precision: 18,
                scale: 3,
            })),
        ),
        TableField::new(
            "decimal256",
            TableDataType::Decimal(DecimalDataType::Decimal256(DecimalSize {
                precision: 46,
                scale: 6,
            })),
        ),
        TableField::new("empty_map", TableDataType::EmptyMap),
    ];
    let want = || TableSchema::new(fields.clone());
    common::test_load_old(func_name!(), schema_v29.as_slice(), 29, want())?;
    common::test_pb_from_to(func_name!(), want())?;
    Ok(())
}
