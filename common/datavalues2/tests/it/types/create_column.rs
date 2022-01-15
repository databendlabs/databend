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

use std::sync::Arc;

use common_datavalues2::prelude::*;
use common_exception::Result;

#[test]
fn test_create_constant() -> Result<()> {
    struct Test {
        name: &'static str,
        data_type: DataTypePtr,
        value: DataValue,
        size: usize,
        column_expected: ColumnRef,
    }

    let tests = vec![
        Test {
            name: "boolean",
            data_type: DataTypeBoolean::arc(),
            value: DataValue::Boolean(true),
            size: 3,
            column_expected: Series::new(vec![true, true, true]),
        },
        Test {
            name: "int8",
            data_type: DataTypeInt8::arc(),
            value: DataValue::Int64(3),
            size: 3,
            column_expected: Series::new(vec![3i8, 3, 3]),
        },
        Test {
            name: "datetime32",
            data_type: DataTypeDateTime::arc(None),
            value: DataValue::UInt64(1630320462),
            size: 2,
            column_expected: Series::new(vec![1630320462u32, 1630320462]),
        },
        Test {
            name: "datetime64",
            data_type: DataTypeDateTime64::arc(None),
            value: DataValue::UInt64(1630320462),
            size: 2,
            column_expected: Series::new(vec![1630320462u64, 1630320462]),
        },
        Test {
            name: "date32",
            data_type: DataTypeDate32::arc(),
            value: DataValue::Int64(18869),
            size: 5,
            column_expected: Series::new(vec![18869i32, 18869, 18869, 18869, 18869]),
        },
        Test {
            name: "date16",
            data_type: DataTypeDate::arc(),
            value: DataValue::Int64(18869),
            size: 5,
            column_expected: Series::new(vec![18869u16, 18869, 18869, 18869, 18869]),
        },
        Test {
            name: "string",
            data_type: DataTypeString::arc(),
            value: DataValue::String("hello".as_bytes().to_vec()),
            size: 2,
            column_expected: Series::new(vec!["hello", "hello"]),
        },
        Test {
            name: "nullable_i32",
            data_type: Arc::new(DataTypeNullable::create(DataTypeInt32::arc())),
            value: DataValue::Null,
            size: 2,
            column_expected: Series::new(&[None, None, Some(1i32)][0..2]),
        },
    ];

    for test in tests {
        let column = test
            .data_type
            .create_constant_column(&test.value, test.size)
            .unwrap();

        if !test.data_type.is_nullable() {
            let c: &ConstColumn = Series::check_get(&column).unwrap();
            let full_column = c.convert_full_column();

            assert!(
                full_column == test.column_expected,
                "case: {:#?}",
                test.name
            );

            let values: Vec<DataValue> = std::iter::repeat(test.value.clone())
                .take(test.size)
                .into_iter()
                .collect();
            let full_column2 = test.data_type.create_column(&values).unwrap();

            assert!(full_column == full_column2, "case: {:#?}", test.name);
        } else {
            let c: &NullableColumn = Series::check_get(&column).unwrap();
            let full_column = c.convert_full_column();

            assert!(
                full_column == test.column_expected,
                "case: {:#?}",
                test.name
            );
        }
    }
    Ok(())
}
