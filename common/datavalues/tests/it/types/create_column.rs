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

use common_datavalues::prelude::*;
use common_exception::Result;
use pretty_assertions::assert_eq;
use serde_json::json;
use serde_json::Value as JsonValue;

#[test]
fn test_create_constant() -> Result<()> {
    struct Test {
        name: &'static str,
        data_type: DataTypeImpl,
        value: DataValue,
        size: usize,
        column_expected: ColumnRef,
    }

    let tests = vec![
        Test {
            name: "boolean",
            data_type: BooleanType::new_impl(),
            value: DataValue::Boolean(true),
            size: 3,
            column_expected: Series::from_data(vec![true, true, true]),
        },
        Test {
            name: "int8",
            data_type: Int8Type::new_impl(),
            value: DataValue::Int64(3),
            size: 3,
            column_expected: Series::from_data(vec![3i8, 3, 3]),
        },
        Test {
            name: "datetime32",
            data_type: TimestampType::new_impl(0),
            value: DataValue::UInt64(1630320462),
            size: 2,
            column_expected: Series::from_data(vec![1630320462i64, 1630320462]),
        },
        Test {
            name: "datetime64",
            data_type: TimestampType::new_impl(3),
            value: DataValue::Int64(1630320462),
            size: 2,
            column_expected: Series::from_data(vec![1630320462i64, 1630320462]),
        },
        Test {
            name: "date32",
            data_type: DateType::new_impl(),
            value: DataValue::Int64(18869),
            size: 5,
            column_expected: Series::from_data(vec![18869i32, 18869, 18869, 18869, 18869]),
        },
        Test {
            name: "date16",
            data_type: DateType::new_impl(),
            value: DataValue::Int64(18869),
            size: 5,
            column_expected: Series::from_data(vec![18869i32, 18869, 18869, 18869, 18869]),
        },
        Test {
            name: "string",
            data_type: StringType::new_impl(),
            value: DataValue::String("hello".as_bytes().to_vec()),
            size: 2,
            column_expected: Series::from_data(vec!["hello", "hello"]),
        },
        Test {
            name: "nullable_i32",
            data_type: NullableType::new_impl(Int32Type::new_impl()),
            value: DataValue::Null,
            size: 2,
            column_expected: Series::from_data(&[None, None, Some(1i32)][0..2]),
        },
        Test {
            name: "variant_null",
            data_type: VariantType::new_impl(),
            value: DataValue::Null,
            size: 2,
            column_expected: Series::from_data(vec![
                VariantValue::from(JsonValue::Null),
                VariantValue::from(JsonValue::Null),
            ]),
        },
        Test {
            name: "variant_boolean",
            data_type: VariantType::new_impl(),
            value: DataValue::Boolean(true),
            size: 2,
            column_expected: Series::from_data(vec![
                VariantValue::from(json!(true)),
                VariantValue::from(json!(true)),
            ]),
        },
        Test {
            name: "variant_int64",
            data_type: VariantType::new_impl(),
            value: DataValue::Int64(1234),
            size: 2,
            column_expected: Series::from_data(vec![
                VariantValue::from(json!(1234i64)),
                VariantValue::from(json!(1234i64)),
            ]),
        },
        Test {
            name: "variant_uint64",
            data_type: VariantType::new_impl(),
            value: DataValue::UInt64(1234),
            size: 2,
            column_expected: Series::from_data(vec![
                VariantValue::from(json!(1234u64)),
                VariantValue::from(json!(1234u64)),
            ]),
        },
        Test {
            name: "variant_float64",
            data_type: VariantType::new_impl(),
            value: DataValue::Float64(12.34),
            size: 2,
            column_expected: Series::from_data(vec![
                VariantValue::from(json!(12.34f64)),
                VariantValue::from(json!(12.34f64)),
            ]),
        },
        Test {
            name: "variant_string",
            data_type: VariantType::new_impl(),
            value: DataValue::String("hello".as_bytes().to_vec()),
            size: 2,
            column_expected: Series::from_data(vec![
                VariantValue::from(json!("hello")),
                VariantValue::from(json!("hello")),
            ]),
        },
        Test {
            name: "variant_array",
            data_type: VariantArrayType::new_impl(),
            value: DataValue::Variant(VariantValue::from(json!([1, 2, 3]))),
            size: 2,
            column_expected: Series::from_data(vec![
                VariantValue::from(json!([1, 2, 3])),
                VariantValue::from(json!([1, 2, 3])),
            ]),
        },
        Test {
            name: "variant_object",
            data_type: VariantObjectType::new_impl(),
            value: DataValue::Variant(VariantValue::from(json!({"a":1,"b":2}))),
            size: 2,
            column_expected: Series::from_data(vec![
                VariantValue::from(json!({"a":1,"b":2})),
                VariantValue::from(json!({"a":1,"b":2})),
            ]),
        },
    ];

    for test in tests {
        let column = test
            .data_type
            .create_constant_column(&test.value, test.size)
            .unwrap();

        let full_column = column.convert_full_column();

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
        assert_eq!(full_column, full_column2, "case: {:#?}", test.name);
    }
    Ok(())
}
