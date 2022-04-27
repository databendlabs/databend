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
use common_datavalues::prelude::*;
use common_datavalues::with_match_scalar_types_error;
use common_exception::Result;
use serde_json::json;
use serde_json::Value as JsonValue;

#[test]
fn test_builder() -> Result<()> {
    struct Test {
        name: &'static str,
        column: ColumnRef,
        inject_null: bool,
    }

    let tests = vec![
        Test {
            name: "test_i32",
            column: Series::from_data(vec![1, 2, 3, 4, 5, 6, 7]),
            inject_null: false,
        },
        Test {
            name: "test_i32_nullable",
            column: Series::from_data(vec![1, 2, 3, 4, 5, 6, 7]),
            inject_null: true,
        },
        Test {
            name: "test_f64",
            column: Series::from_data(&[1.0f64, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]),
            inject_null: false,
        },
        Test {
            name: "test_bool",
            column: Series::from_data(&[true, false, true, true, false]),
            inject_null: false,
        },
        Test {
            name: "test_string",
            column: Series::from_data(&["1", "2", "3", "$"]),
            inject_null: false,
        },
        Test {
            name: "test_string_nullable",
            column: Series::from_data(&["1", "2", "3", "$"]),
            inject_null: false,
        },
        Test {
            name: "test_f64_nullable",
            column: Series::from_data(&[1.0f64, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]),
            inject_null: true,
        },
        Test {
            name: "test_u64_nullable",
            column: Series::from_data(&[
                143u64, 434, 344, 24234, 24, 324, 32, 432, 4, 324, 32, 432423,
            ]),
            inject_null: true,
        },
    ];

    fn get_column<T: Scalar>(column: &ColumnRef) -> Result<ColumnRef>
    where T::ColumnType: Clone + 'static {
        let size = column.len();

        if column.is_nullable() {
            let mut builder = NullableColumnBuilder::<T>::with_capacity(size);
            let viewer = T::try_create_viewer(column)?;

            for i in 0..viewer.size() {
                builder.append(viewer.value_at(i), viewer.valid_at(i));
            }
            let result = builder.build(size);
            Ok(result)
        } else {
            let mut builder = ColumnBuilder::<T>::with_capacity(size);
            let viewer = T::try_create_viewer(column)?;

            for i in 0..viewer.size() {
                builder.append(viewer.value_at(i));
            }

            let result = builder.build(size);
            Ok(result)
        }
    }

    for t in tests {
        let mut column = t.column;
        let size = column.len();
        if t.inject_null {
            let mut bm = MutableBitmap::with_capacity(size);
            for i in 0..size {
                bm.push(i % 3 == 1);
            }
            column = NullableColumn::wrap_inner(column, Some(bm.into()));
        }

        let ty = remove_nullable(&column.data_type());
        let ty = ty.data_type_id().to_physical_type();

        with_match_scalar_types_error!(ty, |$T| {
            let result = get_column::<$T>(&column)?;
            assert!(result == column, "test {}", t.name);
        });
    }
    Ok(())
}

#[test]
fn test_pop_data_value() -> Result<()> {
    struct Test {
        name: &'static str,
        data_type: DataTypeImpl,
        column: ColumnRef,
        expected_err: &'static str,
    }

    let tests = vec![
        Test {
            name: "test bool column",
            data_type: BooleanType::arc(),
            column: Series::from_data(&[true, true, false]),
            expected_err: "Code: 1018, displayText = Bool column is empty when pop data value.",
        },
        Test {
            name: "test primitive(u64) column",
            data_type: UInt64Type::arc(),
            column: Series::from_data(&[1u64, 2, 3]),
            expected_err:
                "Code: 1018, displayText = Primitive column array is empty when pop data value.",
        },
        Test {
            name: "test string column",
            data_type: StringType::arc(),
            column: Series::from_data(&["1", "22", "333"]),
            expected_err:
                "Code: 1018, displayText = String column array is empty when pop data value.",
        },
        Test {
            name: "test object column",
            data_type: VariantType::arc(),
            column: Series::from_data(vec![
                VariantValue::from(json!(10u64)),
                VariantValue::from(JsonValue::Bool(true)),
                VariantValue::from(JsonValue::Null),
            ]),
            expected_err:
                "Code: 1018, displayText = Object column array is empty when pop data value.",
        },
        Test {
            name: "test null column",
            data_type: NullType::arc(),
            column: NullColumn::new(3).arc(),
            expected_err: "Code: 1018, displayText = Null column is empty when pop data value.",
        },
    ];

    for test in tests {
        let name = test.name;
        let data_type = test.data_type.clone();
        let column = test.column;
        let expected_err = test.expected_err;

        // Build MutableColumn
        let mut builder = data_type.create_mutable(column.len());
        for idx in 0..column.len() {
            let value = column.get(idx);
            builder.append_data_value(value)?;
        }

        // Pop when not empty
        for idx in (0..column.len()).rev() {
            let value = builder.pop_data_value()?;
            assert_eq!(value, column.get(idx), "{} pop data value", name);
        }
        assert!(builder.is_empty());

        // Pop when builder is empty.
        let result = builder.pop_data_value();
        assert!(result.is_err());
        let error = &result.unwrap_err().to_string();
        assert_eq!(error, expected_err, "{} pop when empty", name);
    }

    Ok(())
}

#[test]
fn test_nullable_pop() -> Result<()> {
    struct Test {
        name: &'static str,
        data_type: DataTypeImpl,
        values_vec: Vec<DataValue>,
    }

    let tests = vec![
        Test {
            name: "test nullable(bool)",
            data_type: NullableType::arc(BooleanType::arc()),
            values_vec: vec![
                DataValue::Boolean(true),
                DataValue::Null,
                DataValue::Boolean(false),
                DataValue::Null,
            ],
        },
        Test {
            name: "test nullable(u64)",
            data_type: NullableType::arc(UInt64Type::arc()),
            values_vec: vec![
                DataValue::UInt64(1),
                DataValue::Null,
                DataValue::UInt64(2),
                DataValue::Null,
            ],
        },
        Test {
            name: "test nullable(string)",
            data_type: NullableType::arc(StringType::arc()),
            values_vec: vec![
                DataValue::String("1".as_bytes().to_vec()),
                DataValue::Null,
                DataValue::String("22".as_bytes().to_vec()),
                DataValue::Null,
            ],
        },
        Test {
            name: "test nullable(object)",
            data_type: NullableType::arc(VariantType::arc()),
            values_vec: vec![
                DataValue::Variant(VariantValue::from(json!(10u64))),
                DataValue::Null,
                DataValue::Variant(VariantValue::from(JsonValue::Bool(true))),
                DataValue::Null,
            ],
        },
    ];
    let expected_err_msg =
        "Code: 1018, displayText = Nullable column array is empty when pop data value.";

    for test in tests {
        let name = test.name;
        let data_type = test.data_type.clone();
        let values_vec = test.values_vec;

        // Build MutableColumn
        let mut builder = data_type.create_mutable(values_vec.len());
        for value in values_vec.iter() {
            if value.is_null() {
                builder.append_default();
            } else {
                builder.append_data_value(value.clone())?;
            }
        }

        // Pop when not empty
        for expected_value in values_vec.iter().rev() {
            let value = builder.pop_data_value()?;
            assert_eq!(&value, expected_value, "{} pop data value", name);
        }
        assert!(builder.is_empty());

        // Pop when builder is empty.
        let result = builder.pop_data_value();
        assert!(result.is_err());
        let error = &result.unwrap_err().to_string();
        assert_eq!(error, expected_err_msg, "{} pop when empty", name);
    }

    Ok(())
}
