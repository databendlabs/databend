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

use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::scalars::*;
use serde_json::json;

use super::scalar_function_test::ScalarFunctionWithFieldTest;
use crate::scalars::scalar_function_test::test_scalar_functions;
use crate::scalars::scalar_function_test::test_scalar_functions_with_type;
use crate::scalars::scalar_function_test::ScalarFunctionTest;

#[test]
fn test_cast_function() -> Result<()> {
    let tests = vec![
        ("to_int8", ScalarFunctionTest {
            name: "cast-int64-to-int8-passed",
            columns: vec![Series::from_data(vec![4i64, 3, 2, 4])],
            expect: Series::from_data(vec![4i8, 3, 2, 4]),
            error: "",
        }),
        ("to_int8", ScalarFunctionTest {
            name: "cast-string-to-int8-passed",
            columns: vec![Series::from_data(vec!["4", "3", "2", "4"])],
            expect: Series::from_data(vec![4i8, 3, 2, 4]),
            error: "",
        }),
        ("to_int16", ScalarFunctionTest {
            name: "cast-string-to-int16-passed",
            columns: vec![Series::from_data(vec!["4", "3", "2", "4"])],
            expect: Series::from_data(vec![4i16, 3, 2, 4]),
            error: "",
        }),
        ("to_int32", ScalarFunctionTest {
            name: "cast-string-to-int32-passed",
            columns: vec![Series::from_data(vec!["4", "3", "2", "4"])],
            expect: Series::from_data(vec![4i32, 3, 2, 4]),
            error: "",
        }),
        ("to_int32", ScalarFunctionTest {
            name: "cast-string-to-int32-error-passed",
            columns: vec![Series::from_data(vec!["X4", "3", "2", "4"])],
            expect: Series::from_data(vec![4i32, 3, 2, 4]),
            error: "Cast error happens in casting from String to Int32",
        }),
        ("to_int32", ScalarFunctionTest {
            name: "cast-string-to-int32-error-as_null-passed",
            columns: vec![Series::from_data(vec!["X4", "3", "2", "4"])],
            expect: Series::from_data(vec![Some(0i32), Some(3), Some(2), Some(4)]),
            error: "Cast error happens in casting from String to Int32",
        }),
        ("to_int64", ScalarFunctionTest {
            name: "cast-string-to-int64-passed",
            columns: vec![Series::from_data(vec!["4", "3", "2", "4"])],
            expect: Series::from_data(vec![4i64, 3, 2, 4]),
            error: "",
        }),
        ("to_date", ScalarFunctionTest {
            name: "cast-string-to-date32-passed",
            columns: vec![Series::from_data(vec!["2021-03-05", "2021-10-24"])],
            expect: Series::from_data(vec![18691i32, 18924]),
            error: "",
        }),
        ("to_datetime", ScalarFunctionTest {
            name: "cast-string-to-datetime-passed",
            columns: vec![Series::from_data(vec![
                "2021-03-05 01:01:01",
                "2021-10-24 10:10:10",
            ])],
            expect: Series::from_data(vec![1614906061000000i64, 1635070210000000]),
            error: "",
        }),
        ("to_datetime", ScalarFunctionTest {
            name: "cast-int64-to-datetime-passed",
            columns: vec![Series::from_data(vec![
                1630833797i64,
                1630833797123,
                1630833797123456,
            ])],
            expect: Series::from_data(vec![
                1630833797000000i64,
                1630833797123000,
                1630833797123456,
            ]),
            error: "",
        }),
    ];

    for (op, test) in tests {
        test_scalar_functions(op, &[test])?;
    }

    Ok(())
}

#[test]
fn test_datetime_cast_function() -> Result<()> {
    let tests = vec![
        ("to_varchar", ScalarFunctionWithFieldTest {
            name: "cast-date32-to-string-passed",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![18691i32, 18924]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec!["2021-03-05", "2021-10-24"]),
            error: "",
        }),
        ("to_varchar", ScalarFunctionWithFieldTest {
            name: "cast-datetime-to-string-passed",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1614906061000000i64, 1635070210000000]),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec!["2021-03-05 01:01:01", "2021-10-24 10:10:10"]),
            error: "",
        }),
    ];

    for (op, test) in tests {
        test_scalar_functions_with_type(op, &[test])?;
    }

    Ok(())
}

#[test]
fn test_cast_variant_function() -> Result<()> {
    let tests = vec![
        (
            CastFunction::create("cast", "variant", DateType::new_impl())?,
            ScalarFunctionWithFieldTest {
                name: "cast-date32-to-variant-error",
                columns: vec![ColumnWithField::new(
                    Series::from_data(vec![18691i32, 18924]),
                    DataField::new("dummy_1", DateType::new_impl()),
                )],
                expect: Arc::new(NullColumn::new(2)),
                error: "Expression type does not match column data type, expecting VARIANT but got Date",
            },
        ),
        (
            CastFunction::create("cast", "variant", BooleanType::new_impl())?,
            ScalarFunctionWithFieldTest {
                name: "cast-bool-to-variant-passed",
                columns: vec![ColumnWithField::new(
                    Series::from_data(vec![true, false]),
                    DataField::new("dummy_1", BooleanType::new_impl()),
                )],
                expect: Series::from_data(vec![VariantValue::from(json!(true)), VariantValue::from(json!(false))]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant", Int8Type::new_impl())?,
            ScalarFunctionWithFieldTest {
                name: "cast-int8-to-variant-passed",
                columns: vec![ColumnWithField::new(
                    Series::from_data(vec![-128i8, 127]),
                    DataField::new("dummy_1", Int8Type::new_impl()),
                )],
                expect: Series::from_data(vec![VariantValue::from(json!(-128i8)), VariantValue::from(json!(127i8))]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant", Int16Type::new_impl())?,
            ScalarFunctionWithFieldTest {
                name: "cast-int16-to-variant-passed",
                columns: vec![ColumnWithField::new(
                    Series::from_data(vec![-32768i16, 32767]),
                    DataField::new("dummy_1", Int16Type::new_impl()),
                )],
                expect: Series::from_data(vec![VariantValue::from(json!(-32768i16)), VariantValue::from(json!(32767i16))]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant", Int32Type::new_impl())?,
            ScalarFunctionWithFieldTest {
                name: "cast-int32-to-variant-passed",
                columns: vec![ColumnWithField::new(
                    Series::from_data(vec![-2147483648i32, 2147483647]),
                    DataField::new("dummy_1", Int32Type::new_impl()),
                )],
                expect: Series::from_data(vec![VariantValue::from(json!(-2147483648i32)), VariantValue::from(json!(2147483647i32))]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant", Int64Type::new_impl())?,
            ScalarFunctionWithFieldTest {
                name: "cast-int64-to-variant-passed",
                columns: vec![ColumnWithField::new(
                    Series::from_data(vec![
                        -9223372036854775808i64,
                        9223372036854775807,
                    ]),
                    DataField::new("dummy_1", Int64Type::new_impl()),
                )],
                expect: Series::from_data(vec![
                    VariantValue::from(json!(-9223372036854775808i64)),
                    VariantValue::from(json!(9223372036854775807i64)),
                ]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant", UInt8Type::new_impl())?,
            ScalarFunctionWithFieldTest {
                name: "cast-uint8-to-variant-passed",
                columns: vec![ColumnWithField::new(
                    Series::from_data(vec![0u8, 255]),
                    DataField::new("dummy_1", UInt8Type::new_impl()),
                )],
                expect: Series::from_data(vec![VariantValue::from(json!(0u8)), VariantValue::from(json!(255u8))]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant", UInt16Type::new_impl())?,
            ScalarFunctionWithFieldTest {
                name: "cast-uint16-to-variant-passed",
                columns: vec![ColumnWithField::new(
                    Series::from_data(vec![0u16, 65535]),
                    DataField::new("dummy_1", UInt16Type::new_impl()),
                )],
                expect: Series::from_data(vec![VariantValue::from(json!(0u16)), VariantValue::from(json!(65535u16))]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant", UInt32Type::new_impl())?,
            ScalarFunctionWithFieldTest {
                name: "cast-uint32-to-variant-passed",
                columns: vec![ColumnWithField::new(
                    Series::from_data(vec![0u32, 4294967295]),
                    DataField::new("dummy_1", UInt32Type::new_impl()),
                )],
                expect: Series::from_data(vec![VariantValue::from(json!(0u32)), VariantValue::from(json!(4294967295u32))]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant", UInt64Type::new_impl())?,
            ScalarFunctionWithFieldTest {
                name: "cast-uint64-to-variant-passed",
                columns: vec![ColumnWithField::new(
                    Series::from_data(vec![0u64, 18446744073709551615]),
                    DataField::new("dummy_1", UInt64Type::new_impl()),
                )],
                expect: Series::from_data(vec![VariantValue::from(json!(0u64)), VariantValue::from(json!(18446744073709551615u64))]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant", Float32Type::new_impl())?,
            ScalarFunctionWithFieldTest {
                name: "cast-float32-to-variant-passed",
                columns: vec![ColumnWithField::new(
                    Series::from_data(vec![0.12345679f32, 12.34]),
                    DataField::new("dummy_1", Float32Type::new_impl()),
                )],
                expect: Series::from_data(vec![VariantValue::from(json!(0.12345679f32)), VariantValue::from(json!(12.34f32))]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant", Float64Type::new_impl())?,
            ScalarFunctionWithFieldTest {
                name: "cast-float64-to-variant-passed",
                columns: vec![ColumnWithField::new(
                    Series::from_data(vec![0.12345678912121212f64,
                                           12.345678912, ]),
                    DataField::new("dummy_1", Float64Type::new_impl()),
                )],
                expect: Series::from_data(vec![
                    VariantValue::from(json!(0.12345678912121212f64)),
                    VariantValue::from(json!(12.345678912f64)),
                ]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant", StringType::new_impl())?,
            ScalarFunctionWithFieldTest {
                name: "cast-string-to-variant-error",
                columns: vec![ColumnWithField::new(
                    Series::from_data(vec![
                        "abc",
                        "123",
                    ]),
                    DataField::new("dummy_1", StringType::new_impl()),
                )],
                expect: Arc::new(NullColumn::new(2)),
                error: "Expression type does not match column data type, expecting VARIANT but got String",
            },
        ),
    ];

    for (test_func, test) in tests {
        match test_func.eval(
            FunctionContext::default(),
            &test.columns,
            test.columns[0].column().len(),
        ) {
            Ok(v) => {
                let v = v.convert_full_column();

                assert_eq!(test.expect, v, "{}", test.name);
            }
            Err(cause) => {
                assert_eq!(test.error, cause.message(), "{}", test.name);
            }
        }
    }

    Ok(())
}

#[test]
fn test_variant_cast_function() -> Result<()> {
    let tests = vec![
        ("to_uint8", ScalarFunctionTest {
            name: "cast-variant-to-uint8-passed",
            columns: vec![Series::from_data(vec![
                VariantValue::from(json!(4u64)),
                VariantValue::from(json!(3u64)),
                VariantValue::from(json!("2")),
                VariantValue::from(json!("4")),
            ])],
            expect: Series::from_data(vec![4u8, 3, 2, 4]),
            error: "",
        }),
        ("to_uint16", ScalarFunctionTest {
            name: "cast-variant-to-uint16-passed",
            columns: vec![Series::from_data(vec![
                VariantValue::from(json!(4u64)),
                VariantValue::from(json!(3u64)),
                VariantValue::from(json!("2")),
                VariantValue::from(json!("4")),
            ])],
            expect: Series::from_data(vec![4u16, 3, 2, 4]),
            error: "",
        }),
        ("to_uint32", ScalarFunctionTest {
            name: "cast-variant-to-uint32-passed",
            columns: vec![Series::from_data(vec![
                VariantValue::from(json!(4u64)),
                VariantValue::from(json!(3u64)),
                VariantValue::from(json!("2")),
                VariantValue::from(json!("4")),
            ])],
            expect: Series::from_data(vec![4u32, 3, 2, 4]),
            error: "",
        }),
        ("to_uint64", ScalarFunctionTest {
            name: "cast-variant-to-uint64-passed",
            columns: vec![Series::from_data(vec![
                VariantValue::from(json!(4u64)),
                VariantValue::from(json!(3u64)),
                VariantValue::from(json!("2")),
                VariantValue::from(json!("4")),
            ])],
            expect: Series::from_data(vec![4u64, 3, 2, 4]),
            error: "",
        }),
        ("to_uint64", ScalarFunctionTest {
            name: "cast-variant-to-uint64-error",
            columns: vec![Series::from_data(vec![
                VariantValue::from(json!("X4")),
                VariantValue::from(json!(3u64)),
                VariantValue::from(json!("2")),
                VariantValue::from(json!("4")),
            ])],
            expect: Series::from_data(vec![4u64, 3, 2, 4]),
            error: "Cast error happens in casting from Variant to UInt64",
        }),
        ("to_int8", ScalarFunctionTest {
            name: "cast-variant-to-int8-passed",
            columns: vec![Series::from_data(vec![
                VariantValue::from(json!(4i64)),
                VariantValue::from(json!(-3i64)),
                VariantValue::from(json!("2")),
                VariantValue::from(json!("-4")),
            ])],
            expect: Series::from_data(vec![4i8, -3, 2, -4]),
            error: "",
        }),
        ("to_int16", ScalarFunctionTest {
            name: "cast-variant-to-int16-passed",
            columns: vec![Series::from_data(vec![
                VariantValue::from(json!(4i64)),
                VariantValue::from(json!(-3i64)),
                VariantValue::from(json!("2")),
                VariantValue::from(json!("-4")),
            ])],
            expect: Series::from_data(vec![4i16, -3, 2, -4]),
            error: "",
        }),
        ("to_int32", ScalarFunctionTest {
            name: "cast-variant-to-int32-passed",
            columns: vec![Series::from_data(vec![
                VariantValue::from(json!(4i64)),
                VariantValue::from(json!(-3i64)),
                VariantValue::from(json!("2")),
                VariantValue::from(json!("-4")),
            ])],
            expect: Series::from_data(vec![4i32, -3, 2, -4]),
            error: "",
        }),
        ("to_int64", ScalarFunctionTest {
            name: "cast-variant-to-int64-passed",
            columns: vec![Series::from_data(vec![
                VariantValue::from(json!(4i64)),
                VariantValue::from(json!(-3i64)),
                VariantValue::from(json!("2")),
                VariantValue::from(json!("-4")),
            ])],
            expect: Series::from_data(vec![4i64, -3, 2, -4]),
            error: "",
        }),
        ("to_int64", ScalarFunctionTest {
            name: "cast-variant-to-int64-error",
            columns: vec![Series::from_data(vec![
                VariantValue::from(json!("X4")),
                VariantValue::from(json!(-3i64)),
                VariantValue::from(json!("2")),
                VariantValue::from(json!("-4")),
            ])],
            expect: Series::from_data(vec![4i64, -3, 2, -4]),
            error: "Cast error happens in casting from Variant to Int64",
        }),
        ("to_float32", ScalarFunctionTest {
            name: "cast-variant-to-float32-passed",
            columns: vec![Series::from_data(vec![
                VariantValue::from(json!(1.2f64)),
                VariantValue::from(json!(-1.3f64)),
                VariantValue::from(json!("2.1")),
                VariantValue::from(json!("-4.2")),
            ])],
            expect: Series::from_data(vec![1.2f32, -1.3, 2.1, -4.2]),
            error: "",
        }),
        ("to_float32", ScalarFunctionTest {
            name: "cast-variant-to-float32-error",
            columns: vec![Series::from_data(vec![
                VariantValue::from(json!("X4")),
                VariantValue::from(json!(-1.3f64)),
                VariantValue::from(json!("2.1")),
                VariantValue::from(json!("-4.2")),
            ])],
            expect: Series::from_data(vec![1.2f32, -1.3, 2.1, -4.2]),
            error: "Cast error happens in casting from Variant to Float32",
        }),
        ("to_float64", ScalarFunctionTest {
            name: "cast-variant-to-float64-passed",
            columns: vec![Series::from_data(vec![
                VariantValue::from(json!(1.2f64)),
                VariantValue::from(json!(-1.3f64)),
                VariantValue::from(json!("2.1")),
                VariantValue::from(json!("-4.2")),
            ])],
            expect: Series::from_data(vec![1.2f64, -1.3, 2.1, -4.2]),
            error: "",
        }),
        ("to_float64", ScalarFunctionTest {
            name: "cast-variant-to-float64-error",
            columns: vec![Series::from_data(vec![
                VariantValue::from(json!("X4")),
                VariantValue::from(json!(-1.3f64)),
                VariantValue::from(json!("2.1")),
                VariantValue::from(json!("-4.2")),
            ])],
            expect: Series::from_data(vec![1.2f64, -1.3, 2.1, -4.2]),
            error: "Cast error happens in casting from Variant to Float64",
        }),
        ("to_boolean", ScalarFunctionTest {
            name: "cast-variant-to-boolean-passed",
            columns: vec![Series::from_data(vec![
                VariantValue::from(json!(true)),
                VariantValue::from(json!(false)),
                VariantValue::from(json!("true")),
                VariantValue::from(json!("false")),
            ])],
            expect: Series::from_data(vec![true, false, true, false]),
            error: "",
        }),
        ("to_boolean", ScalarFunctionTest {
            name: "cast-variant-to-boolean-error",
            columns: vec![Series::from_data(vec![
                VariantValue::from(json!(1)),
                VariantValue::from(json!("test")),
                VariantValue::from(json!(true)),
                VariantValue::from(json!(false)),
            ])],
            expect: Series::from_data(vec![true, false, true, false]),
            error: "Cast error happens in casting from Variant to Boolean",
        }),
        ("to_date", ScalarFunctionTest {
            name: "cast-variant-to-date-passed",
            columns: vec![Series::from_data(vec![
                VariantValue::from(json!("2021-03-05")),
                VariantValue::from(json!("2021-10-24")),
            ])],
            expect: Series::from_data(vec![18691i32, 18924]),
            error: "",
        }),
        ("to_date", ScalarFunctionTest {
            name: "cast-variant-to-date-error",
            columns: vec![Series::from_data(vec![
                VariantValue::from(json!("a2021-03-05")),
                VariantValue::from(json!("2021-10-24")),
            ])],
            expect: Series::from_data(vec![18691i32, 18924]),
            error: "Cast error happens in casting from Variant to Date",
        }),
        ("to_timestamp", ScalarFunctionTest {
            name: "cast-variant-to-datetime-passed",
            columns: vec![Series::from_data(vec![
                VariantValue::from(json!("2021-03-05 01:01:01")),
                VariantValue::from(json!("2021-10-24 10:10:10")),
            ])],
            expect: Series::from_data(vec![1614906061000000i64, 1635070210000000]),
            error: "",
        }),
        ("to_timestamp", ScalarFunctionTest {
            name: "cast-variant-to-datetime-error",
            columns: vec![Series::from_data(vec![
                VariantValue::from(json!("a2021-03-05 01:01:01")),
                VariantValue::from(json!("2021-10-24 10:10:10")),
            ])],
            expect: Series::from_data(vec![1614906061000000i64, 1635070210000000]),
            error: "Cast error happens in casting from Variant to Timestamp(6)",
        }),
    ];

    for (op, test) in tests {
        test_scalar_functions(op, &[test])?;
    }

    let tests = vec![
        (
            CastFunction::create("cast", "array", VariantType::new_impl())?,
            ScalarFunctionWithFieldTest {
                name: "cast-variant-to-array-passed",
                columns: vec![ColumnWithField::new(
                    Series::from_data(vec![
                        VariantValue::from(json!(1_i32)),
                        VariantValue::from(json!([1_i32, 2, 3])),
                        VariantValue::from(json!(["a", "b", "c"])),
                    ]),
                    DataField::new("dummy_1", VariantType::new_impl()),
                )],
                expect: Series::from_data(vec![
                    VariantValue::from(json!([1_i32])),
                    VariantValue::from(json!([1_i32, 2, 3])),
                    VariantValue::from(json!(["a", "b", "c"])),
                ]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "object", VariantType::new_impl())?,
            ScalarFunctionWithFieldTest {
                name: "cast-variant-to-object-passed",
                columns: vec![ColumnWithField::new(
                    Series::from_data(vec![
                        VariantValue::from(json!({"a":1_i32})),
                        VariantValue::from(json!({"k":"v"})),
                    ]),
                    DataField::new("dummy_1", VariantType::new_impl()),
                )],
                expect: Series::from_data(vec![
                    VariantValue::from(json!({"a":1_i32})),
                    VariantValue::from(json!({"k":"v"})),
                ]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "object", VariantType::new_impl())?,
            ScalarFunctionWithFieldTest {
                name: "cast-variant-to-object-error",
                columns: vec![ColumnWithField::new(
                    Series::from_data(vec![
                        VariantValue::from(json!(["a", "b", "c"])),
                        VariantValue::from(json!("abc")),
                    ]),
                    DataField::new("dummy_1", VariantType::new_impl()),
                )],
                expect: Arc::new(NullColumn::new(2)),
                error: "Failed to cast variant value [\"a\",\"b\",\"c\"] to OBJECT",
            },
        ),
    ];

    for (test_func, test) in tests {
        match test_func.eval(
            FunctionContext::default(),
            &test.columns,
            test.columns[0].column().len(),
        ) {
            Ok(v) => {
                let v = v.convert_full_column();

                assert_eq!(test.expect, v, "{}", test.name);
            }
            Err(cause) => {
                assert_eq!(test.error, cause.message(), "{}", test.name);
            }
        }
    }

    Ok(())
}

#[test]
fn test_binary_contains() {
    fn contains(a: &'_ [u8], b: &'_ [u8], _ctx: &mut EvalContext) -> bool {
        a.windows(b.len()).any(|window| window == b)
    }

    for _ in 0..10 {
        let l = Series::from_data(vec!["11", "22", "33"]);
        let r = Series::from_data(vec!["1", "2", "43"]);
        let expected = Series::from_data(vec![true, true, false]);
        let result = scalar_binary_op::<Vec<u8>, Vec<u8>, bool, _>(
            &l,
            &r,
            contains,
            &mut EvalContext::default(),
        )
        .unwrap();
        let result = Arc::new(result) as ColumnRef;
        assert!(result == expected);
    }
}

#[test]
fn test_binary_simd_op() {
    {
        let l = Series::from_data(vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let r = Series::from_data(vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let expected = Series::from_data(vec![2u8, 4, 6, 8, 10, 12, 14, 16, 18, 20]);
        let result = binary_simd_op::<u8, u8, _, 8>(&l, &r, |a, b| a + b).unwrap();
        let result = Arc::new(result) as ColumnRef;
        assert!(result == expected);
    }

    {
        let l = Series::from_data(vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let r = Arc::new(ConstColumn::new(Series::from_data(vec![1u8]), 10)) as ColumnRef;
        let expected = Series::from_data(vec![2u8, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
        let result = binary_simd_op::<u8, u8, _, 8>(&l, &r, |a, b| a + b).unwrap();
        let result = Arc::new(result) as ColumnRef;
        assert!(result == expected);
    }
}

#[test]
fn test_unary_size() {
    fn len_func(l: &[u8], _ctx: &mut EvalContext) -> i32 {
        l.len() as i32
    }

    let mut ctx = EvalContext::default();

    let l = Series::from_data(vec!["11", "22", "333"]);
    let expected = Series::from_data(vec![2i32, 2, 3]);
    let result = scalar_unary_op::<Vec<u8>, i32, _>(&l, len_func, &mut ctx).unwrap();
    let result = Arc::new(result) as ColumnRef;
    assert!(result == expected);
}
