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

use super::scalar_function2_test::ScalarFunctionWithFieldTest;
use crate::scalars::scalar_function2_test::test_scalar_functions;
use crate::scalars::scalar_function2_test::test_scalar_functions_with_type;
use crate::scalars::scalar_function2_test::ScalarFunctionTest;

#[test]
fn test_cast_function() -> Result<()> {
    let tests = vec![
        (CastFunction::create("cast", "int8")?, ScalarFunctionTest {
            name: "cast-int64-to-int8-passed",
            columns: vec![Series::from_data(vec![4i64, 3, 2, 4])],
            expect: Series::from_data(vec![4i8, 3, 2, 4]),
            error: "",
        }),
        (CastFunction::create("cast", "int8")?, ScalarFunctionTest {
            name: "cast-string-to-int8-passed",
            columns: vec![Series::from_data(vec!["4", "3", "2", "4"])],
            expect: Series::from_data(vec![4i8, 3, 2, 4]),
            error: "",
        }),
        (CastFunction::create("cast", "int16")?, ScalarFunctionTest {
            name: "cast-string-to-int16-passed",
            columns: vec![Series::from_data(vec!["4", "3", "2", "4"])],
            expect: Series::from_data(vec![4i16, 3, 2, 4]),
            error: "",
        }),
        (CastFunction::create("cast", "int32")?, ScalarFunctionTest {
            name: "cast-string-to-int32-passed",
            columns: vec![Series::from_data(vec!["4", "3", "2", "4"])],
            expect: Series::from_data(vec![4i32, 3, 2, 4]),
            error: "",
        }),
        (CastFunction::create("cast", "int32")?, ScalarFunctionTest {
            name: "cast-string-to-int32-error-passed",
            columns: vec![Series::from_data(vec!["X4", "3", "2", "4"])],
            expect: Series::from_data(vec![4i32, 3, 2, 4]),
            error: "Cast error happens in casting from String to Int32",
        }),
        (CastFunction::create("cast", "int32")?, ScalarFunctionTest {
            name: "cast-string-to-int32-error-as_null-passed",
            columns: vec![Series::from_data(vec!["X4", "3", "2", "4"])],
            expect: Series::from_data(vec![Some(0i32), Some(3), Some(2), Some(4)]),
            error: "Cast error happens in casting from String to Int32",
        }),
        (CastFunction::create("cast", "int64")?, ScalarFunctionTest {
            name: "cast-string-to-int64-passed",
            columns: vec![Series::from_data(vec!["4", "3", "2", "4"])],
            expect: Series::from_data(vec![4i64, 3, 2, 4]),
            error: "",
        }),
        (
            CastFunction::create("cast", "date16")?,
            ScalarFunctionTest {
                name: "cast-string-to-date16-passed",
                columns: vec![Series::from_data(vec!["2021-03-05", "2021-10-24"])],
                expect: Series::from_data(vec![18691u16, 18924]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "date32")?,
            ScalarFunctionTest {
                name: "cast-string-to-date32-passed",
                columns: vec![Series::from_data(vec!["2021-03-05", "2021-10-24"])],
                expect: Series::from_data(vec![18691i32, 18924]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "datetime32")?,
            ScalarFunctionTest {
                name: "cast-string-to-datetime32-passed",
                columns: vec![Series::from_data(vec![
                    "2021-03-05 01:01:01",
                    "2021-10-24 10:10:10",
                ])],
                expect: Series::from_data(vec![1614906061u32, 1635070210]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant")?,
            ScalarFunctionTest {
                name: "cast-bool-to-variant-passed",
                columns: vec![Series::from_data(vec![true, false])],
                expect: Series::from_data(vec![json!(true), json!(false)]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant")?,
            ScalarFunctionTest {
                name: "cast-int8-to-variant-passed",
                columns: vec![Series::from_data(vec![-128i8, 127])],
                expect: Series::from_data(vec![json!(-128i8), json!(127i8)]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant")?,
            ScalarFunctionTest {
                name: "cast-int16-to-variant-passed",
                columns: vec![Series::from_data(vec![-32768i16, 32767])],
                expect: Series::from_data(vec![json!(-32768i16), json!(32767i16)]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant")?,
            ScalarFunctionTest {
                name: "cast-int32-to-variant-passed",
                columns: vec![Series::from_data(vec![-2147483648i32, 2147483647])],
                expect: Series::from_data(vec![json!(-2147483648i32), json!(2147483647i32)]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant")?,
            ScalarFunctionTest {
                name: "cast-int64-to-variant-passed",
                columns: vec![Series::from_data(vec![
                    -9223372036854775808i64,
                    9223372036854775807,
                ])],
                expect: Series::from_data(vec![
                    json!(-9223372036854775808i64),
                    json!(9223372036854775807i64),
                ]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant")?,
            ScalarFunctionTest {
                name: "cast-uint8-to-variant-passed",
                columns: vec![Series::from_data(vec![0u8, 255])],
                expect: Series::from_data(vec![json!(0u8), json!(255u8)]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant")?,
            ScalarFunctionTest {
                name: "cast-uint16-to-variant-passed",
                columns: vec![Series::from_data(vec![0u16, 65535])],
                expect: Series::from_data(vec![json!(0u16), json!(65535u16)]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant")?,
            ScalarFunctionTest {
                name: "cast-uint32-to-variant-passed",
                columns: vec![Series::from_data(vec![0u32, 4294967295])],
                expect: Series::from_data(vec![json!(0u32), json!(4294967295u32)]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant")?,
            ScalarFunctionTest {
                name: "cast-uint64-to-variant-passed",
                columns: vec![Series::from_data(vec![0u64, 18446744073709551615])],
                expect: Series::from_data(vec![json!(0u64), json!(18446744073709551615u64)]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant")?,
            ScalarFunctionTest {
                name: "cast-float32-to-variant-passed",
                columns: vec![Series::from_data(vec![0.12345679f32, 12.34])],
                expect: Series::from_data(vec![json!(0.12345679f32), json!(12.34f32)]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant")?,
            ScalarFunctionTest {
                name: "cast-float64-to-variant-passed",
                columns: vec![Series::from_data(vec![
                    0.12345678912121212f64,
                    12.345678912,
                ])],
                expect: Series::from_data(vec![
                    json!(0.12345678912121212f64),
                    json!(12.345678912f64),
                ]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant")?,
            ScalarFunctionTest {
                name: "cast-string-to-variant-error",
                columns: vec![Series::from_data(vec![
                    "abc",
                    "123",
                ])],
                expect: Arc::new(NullColumn::new(2)),
                error: "Expression type does not match column data type, expecting VARIANT but got String",
            },
        ),
    ];

    for (test_func, test) in tests {
        test_scalar_functions(test_func, &[test], false)?;
    }

    Ok(())
}

#[test]
fn test_datetime_cast_function() -> Result<()> {
    let tests = vec![
        (
            CastFunction::create("cast", "string")?,
            ScalarFunctionWithFieldTest {
                name: "cast-date32-to-string-passed",
                columns: vec![ColumnWithField::new(
                    Series::from_data(vec![18691i32, 18924]),
                    DataField::new("dummy_1", Date32Type::arc()),
                )],
                expect: Series::from_data(vec!["2021-03-05", "2021-10-24"]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "string")?,
            ScalarFunctionWithFieldTest {
                name: "cast-datetime-to-string-passed",
                columns: vec![ColumnWithField::new(
                    Series::from_data(vec![1614906061u32, 1635070210]),
                    DataField::new("dummy_1", DateTime32Type::arc(None)),
                )],
                expect: Series::from_data(vec!["2021-03-05 01:01:01", "2021-10-24 10:10:10"]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "variant")?,
            ScalarFunctionWithFieldTest {
                name: "cast-date32-to-variant-error",
                columns: vec![ColumnWithField::new(
                    Series::from_data(vec![18691i32, 18924]),
                    DataField::new("dummy_1", Date32Type::arc()),
                )],
                expect: Arc::new(NullColumn::new(2)),
                error: "Expression type does not match column data type, expecting VARIANT but got Date32",
            },
        ),
    ];

    for (test_func, test) in tests {
        test_scalar_functions_with_type(test_func, &[test], false)?;
    }

    Ok(())
}

#[test]
fn test_variant_cast_function() -> Result<()> {
    let tests = vec![
        (
            CastFunction::create("cast", "array")?,
            ScalarFunctionWithFieldTest {
                name: "cast-variant-to-array-passed",
                columns: vec![ColumnWithField::new(
                    Series::from_data(vec![
                        json!(1_i32),
                        json!([1_i32, 2, 3]),
                        json!(["a", "b", "c"]),
                    ]),
                    DataField::new("dummy_1", VariantType::arc()),
                )],
                expect: Series::from_data(vec![
                    json!([1_i32]),
                    json!([1_i32, 2, 3]),
                    json!(["a", "b", "c"]),
                ]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "object")?,
            ScalarFunctionWithFieldTest {
                name: "cast-variant-to-object-passed",
                columns: vec![ColumnWithField::new(
                    Series::from_data(vec![json!({"a":1_i32}), json!({"k":"v"})]),
                    DataField::new("dummy_1", VariantType::arc()),
                )],
                expect: Series::from_data(vec![json!({"a":1_i32}), json!({"k":"v"})]),
                error: "",
            },
        ),
        (
            CastFunction::create("cast", "object")?,
            ScalarFunctionWithFieldTest {
                name: "cast-variant-to-object-error",
                columns: vec![ColumnWithField::new(
                    Series::from_data(vec![json!(["a", "b", "c"]), json!("abc")]),
                    DataField::new("dummy_1", VariantType::arc()),
                )],
                expect: Arc::new(NullColumn::new(2)),
                error: "Failed to cast variant value [\"a\",\"b\",\"c\"] to OBJECT",
            },
        ),
    ];

    for (test_func, test) in tests {
        test_scalar_functions_with_type(test_func, &[test], false)?;
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
