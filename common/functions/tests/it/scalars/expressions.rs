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
    ];

    for (test_func, test) in tests {
        test_scalar_functions(test_func, &[test])?;
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
    ];

    for (test_func, test) in tests {
        test_scalar_functions_with_type(test_func, &[test])?;
    }

    Ok(())
}

#[test]
fn test_binary_contains() {
    //create two string columns
    struct Contains {}

    impl ScalarBinaryFunction<Vu8, Vu8, bool> for Contains {
        fn eval(&self, a: &'_ [u8], b: &'_ [u8], _ctx: &mut EvalContext) -> bool {
            a.windows(b.len()).any(|window| window == b)
        }
    }

    let binary_expression = ScalarBinaryExpression::<Vec<u8>, Vec<u8>, bool, _>::new(Contains {});

    for _ in 0..10 {
        let l = Series::from_data(vec!["11", "22", "33"]);
        let r = Series::from_data(vec!["1", "2", "43"]);
        let expected = Series::from_data(vec![true, true, false]);
        let result = binary_expression
            .eval(&l, &r, &mut EvalContext::default())
            .unwrap();
        let result = Arc::new(result) as ColumnRef;
        assert!(result == expected);
    }
}

#[test]
fn test_unary_size() {
    struct LenFunc {}

    impl ScalarUnaryFunction<Vu8, i32> for LenFunc {
        fn eval(&self, l: &[u8], _ctx: &mut EvalContext) -> i32 {
            l.len() as i32
        }
    }

    let mut ctx = EvalContext::default();

    let l = Series::from_data(vec!["11", "22", "333"]);
    let expected = Series::from_data(vec![2i32, 2, 3]);
    let unary_expression = ScalarUnaryExpression::<Vec<u8>, i32, _>::new(LenFunc {});
    let result = unary_expression.eval(&l, &mut ctx).unwrap();
    let result = Arc::new(result) as ColumnRef;
    assert!(result == expected);
}
