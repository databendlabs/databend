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
use common_functions::scalars::*;

use crate::scalars::scalar_function2_test::test_scalar_functions;
use crate::scalars::scalar_function2_test::ScalarFunctionTest;

#[test]
fn test_round_number_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "no second arg",
            columns: vec![Series::from_data([12345.6789])],
            expect: Series::from_data([12346.0]),
            error: "",
        },
        ScalarFunctionTest {
            name: "no second arg with null x",
            columns: vec![Series::from_data([Some(12345.6789), None, Some(77.77)])],
            expect: Series::from_data([Some(12346.0), None, Some(78.0)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is const with null x",
            columns: vec![
                Series::from_data([Some(12345.6789), None, Some(77.77)]),
                ConstColumn::new(Series::from_data(vec![0i64]), 3).arc(),
            ],
            expect: Series::from_data([Some(12346.0), None, Some(78.0)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is 0",
            columns: vec![Series::from_data([12345.6789]), Series::from_data([0])],
            expect: Series::from_data([12346.0]),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is 2",
            columns: vec![Series::from_data([12345.6789]), Series::from_data([2])],
            expect: Series::from_data([12345.68]),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is -2",
            columns: vec![Series::from_data([12345.6789]), Series::from_data([-2])],
            expect: Series::from_data([12300.0]),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is const 0",
            columns: vec![
                Series::from_data([12345.6789]),
                ConstColumn::new(Series::from_data(vec![0i64]), 1).arc(),
            ],
            expect: Series::from_data([12346.0]),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is const 2",
            columns: vec![
                Series::from_data([12345.6789]),
                ConstColumn::new(Series::from_data(vec![2i64]), 1).arc(),
            ],
            expect: Series::from_data([12345.68]),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is const -2",
            columns: vec![
                Series::from_data([12345.6789]),
                ConstColumn::new(Series::from_data(vec![-2i64]), 1).arc(),
            ],
            expect: Series::from_data([12300.0]),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is 35",
            columns: vec![
                Series::from_data([
                    0.123_456_789_012_345_68_f64,
                    8888888888888888888888888888888888888888.0_f64,
                ]),
                Series::from_data([35, 35]),
            ],
            expect: Series::from_data([
                0.123_456_789_012_345_68_f64,
                8888888888888888888888888888888888888888.0_f64,
            ]),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is const 35",
            columns: vec![
                Series::from_data([
                    0.123_456_789_012_345_68_f64,
                    8888888888888888888888888888888888888888.0_f64,
                ]),
                ConstColumn::new(Series::from_data(vec![35i64]), 2).arc(),
            ],
            expect: Series::from_data([
                0.123_456_789_012_345_68_f64,
                8888888888888888888888888888888888888888.0_f64,
            ]),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is -35",
            columns: vec![
                Series::from_data([
                    0.123_456_789_012_345_68_f64,
                    8888888888888888888888888888888888888888.0_f64,
                ]),
                Series::from_data([-35, -35]),
            ],
            expect: Series::from_data([0.0, 8888888889000000000000000000000000000000.0_f64]),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is const -35",
            columns: vec![
                Series::from_data([
                    0.123_456_789_012_345_68_f64,
                    8888888888888888888888888888888888888888.0_f64,
                ]),
                ConstColumn::new(Series::from_data(vec![Some(-35i64)]), 2).arc(),
            ],
            expect: Series::from_data([
                Some(0.0),
                Some(8888888889000000000000000000000000000000.0_f64),
            ]),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is NULL",
            columns: vec![
                Series::from_data([12345.6789, 12345.6789]),
                Series::from_data([Some(0), None]),
            ],
            expect: Series::from_data([Some(12346.0), None]),
            error: "",
        },
    ];

    test_scalar_functions(RoundNumberFunction::try_create("round")?, &tests)
}

#[test]
fn test_trunc_number_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "first arg is series, second arg is NULL",
            columns: vec![
                Series::from_data([Some(11.11), None, Some(33.33)]),
                Series::from_data(vec![
                    Option::<i64>::None,
                    Option::<i64>::None,
                    Option::<i64>::None,
                ]),
            ],
            expect: Series::from_data([None::<f64>, None, None]),
            error: "",
        },
        ScalarFunctionTest {
            name: "first arg is series, second arg is 0 const",
            columns: vec![
                Series::from_data([Some(11.11), None, Some(33.33)]),
                Series::from_data(vec![None, Some(1i64), Some(0i64)]),
            ],
            expect: Series::from_data([None, None, Some(33.0)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "first arg is series, second arg is positive const",
            columns: vec![
                Series::from_data([Some(11.11), None, Some(33.33)]),
                ConstColumn::new(Series::from_data(vec![1i64]), 3).arc(),
            ],
            expect: Series::from_data([Some(11.1), None, Some(33.3)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "first arg is series, second arg is negative const",
            columns: vec![
                Series::from_data([Some(11.11), None, Some(33.33)]),
                ConstColumn::new(Series::from_data(vec![-1i64]), 3).arc(),
            ],
            expect: Series::from_data([Some(10.0), None, Some(30.0)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "first arg is const, second is series",
            columns: vec![
                ConstColumn::new(Series::from_data(vec![11.11f64]), 4).arc(),
                Series::from_data([None, Some(-1), Some(0), Some(1)]),
            ],
            expect: Series::from_data([None, Some(10.0), Some(11.0), Some(11.1)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "both arg are series",
            columns: vec![
                Series::from_data([
                    None,
                    None,
                    Some(11.11),
                    Some(22.22),
                    Some(33.33),
                    Some(44.44),
                ]),
                Series::from_data([None, Some(1), None, Some(0), Some(-1), Some(1)]),
            ],
            expect: Series::from_data([None, None, None, Some(22.0), Some(30.0), Some(44.4)]),
            error: "",
        },
    ];

    test_scalar_functions(TruncNumberFunction::try_create("trunc")?, &tests)
}
