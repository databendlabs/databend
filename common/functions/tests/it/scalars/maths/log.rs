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

use std::f64::consts::E;

use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::scalars::*;

use crate::scalars::scalar_function2_test::test_scalar_functions;
use crate::scalars::scalar_function2_test::ScalarFunctionTest;

#[test]
fn test_log_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "log-with-literal",
            columns: vec![Series::from_data([10]), Series::from_data([100])],
            expect: Series::from_data(vec![2f64]),
            error: "",
        },
        ScalarFunctionTest {
            name: "log-with-series",
            columns: vec![
                Series::from_data([10, 10, 10]),
                Series::from_data([100, 1000, 10000]),
            ],
            expect: Series::from_data([2_f64, 2.9999999999999996, 4_f64]),
            error: "",
        },
        ScalarFunctionTest {
            name: "log-with-one-arg",
            columns: vec![Series::from_data([E, E, E])],
            expect: Series::from_data([1_f64, 1_f64, 1_f64]),
            error: "",
        },
        ScalarFunctionTest {
            name: "log-with-null",
            columns: vec![
                Series::from_data([None, Some(10_f64), Some(10_f64)]),
                Series::from_data([Some(10_f64), None, Some(10_f64)]),
            ],
            expect: Series::from_data([None, None, Some(1_f64)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "log-with-null2",
            columns: vec![
                Series::from_data(vec![
                    Option::<f64>::None,
                    Option::<f64>::None,
                    Option::<f64>::None,
                ]),
                Series::from_data([Some(10_f64), None, Some(10_f64)]),
            ],
            expect: Series::from_data(vec![
                Option::<f64>::None,
                Option::<f64>::None,
                Option::<f64>::None,
            ]),
            error: "",
        },
        ScalarFunctionTest {
            name: "log-with-base-constant",
            columns: vec![
                Series::from_data(vec![2, 2, 2]),
                Series::from_data([1, 2, 4]),
            ],
            expect: Series::from_data([0_f64, 1_f64, 2.0]),
            error: "",
        },
        ScalarFunctionTest {
            name: "log-with-num-constant",
            columns: vec![Series::from_data([2, 4]), Series::from_data(vec![2, 2])],
            expect: Series::from_data([1_f64, 0.5]),
            error: "",
        },
    ];

    test_scalar_functions(LogFunction::try_create("log")?, &tests)
}

#[test]
fn test_ln_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "ln on series",
            columns: vec![Series::from_data([Some(E), None])],
            expect: Series::from_data([Some(1_f64), None]),
            error: "",
        },
        ScalarFunctionTest {
            name: "ln on literal",
            columns: vec![Series::from_data([E])],
            expect: Series::from_data(vec![1f64]),
            error: "",
        },
    ];

    test_scalar_functions(LnFunction::try_create("ln")?, &tests)
}

#[test]
fn test_log2_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "log2 on literal",
        columns: vec![Series::from_data([2_f64])],
        expect: Series::from_data(vec![1f64]),
        error: "",
    }];

    test_scalar_functions(Log2Function::try_create("log2")?, &tests)
}

#[test]
fn test_log10_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "log10 on literal",
        columns: vec![Series::from_data([10_f64])],
        expect: Series::from_data(vec![1f64]),
        error: "",
    }];

    test_scalar_functions(Log10Function::try_create("log10")?, &tests)
}
