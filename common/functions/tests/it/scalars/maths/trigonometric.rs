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

use std::f64::consts::FRAC_PI_2;
use std::f64::consts::FRAC_PI_4;
use std::f64::consts::PI;

use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::scalars::*;

use crate::scalars::scalar_function2_test::test_scalar_functions;
use crate::scalars::scalar_function2_test::ScalarFunctionTest;

#[test]
fn test_trigonometric_sin_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "sin-u8-passed",
            columns: vec![Series::from_data(vec![0_u8, 1, 3])],
            expect: Series::from_data(vec![0f64, 0.8414709848078965, 0.1411200080598672]),
            error: "",
        },
        ScalarFunctionTest {
            name: "sin-u16-passed",
            columns: vec![Series::from_data(vec![0_u16, 1, 3])],
            expect: Series::from_data(vec![0f64, 0.8414709848078965, 0.1411200080598672]),
            error: "",
        },
        ScalarFunctionTest {
            name: "sin-u32-passed",
            columns: vec![Series::from_data(vec![0_u32, 1, 3])],
            expect: Series::from_data(vec![0f64, 0.8414709848078965, 0.1411200080598672]),
            error: "",
        },
        ScalarFunctionTest {
            name: "sin-u64-passed",
            columns: vec![Series::from_data(vec![0_u64, 1, 3])],
            expect: Series::from_data(vec![0f64, 0.8414709848078965, 0.1411200080598672]),
            error: "",
        },
        ScalarFunctionTest {
            name: "sin-f64-passed",
            columns: vec![Series::from_data(vec![0_f64, 1.0, 3.0])],
            expect: Series::from_data(vec![0f64, 0.8414709848078965, 0.1411200080598672]),
            error: "",
        },
    ];

    test_scalar_functions(TrigonometricSinFunction::try_create_func("sin")?, &tests)
}

#[test]
fn test_trigonometric_cos_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "cos-f64-passed",
        columns: vec![Series::from_data(vec![0_f64, 1.0, 3.0])],
        expect: Series::from_data(vec![1f64, 0.5403023058681398, -0.9899924966004454]),
        error: "",
    }];

    test_scalar_functions(TrigonometricCosFunction::try_create_func("cos")?, &tests)
}

#[test]
fn test_trigonometric_tan_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "tan-pi4-passed",
        columns: vec![Series::from_data(vec![0_f64, PI / 4.0])],
        expect: Series::from_data(vec![0f64, 0.9999999999999999]),
        error: "",
    }];

    test_scalar_functions(TrigonometricTanFunction::try_create_func("tan")?, &tests)
}

#[test]
fn test_trigonometric_cot_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "cot-pi4-passed",
            columns: vec![Series::from_data(vec![PI / 4.0])],
            expect: Series::from_data(vec![1.0000000000000002]),
            error: "",
        },
        ScalarFunctionTest {
            name: "cot-0-passed",
            columns: vec![Series::from_data(vec![0_f64])],
            expect: Series::from_data(vec![f64::INFINITY]),
            error: "",
        },
    ];

    test_scalar_functions(TrigonometricCotFunction::try_create_func("cot")?, &tests)
}

#[test]
fn test_trigonometric_asin_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "asin-passed",
        columns: vec![Series::from_data(vec![0.2_f64])],
        expect: ConstColumn::new(Series::from_data(vec![0.2013579207903308_f64]), 1).arc(),
        error: "",
    }];

    test_scalar_functions(TrigonometricAsinFunction::try_create_func("asin")?, &tests)
}

#[test]
fn test_trigonometric_acos_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "acos-passed",
        columns: vec![Series::from_data(vec![1])],
        expect: ConstColumn::new(Series::from_data(vec![0f64]), 1).arc(),
        error: "",
    }];

    test_scalar_functions(TrigonometricAcosFunction::try_create_func("acos")?, &tests)
}

#[test]
fn test_trigonometric_atan_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "atan-passed",
            columns: vec![Series::from_data(vec![1, -1])],
            expect: Series::from_data(vec![FRAC_PI_4, -FRAC_PI_4]),
            error: "",
        },
        ScalarFunctionTest {
            name: "atan-passed",
            columns: vec![
                Series::from_data(vec![-2_f64, PI]),
                Series::from_data(vec![2, 0]),
            ],
            expect: Series::from_data(vec![-FRAC_PI_4, FRAC_PI_2]),
            error: "",
        },
    ];

    test_scalar_functions(TrigonometricAtanFunction::try_create_func("atan")?, &tests)
}

#[test]
fn test_trigonometric_atan2_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "atan2-passed",
            columns: vec![
                Series::from_data(vec![-2_f64, PI]),
                Series::from_data(vec![2, 0]),
            ],
            expect: Series::from_data(vec![-FRAC_PI_4, FRAC_PI_2]),
            error: "",
        },
        ScalarFunctionTest {
            name: "atan2-y-constant-passed",
            columns: vec![
                ConstColumn::new(Series::from_data(vec![2]), 2).arc(),
                Series::from_data(vec![0_f64, 2.0]),
            ],
            expect: Series::from_data(vec![FRAC_PI_2, FRAC_PI_4]),
            error: "",
        },
        ScalarFunctionTest {
            name: "atan2-x-constant-passed",
            columns: vec![
                Series::from_data(vec![-2_f64, 2.0]),
                ConstColumn::new(Series::from_data(vec![2]), 2).arc(),
            ],
            expect: Series::from_data(vec![-FRAC_PI_4, FRAC_PI_4]),
            error: "",
        },
    ];

    test_scalar_functions(
        TrigonometricAtan2Function::try_create_func("atan2")?,
        &tests,
    )
}
