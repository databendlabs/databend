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

use crate::scalars::scalar_function_test::test_scalar_functions;
use crate::scalars::scalar_function_test::ScalarFunctionTest;

#[test]
fn test_trigonometric_sin_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "sin-u8-passed",
            nullable: false,
            columns: vec![Series::new(vec![0_u8, 1, 3]).into()],
            expect: Series::new(vec![0f64, 0.8414709848078965, 0.1411200080598672]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "sin-u16-passed",
            nullable: false,
            columns: vec![Series::new(vec![0_u16, 1, 3]).into()],
            expect: Series::new(vec![0f64, 0.8414709848078965, 0.1411200080598672]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "sin-u32-passed",
            nullable: false,
            columns: vec![Series::new(vec![0_u32, 1, 3]).into()],
            expect: Series::new(vec![0f64, 0.8414709848078965, 0.1411200080598672]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "sin-u64-passed",
            nullable: false,
            columns: vec![Series::new(vec![0_u64, 1, 3]).into()],
            expect: Series::new(vec![0f64, 0.8414709848078965, 0.1411200080598672]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "sin-str-passed",
            nullable: false,
            columns: vec![Series::new(vec!["0", "1", "3"]).into()],
            expect: Series::new(vec![0f64, 0.8414709848078965, 0.1411200080598672]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "sin-f64-passed",
            nullable: false,
            columns: vec![Series::new(vec![0_f64, 1.0, 3.0]).into()],
            expect: Series::new(vec![0f64, 0.8414709848078965, 0.1411200080598672]).into(),
            error: "",
        },
    ];

    test_scalar_functions(TrigonometricSinFunction::try_create_func("sin")?, &tests)
}

#[test]
fn test_trigonometric_cos_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "cos-f64-passed",
        nullable: false,
        columns: vec![Series::new(vec![0_f64, 1.0, 3.0]).into()],
        expect: Series::new(vec![1f64, 0.5403023058681398, -0.9899924966004454]).into(),
        error: "",
    }];

    test_scalar_functions(TrigonometricCosFunction::try_create_func("cos")?, &tests)
}

#[test]
fn test_trigonometric_tan_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "tan-pi4-passed",
        nullable: false,
        columns: vec![Series::new(vec![0_f64, PI / 4.0]).into()],
        expect: Series::new(vec![0f64, 0.9999999999999999]).into(),
        error: "",
    }];

    test_scalar_functions(TrigonometricTanFunction::try_create_func("tan")?, &tests)
}

#[test]
fn test_trigonometric_cot_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "cot-pi4-passed",
            nullable: false,
            columns: vec![Series::new(vec![PI / 4.0]).into()],
            expect: Series::new(vec![1.0000000000000002]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "cot-0-passed",
            nullable: false,
            columns: vec![Series::new(vec![0_f64]).into()],
            expect: Series::new(vec![f64::INFINITY]).into(),
            error: "",
        },
    ];

    test_scalar_functions(TrigonometricCotFunction::try_create_func("cot")?, &tests)
}

#[test]
fn test_trigonometric_asin_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "asin-passed",
        nullable: false,
        columns: vec![Series::new(vec![0.2_f64]).into()],
        expect: DataColumn::Constant(0.2013579207903308_f64.into(), 1),
        error: "",
    }];

    test_scalar_functions(TrigonometricAsinFunction::try_create_func("asin")?, &tests)
}

#[test]
fn test_trigonometric_acos_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "acos-passed",
        nullable: false,
        columns: vec![Series::new(vec![1]).into()],
        expect: DataColumn::Constant(0_f64.into(), 1),
        error: "",
    }];

    test_scalar_functions(TrigonometricAcosFunction::try_create_func("acos")?, &tests)
}

#[test]
fn test_trigonometric_atan_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "atan-passed",
            nullable: false,
            columns: vec![Series::new(vec![1, -1]).into()],
            expect: Series::new(vec![FRAC_PI_4, -FRAC_PI_4]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "atan-passed",
            nullable: false,
            columns: vec![
                Series::new(vec![-2_f64, PI]).into(),
                Series::new(vec![2, 0]).into(),
            ],
            expect: Series::new(vec![-FRAC_PI_4, FRAC_PI_2]).into(),
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
            nullable: false,
            columns: vec![
                Series::new(vec![-2_f64, PI]).into(),
                Series::new(vec![2, 0]).into(),
            ],
            expect: Series::new(vec![-FRAC_PI_4, FRAC_PI_2]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "atan2-y-constant-passed",
            nullable: false,
            columns: vec![
                DataColumn::Constant(2.into(), 2),
                Series::new(vec![0_f64, 2.0]).into(),
            ],
            expect: Series::new(vec![FRAC_PI_2, FRAC_PI_4]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "atan2-x-constant-passed",
            nullable: false,
            columns: vec![
                Series::new(vec![-2_f64, 2.0]).into(),
                DataColumn::Constant(2.into(), 2),
            ],
            expect: Series::new(vec![-FRAC_PI_4, FRAC_PI_4]).into(),
            error: "",
        },
    ];

    test_scalar_functions(
        TrigonometricAtan2Function::try_create_func("atan2")?,
        &tests,
    )
}
