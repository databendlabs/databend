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

use crate::scalars::helpers as test_helpers;
use crate::scalars::scalar_function_test::{ScalarFunctionTest, test_scalar_functions};

#[test]
fn test_log_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "log-with-literal",
            nullable: false,
            columns: vec![Series::new([10]).into(), Series::new(["100"]).into()],
            expect: DataColumn::Constant(2_f64.into(), 1),
            error: "",
        },
        ScalarFunctionTest {
            name: "log-with-series",
            nullable: false,
            columns: vec![Series::new([10, 10, 10]).into(), Series::new(["100", "1000", "10000"]).into()],
            expect: Series::new([2_f64, 2.9999999999999996, 4_f64]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "log-with-one-arg",
            nullable: false,
            columns: vec![Series::new([E, E, E]).into()],
            expect: Series::new([1_f64, 1_f64, 1_f64]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "log-with-null",
            nullable: true,
            columns: vec![
                Series::new([None, Some(10_f64), Some(10_f64)]).into(),
                Series::new([Some(10_f64), None, Some(10_f64)]).into(),
            ],
            expect: Series::new([None, None, Some(1_f64)]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "log-with-null2",
            nullable: true,
            columns: vec![
                DataColumn::Constant(DataValue::Float64(None), 3),
                Series::new([Some(10_f64), None, Some(10_f64)]).into(),
            ],
            expect: DFFloat64Array::new_from_opt_slice(&[None, None, None]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "log-with-base-constant",
            nullable: false,
            columns: vec![
                DataColumn::Constant(2.into(), 2),
                Series::new([1, 2, 4]).into(),
            ],
            expect: Series::new([0_f64, 1_f64, 2.0]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "log-with-num-constant",
            nullable: false,
            columns: vec![
                Series::new([2, 4]).into(),
                DataColumn::Constant(2.into(), 2),
            ],
            expect: Series::new([1_f64, 0.5]).into(),
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
            nullable: true,
            columns: vec![Series::new([Some(E), None]).into()],
            expect: Series::new([Some(1_f64), None]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "ln on literal",
            nullable: false,
            columns: vec![Series::new([E]).into()],
            expect: DataColumn::Constant(1_f64.into(), 1),
            error: "",
        },
    ];

    test_scalar_functions(LnFunction::try_create("ln")?, &tests)
}

#[test]
fn test_log2_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "log2 on literal",
            nullable: false,
            columns: vec![Series::new([2_f64]).into()],
            expect: DataColumn::Constant(1_f64.into(), 1),
            error: "",
        }
    ];

    test_scalar_functions(Log2Function::try_create("log2")?, &tests)
}

#[test]
fn test_log10_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "log10 on literal",
            nullable: false,
            columns: vec![Series::new([10_f64]).into()],
            expect: DataColumn::Constant(1_f64.into(), 1),
            error: "",
        }
    ];

    test_scalar_functions(Log10Function::try_create("log10")?, &tests)
}


