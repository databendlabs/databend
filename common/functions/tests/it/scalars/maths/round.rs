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
use crate::scalars::scalar_function_test::{ScalarFunctionTest, test_scalar_functions};

#[test]
fn test_round_number_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "no second arg",
            nullable: false,
            columns: vec![Series::new([12345.6789]).into()],
            expect: Series::new([12346.0]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "no second arg with null x",
            nullable: true,
            columns: vec![Series::new([Some(12345.6789), None, Some(77.77)]).into()],
            expect: Series::new([Some(12346.0), None, Some(78.0)]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is const with null x",
            nullable: true,
            columns: vec![
                Series::new([Some(12345.6789), None, Some(77.77)]).into(),
                DataColumn::Constant(DataValue::Int64(Some(0)), 1),
            ],
            expect: Series::new([Some(12346.0), None, Some(78.0)]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is 0",
            nullable: false,
            columns: vec![Series::new([12345.6789]).into(), Series::new([0]).into()],
            expect: Series::new([12346.0]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is 2",
            nullable: false,
            columns: vec![Series::new([12345.6789]).into(), Series::new([2]).into()],
            expect: Series::new([12345.68]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is -2",
            nullable: false,
            columns: vec![Series::new([12345.6789]).into(), Series::new([-2]).into()],
            expect: Series::new([12300.0]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is const 0",
            nullable: false,
            columns: vec![
                Series::new([12345.6789]).into(),
                DataColumn::Constant(DataValue::Int64(Some(0)), 1),
            ],
            expect: Series::new([12346.0]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is const 2",
            nullable: false,
            columns: vec![
                Series::new([12345.6789]).into(),
                DataColumn::Constant(DataValue::Int64(Some(2)), 1),
            ],
            expect: Series::new([12345.68]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is const -2",
            nullable: false,
            columns: vec![
                Series::new([12345.6789]).into(),
                DataColumn::Constant(DataValue::Int64(Some(-2)), 1),
            ],
            expect: Series::new([12300.0]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is const string '2'",
            nullable: false,
            columns: vec![
                Series::new([12345.6789]).into(),
                DataColumn::Constant(DataValue::String(Some(b"2".to_vec())), 1),
            ],
            expect: Series::new([12345.68]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is const string '-2'",
            nullable: false,
            columns: vec![
                Series::new([12345.6789]).into(),
                DataColumn::Constant(DataValue::String(Some(b"-2".to_vec())), 1),
            ],
            expect: Series::new([12300.0]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is const string '2aaa'",
            nullable: false,
            columns: vec![
                Series::new([12345.6789]).into(),
                DataColumn::Constant(DataValue::String(Some(b"2aaa".to_vec())), 1),
            ],
            expect: Series::new([12345.68]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is const string 'aaa2'",
            nullable: false,
            columns: vec![
                Series::new([12345.6789]).into(),
                DataColumn::Constant(DataValue::String(Some(b"aaa2".to_vec())), 1),
            ],
            expect: Series::new([12346.0]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is const string 'aaa'",
            nullable: false,
            columns: vec![
                Series::new([12345.6789]).into(),
                DataColumn::Constant(DataValue::String(Some(b"aaa".to_vec())), 1),
            ],
            expect: Series::new([12346.0]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is 35",
            nullable: false,
            columns: vec![
                Series::new([
                    0.123_456_789_012_345_68_f64,
                    8888888888888888888888888888888888888888.0_f64,
                ]).into(),
                Series::new([35, 35]).into(),
            ],
            expect: Series::new([
                0.123_456_789_012_345_68_f64,
                8888888888888888888888888888888888888888.0_f64,
            ]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is const 35",
            nullable: false,
            columns: vec![
                Series::new([
                    0.123_456_789_012_345_68_f64,
                    8888888888888888888888888888888888888888.0_f64,
                ])
                    .into(),
                DataColumn::Constant(DataValue::Int64(Some(35)), 1),
            ],
            expect: Series::new([
                0.123_456_789_012_345_68_f64,
                8888888888888888888888888888888888888888.0_f64,
            ])
                .into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is -35",
            nullable: false,
            columns: vec![
                Series::new([
                    0.123_456_789_012_345_68_f64,
                    8888888888888888888888888888888888888888.0_f64,
                ])
                    .into(),
                Series::new([-35, -35]).into(),
            ],
            expect: Series::new([0.0, 8888888889000000000000000000000000000000.0_f64]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is const -35",
            nullable: false,
            columns: vec![
                Series::new([
                    0.123_456_789_012_345_68_f64,
                    8888888888888888888888888888888888888888.0_f64,
                ])
                    .into(),
                DataColumn::Constant(DataValue::Int64(Some(-35)), 1),
            ],
            expect: Series::new([0.0, 8888888889000000000000000000000000000000.0_f64]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is const NULL",
            nullable: true,
            columns: vec![
                Series::new([12345.6789]).into(),
                DataColumn::Constant(DataValue::Int64(None), 1),
            ],
            expect: DFFloat64Array::full_null(1).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "second arg is NULL",
            nullable: true,
            columns: vec![
                Series::new([12345.6789, 12345.6789]).into(),
                Series::new([Some(0), None]).into(),
            ],
            expect: Series::new([Some(12346.0), None]).into(),
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
            nullable: true,
            columns: vec![
                Series::new([Some(11.11), None, Some(33.33)]).into(),
                DataColumn::Constant(DataValue::Int64(None), 1),
            ],
            expect: Series::new([None::<f64>, None, None]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "first arg is series, second arg is 0 const",
            nullable: true,
            columns: vec![
                Series::new([Some(11.11), None, Some(33.33)]).into(),
                DataColumn::Constant(DataValue::Int64(Some(0)), 1),
            ],
            expect: Series::new([Some(11.0), None, Some(33.0)]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "first arg is series, second arg is positive const",
            nullable: true,
            columns: vec![
                Series::new([Some(11.11), None, Some(33.33)]).into(),
                DataColumn::Constant(DataValue::Int64(Some(1)), 1),
            ],
            expect: Series::new([Some(11.1), None, Some(33.3)]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "first arg is series, second arg is negative const",
            nullable: true,
            columns: vec![
                Series::new([Some(11.11), None, Some(33.33)]).into(),
                DataColumn::Constant(DataValue::Int64(Some(-1)), 1),
            ],
            expect: Series::new([Some(10.0), None, Some(30.0)]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "first arg is const, second is series",
            nullable: true,
            columns: vec![
                DataColumn::Constant(DataValue::Float64(Some(11.11)), 1),
                Series::new([None, Some(-1), Some(0), Some(1)]).into(),
            ],
            expect: Series::new([None, Some(10.0), Some(11.0), Some(11.1)]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "both arg are series",
            nullable: true,
            columns: vec![
                Series::new([None, None, Some(11.11), Some(22.22), Some(33.33), Some(44.44)]).into(),
                Series::new([None, Some(1), None, Some(0), Some(-1), Some(1)]).into(),
            ],
            expect: Series::new([None, None, None, Some(22.0), Some(30.0), Some(44.4)]).into(),
            error: "",
        },
    ];

    test_scalar_functions(TruncNumberFunction::try_create("trunc")?, &tests)
}
