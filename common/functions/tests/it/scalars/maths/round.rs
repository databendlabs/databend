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

#[test]
fn test_round_number_function() -> Result<()> {
    struct Test {
        name: &'static str,
        display: &'static str,
        args: Vec<DataColumnWithField>,
        input_rows: usize,
        expect: DataColumn,
        error: &'static str,
    }
    let tests = vec![
        Test {
            name: "no second arg",
            display: "round",
            args: vec![DataColumnWithField::new(
                Series::new([12345.6789]).into(),
                DataField::new("x", DataType::Float64, false),
            )],
            input_rows: 1,
            expect: Series::new([12346.0]).into(),
            error: "",
        },
        Test {
            name: "no second arg with null x",
            display: "round",
            args: vec![DataColumnWithField::new(
                Series::new([Some(12345.6789), None, Some(77.77)]).into(),
                DataField::new("x", DataType::Float64, false),
            )],
            input_rows: 3,
            expect: Series::new([Some(12346.0), None, Some(78.0)]).into(),
            error: "",
        },
        Test {
            name: "second arg is const with null x",
            display: "round",
            args: vec![
                DataColumnWithField::new(
                    Series::new([Some(12345.6789), None, Some(77.77)]).into(),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::Int64(Some(0)), 1),
                    DataField::new("d", DataType::Int64, false),
                ),
            ],
            input_rows: 3,
            expect: Series::new([Some(12346.0), None, Some(78.0)]).into(),
            error: "",
        },
        Test {
            name: "second arg is 0",
            display: "round",
            args: vec![
                DataColumnWithField::new(
                    Series::new([12345.6789]).into(),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    Series::new([0]).into(),
                    DataField::new("d", DataType::Int64, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([12346.0]).into(),
            error: "",
        },
        Test {
            name: "second arg is 2",
            display: "round",
            args: vec![
                DataColumnWithField::new(
                    Series::new([12345.6789]).into(),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    Series::new([2]).into(),
                    DataField::new("d", DataType::Int64, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([12345.68]).into(),
            error: "",
        },
        Test {
            name: "second arg is -2",
            display: "round",
            args: vec![
                DataColumnWithField::new(
                    Series::new([12345.6789]).into(),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    Series::new([-2]).into(),
                    DataField::new("d", DataType::Int64, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([12300.0]).into(),
            error: "",
        },
        Test {
            name: "second arg is const 0",
            display: "round",
            args: vec![
                DataColumnWithField::new(
                    Series::new([12345.6789]).into(),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::Int64(Some(0)), 1),
                    DataField::new("d", DataType::Int64, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([12346.0]).into(),
            error: "",
        },
        Test {
            name: "second arg is const 2",
            display: "round",
            args: vec![
                DataColumnWithField::new(
                    Series::new([12345.6789]).into(),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::Int64(Some(2)), 1),
                    DataField::new("d", DataType::Int64, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([12345.68]).into(),
            error: "",
        },
        Test {
            name: "second arg is const -2",
            display: "round",
            args: vec![
                DataColumnWithField::new(
                    Series::new([12345.6789]).into(),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::Int64(Some(-2)), 1),
                    DataField::new("d", DataType::Int64, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([12300.0]).into(),
            error: "",
        },
        Test {
            name: "second arg is const string '2'",
            display: "round",
            args: vec![
                DataColumnWithField::new(
                    Series::new([12345.6789]).into(),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::String(Some(b"2".to_vec())), 1),
                    DataField::new("d", DataType::String, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([12345.68]).into(),
            error: "",
        },
        Test {
            name: "second arg is const string '-2'",
            display: "round",
            args: vec![
                DataColumnWithField::new(
                    Series::new([12345.6789]).into(),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::String(Some(b"-2".to_vec())), 1),
                    DataField::new("d", DataType::String, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([12300.0]).into(),
            error: "",
        },
        Test {
            name: "second arg is const string '2aaa'",
            display: "round",
            args: vec![
                DataColumnWithField::new(
                    Series::new([12345.6789]).into(),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::String(Some(b"2aaa".to_vec())), 1),
                    DataField::new("d", DataType::String, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([12345.68]).into(),
            error: "",
        },
        Test {
            name: "second arg is const string 'aaa2'",
            display: "round",
            args: vec![
                DataColumnWithField::new(
                    Series::new([12345.6789]).into(),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::String(Some(b"aaa2".to_vec())), 1),
                    DataField::new("d", DataType::String, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([12346.0]).into(),
            error: "",
        },
        Test {
            name: "second arg is const string 'aaa'",
            display: "round",
            args: vec![
                DataColumnWithField::new(
                    Series::new([12345.6789]).into(),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::String(Some(b"aaa".to_vec())), 1),
                    DataField::new("d", DataType::String, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([12346.0]).into(),
            error: "",
        },
        Test {
            name: "second arg is 35",
            display: "round",
            args: vec![
                DataColumnWithField::new(
                    Series::new([
                        0.123_456_789_012_345_68_f64,
                        8888888888888888888888888888888888888888.0_f64,
                    ])
                    .into(),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    Series::new([35, 35]).into(),
                    DataField::new("d", DataType::Int64, false),
                ),
            ],
            input_rows: 2,
            expect: Series::new([
                0.123_456_789_012_345_68_f64,
                8888888888888888888888888888888888888888.0_f64,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "second arg is const 35",
            display: "round",
            args: vec![
                DataColumnWithField::new(
                    Series::new([
                        0.123_456_789_012_345_68_f64,
                        8888888888888888888888888888888888888888.0_f64,
                    ])
                    .into(),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::Int64(Some(35)), 1),
                    DataField::new("d", DataType::Int64, false),
                ),
            ],
            input_rows: 2,
            expect: Series::new([
                0.123_456_789_012_345_68_f64,
                8888888888888888888888888888888888888888.0_f64,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "second arg is -35",
            display: "round",
            args: vec![
                DataColumnWithField::new(
                    Series::new([
                        0.123_456_789_012_345_68_f64,
                        8888888888888888888888888888888888888888.0_f64,
                    ])
                    .into(),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    Series::new([-35, -35]).into(),
                    DataField::new("d", DataType::Int64, false),
                ),
            ],
            input_rows: 2,
            expect: Series::new([0.0, 8888888889000000000000000000000000000000.0_f64]).into(),
            error: "",
        },
        Test {
            name: "second arg is const -35",
            display: "round",
            args: vec![
                DataColumnWithField::new(
                    Series::new([
                        0.123_456_789_012_345_68_f64,
                        8888888888888888888888888888888888888888.0_f64,
                    ])
                    .into(),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::Int64(Some(-35)), 1),
                    DataField::new("d", DataType::Int64, false),
                ),
            ],
            input_rows: 2,
            expect: Series::new([0.0, 8888888889000000000000000000000000000000.0_f64]).into(),
            error: "",
        },
        Test {
            name: "second arg is const NULL",
            display: "round",
            args: vec![
                DataColumnWithField::new(
                    Series::new([12345.6789]).into(),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::Int64(None), 1),
                    DataField::new("d", DataType::Int64, false),
                ),
            ],
            input_rows: 1,
            expect: DFFloat64Array::full_null(1).into(),
            error: "",
        },
        Test {
            name: "second arg is NULL",
            display: "round",
            args: vec![
                DataColumnWithField::new(
                    Series::new([12345.6789, 12345.6789]).into(),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    Series::new([Some(0), None]).into(),
                    DataField::new("d", DataType::Int64, false),
                ),
            ],
            input_rows: 2,
            expect: Series::new([Some(12346.0), None]).into(),
            error: "",
        },
    ];

    for t in tests {
        let func = RoundNumberFunction::try_create("round")?;
        let actual_display = format!("{}", func);
        assert_eq!(t.display.to_string(), actual_display);

        if let Err(e) = func.eval(&t.args, t.input_rows) {
            assert_eq!(t.error, e.to_string(), "{}", t.name);
        }
        let v = &(func.eval(&t.args, t.input_rows)?);
        assert_eq!(v.to_values()?, t.expect.to_values()?, "case: {}", t.name);
    }
    Ok(())
}

#[test]
fn test_trunc_number_function() -> Result<()> {
    struct Test {
        name: &'static str,
        display: &'static str,
        args: Vec<DataColumnWithField>,
        input_rows: usize,
        expect: DataColumn,
        error: &'static str,
    }
    let tests = vec![
        Test {
            name: "first arg is series, second arg is NULL",
            display: "trunc",
            args: vec![
                DataColumnWithField::new(
                    Series::new([Some(11.11), None, Some(33.33)]).into(),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::Int64(None), 1),
                    DataField::new("d", DataType::Int64, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([None::<f64>, None, None]).into(),
            error: "",
        },
        Test {
            name: "first arg is series, second arg is 0 const",
            display: "trunc",
            args: vec![
                DataColumnWithField::new(
                    Series::new([Some(11.11), None, Some(33.33)]).into(),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::Int64(Some(0)), 1),
                    DataField::new("d", DataType::Int64, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([Some(11.0), None, Some(33.0)]).into(),
            error: "",
        },
        Test {
            name: "first arg is series, second arg is positive const",
            display: "trunc",
            args: vec![
                DataColumnWithField::new(
                    Series::new([Some(11.11), None, Some(33.33)]).into(),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::Int64(Some(1)), 1),
                    DataField::new("d", DataType::Int64, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([Some(11.1), None, Some(33.3)]).into(),
            error: "",
        },
        Test {
            name: "first arg is series, second arg is negative const",
            display: "trunc",
            args: vec![
                DataColumnWithField::new(
                    Series::new([Some(11.11), None, Some(33.33)]).into(),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::Int64(Some(-1)), 1),
                    DataField::new("d", DataType::Int64, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([Some(10.0), None, Some(30.0)]).into(),
            error: "",
        },
        Test {
            name: "first arg is const, second is series",
            display: "trunc",
            args: vec![
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::Float64(Some(11.11)), 1),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    Series::new([None, Some(-1), Some(0), Some(1)]).into(),
                    DataField::new("d", DataType::Int64, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([None, Some(10.0), Some(11.0), Some(11.1)]).into(),
            error: "",
        },
        Test {
            name: "both arg are series",
            display: "trunc",
            args: vec![
                DataColumnWithField::new(
                    Series::new([
                        None,
                        None,
                        Some(11.11),
                        Some(22.22),
                        Some(33.33),
                        Some(44.44),
                    ])
                    .into(),
                    DataField::new("x", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    Series::new([None, Some(1), None, Some(0), Some(-1), Some(1)]).into(),
                    DataField::new("d", DataType::Int64, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([None, None, None, Some(22.0), Some(30.0), Some(44.4)]).into(),
            error: "",
        },
    ];

    for t in tests {
        let func = TruncNumberFunction::try_create("trunc")?;
        let actual_display = format!("{}", func);
        assert_eq!(t.display.to_string(), actual_display);

        if let Err(e) = func.eval(&t.args, t.input_rows) {
            assert_eq!(t.error, e.to_string(), "{}", t.name);
        }
        let v = &(func.eval(&t.args, t.input_rows)?);
        assert_eq!(v.to_values()?, t.expect.to_values()?, "case: {}", t.name);
    }
    Ok(())
}
