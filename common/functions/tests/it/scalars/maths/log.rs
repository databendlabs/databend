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

#[test]
fn test_log_function() -> Result<()> {
    struct Test {
        name: &'static str,
        display: &'static str,
        args: Vec<DataColumnWithField>,
        expect: DataColumn,
        error: &'static str,
        func: Box<dyn Function>,
    }
    let tests = vec![
        Test {
            name: "log-with-literal",
            display: "LOG",
            args: vec![
                DataColumnWithField::new(
                    Series::new([10]).into(),
                    DataField::new("base", DataType::Int32, false),
                ),
                DataColumnWithField::new(
                    Series::new(["100"]).into(),
                    DataField::new("num", DataType::String, false),
                ),
            ],
            func: LogFunction::try_create("log")?,
            expect: DataColumn::Constant(2_f64.into(), 1),
            error: "",
        },
        Test {
            name: "log-with-series",
            display: "LOG",
            args: vec![
                DataColumnWithField::new(
                    Series::new([10, 10, 10]).into(),
                    DataField::new("base", DataType::UInt32, false),
                ),
                DataColumnWithField::new(
                    Series::new(["100", "1000", "10000"]).into(),
                    DataField::new("num", DataType::String, false),
                ),
            ],
            func: LogFunction::try_create("log")?,
            expect: Series::new([2_f64, 2.9999999999999996, 4_f64]).into(),
            error: "",
        },
        Test {
            name: "log-with-one-arg",
            display: "LOG",
            args: vec![DataColumnWithField::new(
                Series::new([E, E, E]).into(),
                DataField::new("num", DataType::Float64, false),
            )],
            func: LogFunction::try_create("log")?,
            expect: Series::new([1_f64, 1_f64, 1_f64]).into(),
            error: "",
        },
        Test {
            name: "log-with-null",
            display: "LOG",
            args: vec![
                DataColumnWithField::new(
                    Series::new([None, Some(10_f64), Some(10_f64)]).into(),
                    DataField::new("base", DataType::Float64, true),
                ),
                DataColumnWithField::new(
                    Series::new([Some(10_f64), None, Some(10_f64)]).into(),
                    DataField::new("num", DataType::Float64, true),
                ),
            ],
            func: LogFunction::try_create("log")?,
            expect: Series::new([None, None, Some(1_f64)]).into(),
            error: "",
        },
        Test {
            name: "log-with-null2",
            display: "LOG",
            args: vec![
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::Float64(None), 3),
                    DataField::new("base", DataType::Float64, true),
                ),
                DataColumnWithField::new(
                    Series::new([Some(10_f64), None, Some(10_f64)]).into(),
                    DataField::new("num", DataType::Float64, true),
                ),
            ],
            func: LogFunction::try_create("log")?,
            expect: DFFloat64Array::new_from_opt_slice(&[None, None, None]).into(),
            error: "",
        },
        Test {
            name: "log-with-base-constant",
            display: "LOG",
            args: vec![
                DataColumnWithField::new(
                    DataColumn::Constant(2.into(), 2),
                    DataField::new("base", DataType::Float64, true),
                ),
                DataColumnWithField::new(
                    Series::new([1, 2, 4]).into(),
                    DataField::new("num", DataType::Float64, true),
                ),
            ],
            func: LogFunction::try_create("log")?,
            expect: Series::new([0_f64, 1_f64, 2.0]).into(),
            error: "",
        },
        Test {
            name: "log-with-num-constant",
            display: "LOG",
            args: vec![
                DataColumnWithField::new(
                    Series::new([2, 4]).into(),
                    DataField::new("base", DataType::Float64, true),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(2.into(), 2),
                    DataField::new("num", DataType::Float64, true),
                ),
            ],
            func: LogFunction::try_create("log")?,
            expect: Series::new([1_f64, 0.5]).into(),
            error: "",
        },
        Test {
            name: "ln on series",
            display: "LN",
            args: vec![DataColumnWithField::new(
                Series::new([Some(E), None]).into(),
                DataField::new("num", DataType::Float64, false),
            )],
            func: LnFunction::try_create("ln")?,
            expect: Series::new([Some(1_f64), None]).into(),
            error: "",
        },
        Test {
            name: "ln on literal",
            display: "LN",
            args: vec![DataColumnWithField::new(
                Series::new([E]).into(),
                DataField::new("num", DataType::Float64, false),
            )],
            func: LnFunction::try_create("ln")?,
            expect: DataColumn::Constant(1_f64.into(), 1),
            error: "",
        },
        Test {
            name: "log2 on literal",
            display: "LOG2",
            args: vec![DataColumnWithField::new(
                Series::new([2_f64]).into(),
                DataField::new("num", DataType::Float64, false),
            )],
            func: Log2Function::try_create("log2")?,
            expect: DataColumn::Constant(1_f64.into(), 1),
            error: "",
        },
        Test {
            name: "log10 on literal",
            display: "LOG10",
            args: vec![DataColumnWithField::new(
                Series::new([10_f64]).into(),
                DataField::new("num", DataType::Float64, false),
            )],
            func: Log10Function::try_create("log10")?,
            expect: DataColumn::Constant(1_f64.into(), 1),
            error: "",
        },
    ];
    for t in tests {
        let func = t.func;

        if let Err(e) = func.eval(&t.args, t.args[0].column().len()) {
            assert_eq!(t.error, e.to_string(), "{}", t.name);
        }

        // Display check.
        let expect_display = t.display.to_string();
        let actual_display = format!("{}", func);
        assert_eq!(expect_display, actual_display);

        let v = test_helpers::eval_function(&t.args, t.args[0].column().len(), func)?;
        assert_eq!(&v, &t.expect, "case: {}", t.name);
    }
    Ok(())
}
