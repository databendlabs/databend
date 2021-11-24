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

#[test]
fn test_trigonometic_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        nullable: bool,
        args: Vec<DataColumnWithField>,
        expect: DataColumn,
        error: &'static str,
        func: Box<dyn Function>,
    }

    let tests = vec![
        Test {
            name: "sin-u8-passed",
            display: "sin",
            nullable: false,
            args: vec![DataColumnWithField::new(
                Series::new(vec![0_u8, 1, 3]).into(),
                DataField::new("dummy", DataType::UInt8, false),
            )],
            func: TrigonometricSinFunction::try_create_func("sin")?,
            expect: Series::new(vec![0f64, 0.8414709848078965, 0.1411200080598672]).into(),
            error: "",
        },
        Test {
            name: "sin-u16-passed",
            display: "sin",
            nullable: false,
            args: vec![DataColumnWithField::new(
                Series::new(vec![0_u16, 1, 3]).into(),
                DataField::new("dummy", DataType::UInt16, false),
            )],
            func: TrigonometricSinFunction::try_create_func("sin")?,
            expect: Series::new(vec![0f64, 0.8414709848078965, 0.1411200080598672]).into(),
            error: "",
        },
        Test {
            name: "sin-u32-passed",
            display: "sin",
            nullable: false,
            args: vec![DataColumnWithField::new(
                Series::new(vec![0_u32, 1, 3]).into(),
                DataField::new("dummy", DataType::UInt32, false),
            )],
            func: TrigonometricSinFunction::try_create_func("sin")?,
            expect: Series::new(vec![0f64, 0.8414709848078965, 0.1411200080598672]).into(),
            error: "",
        },
        Test {
            name: "sin-u64-passed",
            display: "sin",
            nullable: false,
            args: vec![DataColumnWithField::new(
                Series::new(vec![0_u64, 1, 3]).into(),
                DataField::new("dummy", DataType::UInt64, false),
            )],
            func: TrigonometricSinFunction::try_create_func("sin")?,
            expect: Series::new(vec![0f64, 0.8414709848078965, 0.1411200080598672]).into(),
            error: "",
        },
        Test {
            name: "sin-str-passed",
            display: "sin",
            nullable: false,
            args: vec![DataColumnWithField::new(
                Series::new(vec!["0", "1", "3"]).into(),
                DataField::new("dummy", DataType::String, false),
            )],
            func: TrigonometricSinFunction::try_create_func("sin")?,
            expect: Series::new(vec![0f64, 0.8414709848078965, 0.1411200080598672]).into(),
            error: "",
        },
        Test {
            name: "sin-f64-passed",
            display: "sin",
            nullable: false,
            args: vec![DataColumnWithField::new(
                Series::new(vec![0_f64, 1.0, 3.0]).into(),
                DataField::new("dummy", DataType::Float64, false),
            )],
            func: TrigonometricSinFunction::try_create_func("sin")?,
            expect: Series::new(vec![0f64, 0.8414709848078965, 0.1411200080598672]).into(),
            error: "",
        },
        Test {
            name: "cos-f64-passed",
            display: "cos",
            nullable: false,
            args: vec![DataColumnWithField::new(
                Series::new(vec![0_f64, 1.0, 3.0]).into(),
                DataField::new("dummy", DataType::Float64, false),
            )],
            func: TrigonometricCosFunction::try_create_func("cos")?,
            expect: Series::new(vec![1f64, 0.5403023058681398, -0.9899924966004454]).into(),
            error: "",
        },
        Test {
            name: "tan-pi4-passed",
            display: "tan",
            nullable: false,
            args: vec![DataColumnWithField::new(
                Series::new(vec![0_f64, PI / 4.0]).into(),
                DataField::new("dummy", DataType::Float64, false),
            )],
            func: TrigonometricTanFunction::try_create_func("tan")?,
            expect: Series::new(vec![0f64, 0.9999999999999999]).into(),
            error: "",
        },
        Test {
            name: "cot-pi4-passed",
            display: "cot",
            nullable: false,
            args: vec![DataColumnWithField::new(
                Series::new(vec![PI / 4.0]).into(),
                DataField::new("dummy", DataType::Float64, false),
            )],
            func: TrigonometricCotFunction::try_create_func("cot")?,
            expect: Series::new(vec![1.0000000000000002]).into(),
            error: "",
        },
        Test {
            name: "cot-0-passed",
            display: "cot",
            nullable: false,
            args: vec![DataColumnWithField::new(
                Series::new(vec![0_f64]).into(),
                DataField::new("dummy", DataType::Float64, false),
            )],
            func: TrigonometricCotFunction::try_create_func("cot")?,
            expect: Series::new(vec![f64::INFINITY]).into(),
            error: "",
        },
        Test {
            name: "asin-passed",
            display: "asin",
            nullable: false,
            args: vec![DataColumnWithField::new(
                Series::new(vec![0.2_f64]).into(),
                DataField::new("dummy", DataType::Float64, false),
            )],
            func: TrigonometricAsinFunction::try_create_func("asin")?,
            expect: DataColumn::Constant(0.2013579207903308_f64.into(), 1),
            error: "",
        },
        Test {
            name: "acos-passed",
            display: "acos",
            nullable: false,
            args: vec![DataColumnWithField::new(
                Series::new(vec![1]).into(),
                DataField::new("dummy", DataType::Float64, false),
            )],
            func: TrigonometricAcosFunction::try_create_func("acos")?,
            expect: DataColumn::Constant(0_f64.into(), 1),
            error: "",
        },
        Test {
            name: "atan-passed",
            display: "atan",
            nullable: false,
            args: vec![DataColumnWithField::new(
                Series::new(vec![1, -1]).into(),
                DataField::new("dummy", DataType::Float64, false),
            )],
            func: TrigonometricAtanFunction::try_create_func("atan")?,
            expect: Series::new(vec![FRAC_PI_4, -FRAC_PI_4]).into(),
            error: "",
        },
        Test {
            name: "atan-passed",
            display: "atan",
            nullable: false,
            args: vec![
                DataColumnWithField::new(
                    Series::new(vec![-2_f64, PI]).into(),
                    DataField::new("y", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    Series::new(vec![2, 0]).into(),
                    DataField::new("x", DataType::Float64, false),
                ),
            ],
            func: TrigonometricAtanFunction::try_create_func("atan")?,
            expect: Series::new(vec![-FRAC_PI_4, FRAC_PI_2]).into(),
            error: "",
        },
        Test {
            name: "atan2-passed",
            display: "atan2",
            nullable: false,
            args: vec![
                DataColumnWithField::new(
                    Series::new(vec![-2_f64, PI]).into(),
                    DataField::new("y", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    Series::new(vec![2, 0]).into(),
                    DataField::new("x", DataType::Float64, false),
                ),
            ],
            func: TrigonometricAtan2Function::try_create_func("atan2")?,
            expect: Series::new(vec![-FRAC_PI_4, FRAC_PI_2]).into(),
            error: "",
        },
        Test {
            name: "atan2-y-constant-passed",
            display: "atan2",
            nullable: false,
            args: vec![
                DataColumnWithField::new(
                    DataColumn::Constant(2.into(), 2),
                    DataField::new("y", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    Series::new(vec![0_f64, 2.0]).into(),
                    DataField::new("x", DataType::Float64, false),
                ),
            ],
            func: TrigonometricAtan2Function::try_create_func("atan2")?,
            expect: Series::new(vec![FRAC_PI_2, FRAC_PI_4]).into(),
            error: "",
        },
        Test {
            name: "atan2-x-constant-passed",
            display: "atan2",
            nullable: false,
            args: vec![
                DataColumnWithField::new(
                    Series::new(vec![-2_f64, 2.0]).into(),
                    DataField::new("y", DataType::Float64, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(2.into(), 2),
                    DataField::new("x", DataType::Float64, false),
                ),
            ],
            func: TrigonometricAtan2Function::try_create_func("atan2")?,
            expect: Series::new(vec![-FRAC_PI_4, FRAC_PI_4]).into(),
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

        let v = &(func.eval(&t.args, t.args[0].column().len())?);
        assert_eq!(v, &t.expect, "{}", t.name);
    }
    Ok(())
}
