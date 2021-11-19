// Copyright 2020 Datafuse Labs.
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
        columns: DataColumn,
        expect: DataColumn,
        error: &'static str,
        func: Box<dyn Function>,
    }

    let tests = vec![
        Test {
            name: "sin-u8-passed",
            display: "sin",
            nullable: false,
            columns: Series::new(vec![0_u8, 1, 3]).into(),
            func: TrigonometricSinFunction::try_create_func("sin")?,
            expect: Series::new(vec![0f64, 0.8414709848078965, 0.1411200080598672]).into(),
            error: "",
        },
        Test {
            name: "sin-u16-passed",
            display: "sin",
            nullable: false,
            columns: Series::new(vec![0_u16, 1, 3]).into(),
            func: TrigonometricSinFunction::try_create_func("sin")?,
            expect: Series::new(vec![0f64, 0.8414709848078965, 0.1411200080598672]).into(),
            error: "",
        },
        Test {
            name: "sin-u32-passed",
            display: "sin",
            nullable: false,
            columns: Series::new(vec![0_u32, 1, 3]).into(),
            func: TrigonometricSinFunction::try_create_func("sin")?,
            expect: Series::new(vec![0f64, 0.8414709848078965, 0.1411200080598672]).into(),
            error: "",
        },
        Test {
            name: "sin-u64-passed",
            display: "sin",
            nullable: false,
            columns: Series::new(vec![0_u64, 1, 3]).into(),
            func: TrigonometricSinFunction::try_create_func("sin")?,
            expect: Series::new(vec![0f64, 0.8414709848078965, 0.1411200080598672]).into(),
            error: "",
        },
        Test {
            name: "sin-str-passed",
            display: "sin",
            nullable: false,
            columns: Series::new(vec!["0", "1", "3"]).into(),
            func: TrigonometricSinFunction::try_create_func("sin")?,
            expect: Series::new(vec![0f64, 0.8414709848078965, 0.1411200080598672]).into(),
            error: "",
        },
        Test {
            name: "sin-bool-passed",
            display: "sin",
            nullable: false,
            columns: Series::new(vec![true]).into(),
            func: TrigonometricSinFunction::try_create_func("sin")?,
            expect: Series::new(vec![0.8414709848078965]).into(),
            error: "",
        },
        Test {
            name: "sin-f64-passed",
            display: "sin",
            nullable: false,
            columns: Series::new(vec![0_f64, 1.0, 3.0]).into(),
            func: TrigonometricSinFunction::try_create_func("sin")?,
            expect: Series::new(vec![0f64, 0.8414709848078965, 0.1411200080598672]).into(),
            error: "",
        },
        Test {
            name: "cos-f64-passed",
            display: "cos",
            nullable: false,
            columns: Series::new(vec![0_f64, 1.0, 3.0]).into(),
            func: TrigonometricCosFunction::try_create_func("cos")?,
            expect: Series::new(vec![1f64, 0.5403023058681398, -0.9899924966004454]).into(),
            error: "",
        },
        Test {
            name: "tan-pi4-passed",
            display: "tan",
            nullable: false,
            columns: Series::new(vec![0_f64, PI / 4.0]).into(),
            func: TrigonometricTanFunction::try_create_func("tan")?,
            expect: Series::new(vec![0f64, 0.9999999999999999]).into(),
            error: "",
        },
        Test {
            name: "cot-pi4-passed",
            display: "cot",
            nullable: false,
            columns: Series::new(vec![PI / 4.0]).into(),
            func: TrigonometricCotFunction::try_create_func("cot")?,
            expect: Series::new(vec![1.0000000000000002]).into(),
            error: "",
        },
        Test {
            name: "cot-0-passed",
            display: "cot",
            nullable: false,
            columns: Series::new(vec![0_f64]).into(),
            func: TrigonometricCotFunction::try_create_func("cot")?,
            expect: Series::new(vec![f64::INFINITY]).into(),
            error: "",
        },
    ];

    for t in tests {
        let func = t.func;
        let rows = t.columns.len();

        let columns = vec![DataColumnWithField::new(
            t.columns.clone(),
            DataField::new("dummpy", t.columns.data_type(), false),
        )];

        if let Err(e) = func.eval(&columns, rows) {
            assert_eq!(t.error, e.to_string(), "{}", t.name);
        }

        // Display check.
        let expect_display = t.display.to_string();
        let actual_display = format!("{}", func);
        assert_eq!(expect_display, actual_display);

        let v = &(func.eval(&columns, rows)?);
        assert_eq!(v, &t.expect, "{}", t.name);
    }
    Ok(())
}
