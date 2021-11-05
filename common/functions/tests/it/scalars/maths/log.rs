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

use std::f64::consts::E;

use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::scalars::*;

#[test]
fn test_log_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        nullable: bool,
        args: Vec<DataColumnWithField>,
        expect: DataColumn,
        error: &'static str,
        input_rows: usize,
        func: Box<dyn Function>,
    }
    let tests = vec![
        Test {
            name: "log-with-literal",
            display: "LOG",
            nullable: false,
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
            input_rows: 1,
            func: LogFunction::try_create("log")?,
            expect: DataColumn::Constant(2_f64.into(), 1),
            error: "",
        },
        Test {
            name: "log-with-series",
            display: "LOG",
            nullable: false,
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
            input_rows: 3,
            func: LogFunction::try_create("log")?,
            expect: Series::new([2_f64, 2.9999999999999996, 4_f64]).into(),
            error: "",
        },
        Test {
            name: "log-with-one-arg",
            display: "LOG",
            nullable: false,
            args: vec![DataColumnWithField::new(
                Series::new([E, E, E]).into(),
                DataField::new("num", DataType::String, false),
            )],
            input_rows: 3,
            func: LogFunction::try_create("log")?,
            expect: Series::new([1_f64, 1_f64, 1_f64]).into(),
            error: "",
        },
    ];
    for t in tests {
        let func = t.func;

        if let Err(e) = func.eval(&t.args, t.input_rows) {
            assert_eq!(t.error, e.to_string(), "{}", t.name);
        }

        // Display check.
        let expect_display = t.display.to_string();
        let actual_display = format!("{}", func);
        assert_eq!(expect_display, actual_display);

        let v = &(func.eval(&t.args, t.input_rows)?);
        assert_eq!(
            v,
            &t.expect,
            "case: {} got_values: {:?}",
            t.name,
            v.to_values()
        );
    }
    Ok(())
}
