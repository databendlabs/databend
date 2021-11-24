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
fn test_pow_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        args: Vec<DataColumnWithField>,
        expect: DataColumn,
        error: &'static str,
    }
    let tests = vec![
        Test {
            name: "pow-with-literal",
            display: "POW",
            args: vec![
                DataColumnWithField::new(
                    Series::new([2]).into(),
                    DataField::new("x", DataType::Int32, false),
                ),
                DataColumnWithField::new(
                    Series::new([2]).into(),
                    DataField::new("y", DataType::Int32, false),
                ),
            ],
            expect: DataColumn::Constant(4_f64.into(), 1),
            error: "",
        },
        Test {
            name: "pow-with-series",
            display: "POW",
            args: vec![
                DataColumnWithField::new(
                    Series::new([2, 2]).into(),
                    DataField::new("x", DataType::Int32, false),
                ),
                DataColumnWithField::new(
                    Series::new([2, -2]).into(),
                    DataField::new("y", DataType::Int32, false),
                ),
            ],
            expect: Series::new([4_f64, 0.25]).into(),
            error: "",
        },
        Test {
            name: "pow-with-null",
            display: "POW",
            args: vec![
                DataColumnWithField::new(
                    Series::new([Some(2), None, None]).into(),
                    DataField::new("x", DataType::Int32, false),
                ),
                DataColumnWithField::new(
                    Series::new([Some(2), Some(-2), None]).into(),
                    DataField::new("y", DataType::Int32, false),
                ),
            ],
            expect: Series::new([Some(4_f64), None, None]).into(),
            error: "",
        },
        Test {
            name: "pow-x-constant",
            display: "POW",
            args: vec![
                DataColumnWithField::new(
                    DataColumn::Constant(2.into(), 2),
                    DataField::new("x", DataType::Int32, false),
                ),
                DataColumnWithField::new(
                    Series::new([Some(2), Some(-2), None]).into(),
                    DataField::new("y", DataType::Int32, false),
                ),
            ],
            expect: Series::new([Some(4_f64), Some(0.25), None]).into(),
            error: "",
        },
        Test {
            name: "pow-y-constant",
            display: "POW",
            args: vec![
                DataColumnWithField::new(
                    Series::new([Some(2), Some(-2), None]).into(),
                    DataField::new("x", DataType::Int32, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(2.into(), 2),
                    DataField::new("y", DataType::Int32, false),
                ),
            ],
            expect: Series::new([Some(4_f64), Some(4.0), None]).into(),
            error: "",
        },
    ];
    let func = PowFunction::try_create("pow")?;
    for t in tests {
        if let Err(e) = func.eval(&t.args, t.args[0].column().len()) {
            assert_eq!(t.error, e.to_string(), "{}", t.name);
        }

        // Display check.
        let expect_display = t.display.to_string();
        let actual_display = format!("{}", func);
        assert_eq!(expect_display, actual_display);

        let v = &(func.eval(&t.args, t.args[0].column().len())?);
        assert_eq!(v, &t.expect, "{}", t.name);

        let return_type = v.data_type();
        let expected_type = func.return_type(&[t.args[0].column().data_type()])?;
        assert_eq!(expected_type, return_type);
    }
    Ok(())
}
