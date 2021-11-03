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

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::*;

#[test]
fn test_abs_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        func: Box<dyn Function>,
        arg: DataColumnWithField,
        expect: Result<DataColumn>,
    }
    let tests = vec![
        Test {
            name: "abs(-1)",
            func: AbsFunction::try_create("abs(-1)")?,
            arg: DataColumnWithField::new(
                Series::new([-1]).into(),
                DataField::new("arg1", DataType::Int32, false),
            ),
            expect: Ok(Series::new(vec![1_u32]).into()),
        },
        Test {
            name: "abs(-10086)",
            func: AbsFunction::try_create("abs(-10086)")?,
            arg: DataColumnWithField::new(
                Series::new([-10086]).into(),
                DataField::new("arg1", DataType::Int32, false),
            ),
            expect: Ok(Series::new(vec![10086_u32]).into()),
        },
        Test {
            name: "abs('-2.0')",
            func: AbsFunction::try_create("abs('-2.0')")?,
            arg: DataColumnWithField::new(
                Series::new(["-2.0"]).into(),
                DataField::new("arg1", DataType::String, false),
            ),
            expect: Ok(Series::new(vec![2.0_f64]).into()),
        },
        Test {
            name: "abs(true)",
            func: AbsFunction::try_create("abs(false)")?,
            arg: DataColumnWithField::new(
                Series::new([false]).into(),
                DataField::new("arg1", DataType::Boolean, false),
            ),
            expect: Err(ErrorCode::IllegalDataType(
                "Expected numeric types, but got Boolean",
            )),
        },
    ];

    for t in tests {
        let func = t.func;
        let got = func.eval(&[t.arg.clone()], 1);
        match t.expect {
            Ok(expected) => {
                assert_eq!(&got.unwrap(), &expected, "case: {}", t.name);
            }
            Err(expected_err) => {
                assert_eq!(got.unwrap_err().to_string(), expected_err.to_string());
            }
        }
    }
    Ok(())
}
