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
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::*;

#[test]
fn test_ceil_function() -> Result<()> {
    struct Test {
        name: &'static str,
        func: Box<dyn Function>,
        arg: DataColumnWithField,
        expect: Result<DataColumn>,
    }
    let tests = vec![
        Test {
            name: "ceil(123)",
            func: CeilFunction::try_create("ceil")?,
            arg: DataColumnWithField::new(
                Series::new([123]).into(),
                DataField::new("arg1", DataType::Int32, false),
            ),
            expect: Ok(Series::new(vec![123_f64]).into()),
        },
        Test {
            name: "ceil(1.2)",
            func: CeilFunction::try_create("ceil")?,
            arg: DataColumnWithField::new(
                Series::new([1.2]).into(),
                DataField::new("arg1", DataType::Float64, false),
            ),
            expect: Ok(Series::new(vec![2_f64]).into()),
        },
        Test {
            name: "ceil(-1.2)",
            func: CeilFunction::try_create("ceil")?,
            arg: DataColumnWithField::new(
                Series::new([-1.2]).into(),
                DataField::new("arg1", DataType::Float64, false),
            ),
            expect: Ok(Series::new(vec![-1_f64]).into()),
        },
        Test {
            name: "ceil('123')",
            func: CeilFunction::try_create("ceil")?,
            arg: DataColumnWithField::new(
                Series::new(["123"]).into(),
                DataField::new("arg1", DataType::String, true),
            ),
            expect: Ok(Series::new(vec![123_f64]).into()),
        },
        Test {
            name: "ceil('+123.2a1')",
            func: CeilFunction::try_create("ceil")?,
            arg: DataColumnWithField::new(
                Series::new(["+123.2a1"]).into(),
                DataField::new("arg1", DataType::String, true),
            ),
            expect: Ok(Series::new(vec![124_f64]).into()),
        },
        Test {
            name: "ceil('-123.2a1')",
            func: CeilFunction::try_create("ceil")?,
            arg: DataColumnWithField::new(
                Series::new(["-123.2a1"]).into(),
                DataField::new("arg1", DataType::String, true),
            ),
            expect: Ok(Series::new(vec![-123_f64]).into()),
        },
        Test {
            name: "ceil('a')",
            func: CeilFunction::try_create("ceil")?,
            arg: DataColumnWithField::new(
                Series::new(["a"]).into(),
                DataField::new("arg1", DataType::String, true),
            ),
            expect: Ok(Series::new(vec![0_f64]).into()),
        },
        Test {
            name: "ceil('a123')",
            func: CeilFunction::try_create("ceil")?,
            arg: DataColumnWithField::new(
                Series::new(["a123"]).into(),
                DataField::new("arg1", DataType::String, true),
            ),
            expect: Ok(Series::new(vec![0_f64]).into()),
        },
        Test {
            name: "ceil(true)",
            func: CeilFunction::try_create("ceil")?,
            arg: DataColumnWithField::new(
                Series::new([true]).into(),
                DataField::new("arg1", DataType::Boolean, true),
            ),
            expect: Err(ErrorCode::IllegalDataType(
                "Expected numeric types, but got Boolean",
            )),
        },
    ];

    for t in tests {
        let func = t.func;
        let got = func.return_type(&[t.arg.data_type().clone()]);
        let got = got.and_then(|_| func.eval(&[t.arg], 1));

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
